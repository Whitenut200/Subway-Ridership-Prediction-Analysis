import boto3
import joblib
import pandas as pd
import numpy as np
from io import BytesIO

# ===== 설정 =====
S3_BUCKET = "subway-whitenut-bucket"
INPUT_PREFIX = "prepared_data"     # 전처리 완료된 데이터가 있는 폴더
OUTPUT_PREFIX = "predictions"      # 예측 CSV 저장 폴더

# 모델/인코더 파일
MODEL_LGB_KEY = "model/model_lgb_only.joblib"
FEATURES_KEY = "model/features.joblib"
LINE_ENCODER_KEY = "model/line_encoder.joblib"
STATION_ENCODER_KEY = "model/station_encoder.joblib"

# s3에 저장된 전처리 완료된 데이터 가져오기
def _read_csv_from_s3(s3, key, encoding="utf-8"):
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return pd.read_csv(BytesIO(obj["Body"].read()), encoding=encoding)

# 예측결과 S3에 csv 형식으로 저장하기
def _write_csv_to_s3(s3, df, key):
    data = df.to_csv(index=False).encode("utf-8")
    buf = BytesIO(data)
    s3.upload_fileobj(buf, S3_BUCKET, key, ExtraArgs={"ContentType": "text/csv; charset=utf-8"})

# S3에 저장된 학습 데이터 가져오기
# 모델 학습, 인코딩 정보
def _load_joblib(s3, key):
    buf = BytesIO()
    s3.download_fileobj(S3_BUCKET, key, buf)
    buf.seek(0)
    return joblib.load(buf)

# 날짜 인코딩
def _onehot_weekday(df):
    df["날짜"] = pd.to_datetime(df["날짜"]).dt.normalize()
    df["년"] = df["날짜"].dt.year
    df["월"] = df["날짜"].dt.month
    df["일"] = df["날짜"].dt.day
    weekday_map = {0:'월',1:'화',2:'수',3:'목',4:'금',5:'토',6:'일'}
    df["요일문자"] = df["날짜"].dt.dayofweek.map(weekday_map)
    for d in ['월','화','수','목','금','토','일']:
        df[f'요일_{d}'] = (df['요일문자'] == d).astype(int)
    return df

# 새로 생긴 역이 들어올 경우
# 라벨인코딩을 안전하게 실행
def _safe_label_encode(le, series, name):
    series = series.astype(str).str.strip()
    known = set(le.classes_.tolist())
    mask = series.isin(known)
    if not mask.all():
        print(f"[WARN] unseen {name} labels dropped: {series[~mask].unique().tolist()}")
    enc = le.transform(series[mask])
    return enc, mask

# 가장 마지막 날짜의 데이터 가져오기기
def _find_latest_prepared_csv(s3):
    paginator = s3.get_paginator("list_objects_v2")
    latest = None
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{INPUT_PREFIX}/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".csv"):
                if (latest is None) or (obj["LastModified"] > latest["LastModified"]):
                    latest = obj
    return None if latest is None else latest["Key"]

def lambda_handler(event, context):
    """
    event 예시(옵션):
    { "s3_key": "prepared_data/2025-08-09.csv" }
    주지 않으면 prepared_data/에서 최신 CSV 자동 선택.
    """
    try:
        s3 = boto3.client("s3")

        # 입력 키 결정
        in_key = (event or {}).get("s3_key")
        if not in_key:
            in_key = _find_latest_prepared_csv(s3)
        if not in_key:
            return {"status": "error", "message": "prepared_data/ 아래 CSV를 찾지 못했습니다. (s3_key 미지정)"}
        if not in_key.startswith(f"{INPUT_PREFIX}/") or not in_key.endswith(".csv"):
            return {"status": "error", "message": f"잘못된 입력 키: {in_key}"}

        # 입력 데이터 로드
        df = _read_csv_from_s3(s3, in_key)
        if df.empty:
            return {"status": "error", "message": "입력 CSV가 비어 있음", "input_key": in_key}

        # 모델/인코더/피처 로드
        models      = _load_joblib(s3, MODEL_LGB_KEY)            # dict 구조
        features    = _load_joblib(s3, FEATURES_KEY)         # list
        le_line     = _load_joblib(s3, LINE_ENCODER_KEY)
        le_station  = _load_joblib(s3, STATION_ENCODER_KEY)

        lgb_board   = models['승차']['lgb']                  # LightGBM(승차)
        lgb_alight  = models['하차']['lgb']                  # LightGBM(하차)

        # 파생 컬럼/요일 one-hot
        df = _onehot_weekday(df)

        # 라벨 인코딩 및 미등록 라벨 제거
        df['호선'] = df['호선'].astype(str).str.strip()
        df['역명'] = df['역명'].astype(str).str.strip()
        line_enc, mask_line       = _safe_label_encode(le_line, df['호선'], "호선")
        station_enc, mask_station = _safe_label_encode(le_station, df['역명'], "역명")
        mask = mask_line & mask_station
        df = df[mask].copy()
        if df.empty:
            return {"status": "error", "message": "인코딩 가능한 행이 없음(모든 라벨이 미등록)"}
        df.loc[:, '호선_enc'] = line_enc[:len(df)]
        df.loc[:, '역명_enc'] = station_enc[:len(df)]

        # 결측/형 변환 & feature 정렬
        for col in ['기온','강수형태','강수','습도','풍속','공휴일여부']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            else:
                df[col] = 0

        for col in features:
            if col not in df.columns:
                df[col] = 0
        X = df[features].astype(float)

        # 예측
        y_board  = lgb_board.predict(X)
        y_alight = lgb_alight.predict(X)

        # 결과 생성
        out_rows = []
        for i, row in df.iterrows():
            day_str = str(pd.to_datetime(row['날짜']).date())
            out_rows.append({
                "날짜": day_str, "호선": row["호선"], "역명": row["역명"],
                "구분": "승차_lgb", "예측값": int(max(0, np.rint(y_board[i])))
            })
            out_rows.append({
                "날짜": day_str, "호선": row["호선"], "역명": row["역명"],
                "구분": "하차_lgb", "예측값": int(max(0, np.rint(y_alight[i])))
            })
        out_df = pd.DataFrame(out_rows)

        # 저장
        date_token = pd.to_datetime(df['날짜'].iloc[0]).strftime("%Y-%m-%d")
        out_key = f"{OUTPUT_PREFIX}/{date_token}_lgb.csv"
        _write_csv_to_s3(s3, out_df, out_key)

        return {"status": "ok", "input_key": in_key, "s3_key": out_key, "rows": int(len(out_df))}

    except Exception as e:
        return {"status": "error", "message": str(e)}
