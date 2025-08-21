import os
import re
import io
import boto3
import pandas as pd
import numpy as np
import joblib
from datetime import datetime, timedelta
from io import BytesIO
from sqlalchemy import create_engine, text

# 설정
S3_BUCKET = "subway-whitenut-S3_BUCKET"
S3_KEY_PREFIX = "prepared_data"  # 결과 저장 폴더 (CSV)
INPUT_PREFIX = "prepared_data"
OUTPUT_PREFIX = "predictions"

# 모델/인코더 파일
MODEL_XGB_KEY = "model/model_xgb_only.joblib"
MODEL_LGB_KEY = "model/model_lgb_only.joblib"
FEATURES_KEY = "model/features.joblib"
LINE_ENCODER_KEY = "model/line_encoder.joblib"
STATION_ENCODER_KEY = "model/station_encoder.joblib"

# RDS 설정
DB_USER = ""
DB_PASSWORD = ""
DB_HOST = ""
DB_PORT = "5432"
DB_NAME = "subway"
TABLE_NAME = "pred_data"

def _read_csv_from_s3(s3, key, encoding="utf-8"):
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return pd.read_csv(BytesIO(obj["Body"].read()), encoding=encoding)

def _write_csv_to_s3(s3, df, key):
    data = df.to_csv(index=False).encode("utf-8")
    buf = BytesIO(data)
    s3.upload_fileobj(buf, S3_BUCKET, key, ExtraArgs={"ContentType": "text/csv; charset=utf-8"})

def _load_joblib(s3, key):
    buf = BytesIO()
    s3.download_fileobj(S3_BUCKET, key, buf)
    buf.seek(0)
    return joblib.load(buf)

# ===== 1단계: 전처리 =====
def preprocess(subway, weather, holiday, join_how='left'):
    # 컬럼 표준화
    if '사용일자' in subway.columns and '날짜' not in subway.columns:
        subway = subway.rename(columns={'사용일자': '날짜'})
    required_subway_cols = {'날짜', '호선', '역명'}
    missing = required_subway_cols - set(subway.columns)
    if missing:
        raise ValueError(f"subway 필수 컬럼 누락: {missing}")

    # 날짜 파싱/정규화
    subway['날짜'] = safe_to_datetime(subway['날짜']).dt.normalize()
    if '날짜' in weather.columns:
        weather['날짜'] = safe_to_datetime(weather['날짜']).dt.normalize()
    if '날짜' in holiday.columns:
        holiday['날짜'] = safe_to_datetime(holiday['날짜']).dt.normalize()

    # 날씨 피벗
    weather_daily = pd.DataFrame({'날짜': pd.Series(dtype='datetime64[ns]')})
    if {'구분','값','날짜'}.issubset(weather.columns):
        weather['구분'] = weather['구분'].astype(str).str.replace(" ", "", regex=False)
        weather_daily = (
            weather.pivot_table(index='날짜', columns='구분', values='값', aggfunc='mean')
                   .reset_index()
        )

    # 조인
    df = subway.merge(weather_daily, on='날짜', how=join_how)
    if '공휴일여부' in holiday.columns:
        df = df.merge(holiday[['날짜','공휴일여부']], on='날짜', how=join_how)
    else:
        tmp = holiday[['날짜']].drop_duplicates() if '날짜' in holiday.columns else pd.DataFrame(columns=['날짜'])
        tmp['공휴일여부'] = 'N'
        df = df.merge(tmp, on='날짜', how=join_how)

    # 공휴일여부 → 0/1
    df['공휴일여부'] = df['공휴일여부'].fillna('N').map({'Y': 1, 'N': 0}).fillna(0).astype(int)

    # 6) 날짜 파생
    df['년'] = df['날짜'].dt.year
    df['월'] = df['날짜'].dt.month
    df['일'] = df['날짜'].dt.day
    return df

def step1_preprocessing(target_date_str):
    """
    지정된 날짜에 대해 전처리 수행 후 S3에 저장
    target_date_str: "2025-08-13" 형식
    """
    print(f"전처리 시작 (대상날짜: {target_date_str})")
    
    s3 = boto3.client("s3")
    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    target_date = pd.to_datetime(target_date_str).normalize()
    # 전날까지의 데이터로 다음날 예측용 데이터 생성
    end_date = target_date - pd.Timedelta(days=1)
    start_date = end_date - pd.Timedelta(days=7)
    
    # DB에서 데이터 로드 (타입 캐스팅 추가)
    subway = pd.read_sql(
        text("SELECT 사용일자, 호선, 역명 FROM subway_stats WHERE 사용일자::date >= :sd AND 사용일자::date <= :ed"),
        engine, params={"sd": start_date.date(), "ed": end_date.date()}
    )
    
    if subway.empty:
        raise ValueError(f"subway 데이터 없음 ({start_date.date()} ~ {end_date.date()})")
    
    weather = pd.read_sql(
        text("SELECT 날짜, 구분, 값 FROM weather_stats WHERE 날짜::date >= :sd AND 날짜::date <= :td"),
        engine, params={"sd": start_date.date(), "td": target_date.date()}
    )
    
    holiday = pd.read_sql(
        text("SELECT 날짜, 공휴일여부 FROM holidays_stats WHERE 날짜::date >= :sd AND 날짜::date <= :td"),
        engine, params={"sd": start_date.date(), "td": target_date.date()}
    )
    
    # 전처리
    df_all = preprocess(subway, weather, holiday, join_how='left')
    
    # target_date에 해당하는 예측용 데이터 생성 (호선/역명은 마지막 날 기준)
    subway['사용일자'] = safe_to_datetime(subway['사용일자'])  # 날짜 파싱 먼저
    last_day_stations = subway[subway['사용일자'] == subway['사용일자'].max()][['호선', '역명']].drop_duplicates()
    
    # target_date의 날씨/공휴일 정보 가져오기
    weather_target = weather[weather['날짜'] == target_date] if not weather.empty else pd.DataFrame()
    holiday_target = holiday[holiday['날짜'] == target_date] if not holiday.empty else pd.DataFrame()
    
    # target_date 데이터 구성
    df_target_rows = []
    for _, station_row in last_day_stations.iterrows():
        target_row = {
            '날짜': target_date,
            '호선': station_row['호선'],
            '역명': station_row['역명'],
            '년': target_date.year,
            '월': target_date.month,
            '일': target_date.day,
        }
        
        # 날씨 정보 추가
        if not weather_target.empty:
            weather_pivot = weather_target.pivot_table(
                index='날짜', columns='구분', values='값', aggfunc='mean'
            ).reset_index()
            for col in weather_pivot.columns:
                if col != '날짜':
                    target_row[col] = weather_pivot[col].iloc[0] if not weather_pivot[col].isna().iloc[0] else 0
        
        # 공휴일 정보 추가
        if not holiday_target.empty and '공휴일여부' in holiday_target.columns:
            target_row['공휴일여부'] = 1 if holiday_target['공휴일여부'].iloc[0] == 'Y' else 0
        else:
            target_row['공휴일여부'] = 0
            
        df_target_rows.append(target_row)
    
    df_target = pd.DataFrame(df_target_rows)
    
    if df_target.empty:
        raise ValueError(f"{target_date_str} 예측용 데이터 생성 실패")
    
    # 기상 컬럼 기본값 설정
    for col in ['기온','강수형태','강수','습도','풍속']:
        if col not in df_target.columns:
            df_target[col] = 0
        else:
            df_target[col] = pd.to_numeric(df_target[col], errors='coerce').fillna(0)
    
    # S3에 저장
    s3_key = f"{INPUT_PREFIX}/{target_date_str}.csv"
    _write_csv_to_s3(s3, df_target, s3_key)
    
    print(f"전처리 완료: {len(df_target)}건 → {s3_key}")
    return s3_key

# ===== 2단계: 모델 예측 공통 함수 =====
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

def _safe_label_encode(le, series, name):
    series = series.astype(str).str.strip()
    known = set(le.classes_.tolist())
    mask = series.isin(known)
    if not mask.all():
        print(f"[WARN] unseen {name} labels dropped: {series[~mask].unique().tolist()}")
    enc = le.transform(series[mask])
    return enc, mask

def predict_with_model(s3_input_key, model_key, model_type):
    """
    model_type: 'xgb' 또는 'lgb'
    """
    print(f"🔄 Step 2-{model_type.upper()}: {model_type.upper()} 예측 시작")
    
    s3 = boto3.client("s3")
    
    # 입력 데이터 로드
    df = _read_csv_from_s3(s3, s3_input_key)
    if df.empty:
        raise ValueError("입력 CSV가 비어 있음")
    
    # 모델/인코더 로드
    models = _load_joblib(s3, model_key)
    features = _load_joblib(s3, FEATURES_KEY)
    le_line = _load_joblib(s3, LINE_ENCODER_KEY)
    le_station = _load_joblib(s3, STATION_ENCODER_KEY)
    
    if model_type == 'xgb':
        model_board = models['승차']['xgb']
        model_alight = models['하차']['xgb']
        구분_board = "승차_xgb"
        구분_alight = "하차_xgb"
    else:  # lgb
        model_board = models['승차']['출)
    
print(f"실행할 날짜: {TARGET_DATE}")
result = manual_prediction_pipeline(TARGET_DATE)
print("\n🏁 최종 결과:")
print(result)
