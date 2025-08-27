import re
import io
import boto3
import pandas as pd
from sqlalchemy import create_engine, text

# ==== 설정 ====
S3_BUCKET = "subway-whitenut-bucket"
PREDICTIONS_PREFIX = "predictions/"
# RDS
DB_USER = ""
DB_PASSWORD = ""
DB_HOST = ""
DB_PORT = "5432"
DB_NAME = "subway"
TABLE_NAME = "pred_data"
# =============

pat = re.compile(r"(?P<date>\d{4}-\d{2}-\d{2})_(?P<model>xgb|lgb)\.csv$")

def _list_prediction_keys(s3):
    pg = s3.get_paginator("list_objects_v2")
    keys = []
    for page in pg.paginate(Bucket=S3_BUCKET, Prefix=PREDICTIONS_PREFIX):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".csv"):
                m = pat.search(k)
                if m:
                    keys.append(k)
    return keys

# xgb와 lgb가 모두 존재하는 최근 날짜 가져오기
def _find_latest_pair(keys):
    by_date = {}
    for k in keys:
        m = pat.search(k)
        if not m:
            continue
        d = m.group("date")
        model = m.group("model")
        by_date.setdefault(d, set()).add(model)

    # 두 모델 다 있는 날짜만 후보
    candidates = [d for d, models in by_date.items() if {"xgb", "lgb"} <= models]
    if not candidates:
        return None, None, None

    latest = max(candidates)  # YYYY-MM-DD 문자열은 사전순=시간순
    # 실제 키 찾아서 리턴
    xgb_key = f"{PREDICTIONS_PREFIX}{latest}_xgb.csv"
    lgb_key = f"{PREDICTIONS_PREFIX}{latest}_lgb.csv"
    return latest, xgb_key, lgb_key

def _read_csv(s3, key):
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()), encoding="utf-8")

# 날짜 중복 체크 후 새 날짜 데이터만 DB에 저장
def _write_db(df):
    if df.empty:
        print("저장할 데이터 없음")
        return
    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    # 예측 날짜 추출
    target_date = df["날짜"].iloc[0]
    
    try:
        with engine.begin() as conn:
            # 해당 날짜 데이터가 이미 존재하는지 확인
            check_query = text("""
                SELECT COUNT(*) as count 
                FROM pred_data 
                WHERE 날짜::text = :target_date
            """)
            
            result = conn.execute(check_query, {"target_date": str(target_date)})
            existing_count = result.fetchone()[0]
            
            # 날짜별 중복 체크
            if existing_count > 0:
                print(f"{target_date} 날짜 데이터가 이미 {existing_count}건 존재합니다. 저장 건너뜀.")
                return
            else:
                # 새 날짜 데이터 저장
                df.to_sql("pred_data", con=conn, if_exists="append", index=False)
                print(f"{target_date} 날짜 예측 데이터 {len(df)}건 저장 완료")
                
    except Exception as e:
        print(f"DB 저장 중 오류 발생: {str(e)}")
        raise e  # 상위에서 처리하도록 예외 재발생
                
    except Exception as e:
        print(f"DB 저장 중 오류 발생: {str(e)}")
        raise e  # 상위에서 처리하도록 예외 재발생

def lambda_handler(event, context):
    try:
        s3 = boto3.client("s3")

        # 날짜 결정
        forced_date = (event or {}).get("date")
        if forced_date:
            xgb_key = f"{PREDICTIONS_PREFIX}{forced_date}_xgb.csv"
            lgb_key = f"{PREDICTIONS_PREFIX}{forced_date}_lgb.csv"
        else:
            keys = _list_prediction_keys(s3)
            latest, xgb_key, lgb_key = _find_latest_pair(keys)
            if not latest:
                return {"status":"error","message":"최근 날짜 쌍(xgb,lgb)을 찾지 못함"}

        # 읽기
        df_xgb = _read_csv(s3, xgb_key)
        df_lgb = _read_csv(s3, lgb_key)

        # 검증 & 정리
        required_cols = ["날짜","호선","역명","구분","예측값"]
        for name, df in [("xgb", df_xgb), ("lgb", df_lgb)]:
            missing = [c for c in required_cols if c not in df.columns]
            if missing:
                return {"status":"error","message":f"{name} 컬럼 누락: {missing}"}

        # 타입 보정
        for df in (df_xgb, df_lgb):
            df["날짜"] = pd.to_datetime(df["날짜"]).dt.date
            df["예측값"] = pd.to_numeric(df["예측값"], errors="coerce").fillna(0).astype(int)
            df["호선"] = df["호선"].astype(str)
            df["역명"] = df["역명"].astype(str)
            df["구분"] = df["구분"].astype(str)

        # 유니온
        df_all = pd.concat([df_xgb, df_lgb], ignore_index=True)
        df_all = df_all.rename(columns={"구분": "target_model"})
        
        # DB 저장
        _write_db(df_all)

        target_date = str(df_all["날짜"].iloc[0])
        return {
            "status":"ok",
            "date": target_date,
            "rows": int(len(df_all)),
            "xgb_key": xgb_key,
            "lgb_key": lgb_key,
            "table": TABLE_NAME
        }

    except Exception as e:
        return {"status":"error","message":str(e)}
