import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import lightgbm as lgb
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import LabelEncoder
from datetime import datetime, timedelta
import boto3
from io import BytesIO
import joblib

# RDS 설정
DB_USER = ""
DB_PASSWORD = ""
DB_HOST = ""
DB_PORT = "5432"
DB_NAME = "subway"
TABLE_NAME = "pred_data"

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# 데이터 불러오기
subway = pd.read_sql("SELECT 사용일자, 역명, 호선, 구분, 인원수 FROM subway_stats", engine)
weather = pd.read_sql("SELECT * FROM weather_stats", engine)
holiday = pd.read_sql("SELECT * FROM holidays_stats", engine)

# 전처리 함수
def preprocess(subway, weather, holiday):
    subway.rename(columns={'사용일자': '날짜'}, inplace=True)
    weather['구분'] = weather['구분'].str.replace(" ", "")
    
    # 데이터 병합
    subway['날짜'] = pd.to_datetime(subway['날짜'], format='mixed')
    weather['날짜'] = pd.to_datetime(weather['날짜'], format='mixed')
    holiday['날짜'] = pd.to_datetime(holiday['날짜'], format='mixed')
    
    # 시간 단위 → 일 단위 평균/최대값 집계
    weather_daily = weather.pivot_table(
        index='날짜',
        columns='구분',
        values='값',
        aggfunc='mean'  # or max depending on category
    ).reset_index()

    subway_daily = subway.pivot_table(
        index=['날짜','호선','역명'],
        columns='구분',
        values='인원수',
    ).reset_index()
    
    # subway 기준으로 합치기
    df = subway_daily.merge(weather_daily, on='날짜', how='left')
    df = df.merge(holiday, on='날짜', how='left')
    df['날짜'] = df['날짜'].dt.strftime('%Y%m%d')
    
    df['공휴일여부'] = df['공휴일여부'].map({'Y': 1, 'N': 0})
    df['날짜'] = pd.to_datetime(df['날짜'])
    
    # 날짜에서 필요한 숫자형 파생변수 생성 예시
    df['년'] = df['날짜'].dt.year
    df['월'] = df['날짜'].dt.month
    df['일'] = df['날짜'].dt.day

    return df

df=preprocess(subway, weather, holiday)
# 인코딩
# 호선, 역명은 Label Encoding
le_line = LabelEncoder()
df['호선_enc'] = le_line.fit_transform(df['호선'])
le_station = LabelEncoder()
df['역명_enc'] = le_station.fit_transform(df['역명'])

# 요일은 One-Hot Encoding
df = pd.get_dummies(df, columns=['요일'])
print(df)
# Feature와 Target 정의
features = ['년','월','일',
    '공휴일여부',
    '기온',
    '강수형태',
    '강수',
    '습도',
    '풍속',
    '호선_enc',
    '역명_enc',
    '요일_월', '요일_화', '요일_수', '요일_목', '요일_금', '요일_토', '요일_일'
]

# 승차와 하차 각각에 대한 모델 학습
def train_models(df, features):
    """
    승차와 하차 각각에 대해 XGBoost와 LightGBM 모델 학습
    """
    models = {}
    
    for target in ['승차', '하차']:
        print(f"\n=== {target} 예측 모델 학습 ===")
        
        X = df[features]
        y = df[target]
        
        # 학습/검증 데이터 분리
        X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, shuffle=False)
        
        models[target] = {}
        
        # XGBoost 모델
        print("XGBoost 학습 중...")
        xgb_model = xgb.XGBRegressor(
            random_state=42,
            n_estimators=100,
            learning_rate=0.1,
            max_depth=6,
            n_jobs=-1
        )
        xgb_model.fit(X_train, y_train)
        
        xgb_pred = xgb_model.predict(X_val)
        xgb_rmse = np.sqrt(mean_squared_error(y_val, xgb_pred))
        print(f"XGBoost {target} 검증 RMSE: {xgb_rmse:.2f}")
        
        models[target]['xgb'] = xgb_model
        
        # LightGBM 모델
        print("LightGBM 학습 중...")
        lgb_model = lgb.LGBMRegressor(
            random_state=42,
            n_estimators=100,
            learning_rate=0.1,
            max_depth=6,
            verbose=-1
        )
        lgb_model.fit(X_train, y_train)
        
        lgb_pred = lgb_model.predict(X_val)
        lgb_rmse = np.sqrt(mean_squared_error(y_val, lgb_pred))
        print(f"LightGBM {target} 검증 RMSE: {lgb_rmse:.2f}")
        
        models[target]['lgb'] = lgb_model
        
        # 앙상블 성능
        ensemble_pred = (xgb_pred + lgb_pred) / 2
        ensemble_rmse = np.sqrt(mean_squared_error(y_val, ensemble_pred))
        print(f"Ensemble {target} 검증 RMSE: {ensemble_rmse:.2f}")
    
    return models

# 모델 학습
models = train_models(df, features)

# S3 저장
# XGB / LGB 따로
bucket = "subway-whitenut-bucket"
prefix = ""  # 루트에 저장

def save_to_s3_split(models, le_line, le_station, features, bucket, prefix=""):
    s3 = boto3.client('s3')

    def upload_joblib(obj, key):
        buf = BytesIO()
        joblib.dump(obj, buf, compress=3, protocol=4)
        buf.seek(0)
        s3.upload_fileobj(buf, bucket, key)

    # XGBoost만
    xgb_only = {
        "승차": {"xgb": models["승차"]["xgb"]},
        "하차": {"xgb": models["하차"]["xgb"]}
    }
    upload_joblib(xgb_only, f"{prefix}model/model_xgb_only.joblib")

    # LightGBM만
    lgb_only = {
        "승차": {"lgb": models["승차"]["lgb"]},
        "하차": {"lgb": models["하차"]["lgb"]}
    }
    upload_joblib(lgb_only, f"{prefix}model/model_lgb_only.joblib")

    # 인코더/피처
    upload_joblib(le_line,     f"{prefix}model/line_encoder.joblib")
    upload_joblib(le_station,  f"{prefix}model/station_encoder.joblib")
    upload_joblib(features,    f"{prefix}model/features.joblib")

# 저장 실행
save_to_s3_split(models, le_line, le_station, features, bucket, prefix)
