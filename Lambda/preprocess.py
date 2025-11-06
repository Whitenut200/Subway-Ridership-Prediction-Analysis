import boto3
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from io import BytesIO

# ==== 환경/상수 ====
DB_USER = ""
DB_PASSWORD = ""
DB_HOST = ""
DB_PORT = "5432"
DB_NAME = "subway"

S3_BUCKET = ""
S3_KEY_PREFIX = "prepared_data"  # 결과 저장 폴더 (CSV)
# ===================

def safe_to_datetime(series):
    # 빈 값들을 먼저 NaT로 처리
    series = series.astype(str)
    series = series.replace(['', 'nan', 'None', 'nat', 'NaT'], pd.NaT)
    
    # pandas의 to_datetime으로 변환 (에러는 NaT로)
    result = pd.to_datetime(series, errors='coerce')
    
    # 시간대 정보가 있으면 제거
    if result.dt.tz is not None:
        result = result.dt.tz_localize(None)
    
    return result

def lambda_handler(event, context):
    try:
        s3 = boto3.client("s3")
        engine = create_engine(
            f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )

        # subway_stats에서 max(사용일자)+1일
        # SQL에서 max(사용일자) 구해서 가져오기
        # 날짜가 (YYYYMMDD, YYYY-MM-DD HH:MI:SS) 두가지가 섞여있음
        q_max = text("""
            SELECT MAX(
                CASE
                    WHEN "사용일자" ~ '^[0-9]{8}$' THEN to_date("사용일자",'YYYYMMDD')
                    WHEN "사용일자" ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN to_date(left("사용일자",10),'YYYY-MM-DD')
                    ELSE NULL
                END
            ) AS max_date
            FROM subway_stats
        """)
      
        max_row = pd.read_sql(q_max, engine)
        max_date = max_row.loc[0, 'max_date']
      
        if pd.isna(max_date):
            return {"message": "subway_stats에서 max(사용일자) 계산 실패"}
          
        # target_date는 max_date +1일
        target_date = (pd.to_datetime(max_date) + pd.Timedelta(days=1)).normalize()

        # 역 목록 DISTINCT
        # target_date의 승하차수는 없음
        # 해당 요일의 승하차수를 예측할 예정
        stations = pd.read_sql(
            text('SELECT DISTINCT 호선, 역명 FROM subway_stats WHERE 역명 IS NOT NULL'),
            engine
        )
        if stations.empty:
            return {"message": "역 목록이 비어 있음"}

        # target_date의 weather/holiday만 로드
        weather = pd.read_sql(
            text("SELECT 날짜, 구분, 값 FROM weather_stats WHERE 날짜::date = :td"),
            engine, params={"td": target_date.date()}
        )
        holiday = pd.read_sql(
            text("SELECT 날짜, 공휴일여부 FROM holidays_stats WHERE 날짜::date = :td"),
            engine, params={"td": target_date.date()}
        )
        if weather.empty or holiday.empty:
            return {"status": "no_data",
                    "message": "해당 날짜의 날씨/공휴일 데이터 없음"}

        # 가공/머지
        weather['날짜'] = safe_to_datetime(weather['날짜']).dt.normalize()
        holiday['날짜'] = safe_to_datetime(holiday['날짜']).dt.normalize()
      
        # 구분의 "강수형태","강수 형태" 해당 값들 때문에 띄여쓰기 없애기
        weather['구분'] = weather['구분'].astype(str).str.replace(" ", "", regex=False)

        # 학습을 위해서 melt 풀어주기
        weather_daily = (
            weather.pivot_table(index='날짜', columns='구분', values='값', aggfunc='mean')
                   .reset_index()
        )
        # 역명, 호선명만 있는 테이블 가져오기
        df = stations.copy()
      
        df['날짜'] = target_date
        # weather과 station 조인
        # 조인한 테이블과 공휴일 데이터 조인
        # 모두 '날짜' 컬럼으로
        df = df.merge(weather_daily, on='날짜', how='left') \
               .merge(holiday[['날짜','공휴일여부']], on='날짜', how='left')

        # 공휴일여부 0/1
        df['공휴일여부'] = df['공휴일여부'].fillna('N').map({'Y': 1, 'N': 0}).fillna(0).astype(int)

        # 기상 수치 결측 0
        for col in ['기온','강수형태','강수','습도','풍속']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # 날짜 파생
        df['년'] = pd.to_datetime(df['날짜']).dt.year
        df['월'] = pd.to_datetime(df['날짜']).dt.month
        df['일'] = pd.to_datetime(df['날짜']).dt.day

        # S3 CSV 저장
        key = f"{S3_KEY_PREFIX}/{target_date.strftime('%Y-%m-%d')}.csv"
        buf = BytesIO()

        df.to_csv(buf, index=False)
        buf.seek(0)
        s3.upload_fileobj(buf, S3_BUCKET, key,
                          ExtraArgs={"ContentType": "text/csv; charset=utf-8"})

        return {"status": "prepared", "s3_key": key, "rows": int(len(df)), "target_date": str(target_date.date())}

    except Exception as e:
        return {"status": "error", "message": str(e)}
