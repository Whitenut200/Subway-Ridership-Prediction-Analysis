import pandas as pd
from datetime import datetime
import holidays
from sqlalchemy import create_engine

# RDS 접속 정보
DB_USER = ""
DB_PASSWORD = ""
DB_HOST = ""
DB_PORT = "5432"
DB_NAME = "subway"
engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# 특정 날짜 (예: 2025-08-19)
target_date = datetime(2025, 8, 16).date()
# 공휴일 데이터 생성
kr_holidays = holidays.KR(years=[target_date.year])

df = pd.DataFrame({'날짜': [pd.to_datetime(target_date)]})

df['요일'] = df['날짜'].dt.strftime('%a')
df['공휴일여부'] = df['날짜'].isin(kr_holidays)
df['공휴일이름'] = df['날짜'].map(kr_holidays)

# 요일을 한국어로 변환
df['요일'] = df['요일'].map({
    'Mon': '월', 'Tue': '화', 'Wed': '수',
    'Thu': '목', 'Fri': '금', 'Sat': '토', 'Sun': '일'
})

# 하루 데이터만 적재 (append)
df.to_sql("holidays_stats", engine, if_exists="append", index=False)

print("데이터 업로드 완료!".format(target_date))
