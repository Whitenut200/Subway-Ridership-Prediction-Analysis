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

# 날짜 범위 및 공휴일 데이터 생성
date_range = pd.date_range(start='2020-01-01', end='2025-08-05')
kr_holidays = holidays.KR(years=range(2020, 2025))

df = pd.DataFrame({'date': date_range})
df['요일'] = df['date'].dt.strftime('%a')
df['공휴일여부'] = df['date'].isin(kr_holidays)
df['공휴일이름'] = df['date'].map(kr_holidays)

# 요일을 한국어로 변환
df['요일'] = df['요일'].map({
    'Mon': '월', 'Tue': '화', 'Wed': '수',
    'Thu': '목', 'Fri': '금', 'Sat': '토', 'Sun': '일'
})
df.rename(columns={"date": "날짜"}, inplace=True)


# 'holidays_stats'에 데이터 적재
# 처음이면 replace (대체), 추가하고 싶다면 append (기존데이터에 추가)
df.to_sql("holidays_stats", engine, if_exists="replace", index=False)

print("업로드 완료")
