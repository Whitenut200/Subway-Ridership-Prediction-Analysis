import requests
import pandas as pd
from datetime import datetime, timedelta
import lxml
import requests
from bs4 import BeautifulSoup
import time
import math
from sqlalchemy import create_engine

# RDS 접속 정보
DB_USER = ""
DB_PASSWORD = ""
DB_HOST = ""
DB_PORT = "5432"
DB_NAME = "subway"
engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# 에러가 나서 수집이 필요한 요일 설정
start_date = datetime.strptime("20250812", "%Y%m%d")
end_date   = datetime.strptime("20250812", "%Y%m%d")

# 누적할 데이터프레임
all_data = pd.DataFrame()

# 날짜별로 반복
date = start_date

# API 인증키
service_key = "API 인증키"  # 여기에 실제 API 키 입력

while date <= end_date:
    date_str = date.strftime("%Y%m%d")
    url = f'http://openapi.seoul.go.kr:8088/{service_key}/json/CardSubwayStatsNew/1/999/{date_str}' # 오픈 API URL

    try:
        response = requests.get(url)
        data = response.json()

        # 유효한 데이터인지 확인
        if 'CardSubwayStatsNew' in data and 'row' in data['CardSubwayStatsNew']:
            day_df = pd.DataFrame(data['CardSubwayStatsNew']['row'])
            all_data = pd.concat([all_data, day_df], ignore_index=True) 
            print(f"{date_str} → {day_df.shape[0]}건 수집")
        else:
            print(f"{date_str} → 데이터 없음")

    except Exception as e:
        print(f"{date_str} → 오류 발생: {e}")

    date += timedelta(days=1)

print("전체 수집 완료:", all_data.shape)

# 누적 완료된 all_data를 기준으로 전처리
all_data.rename(columns={
    'USE_YMD': '사용일자',
    'SBWY_ROUT_LN_NM': '호선',
    'SBWY_STNS_NM': '역명',
    'GTON_TNOPE': '승차',
    'GTOFF_TNOPE': '하차',
    'REG_YMD': '갱신일'
}, inplace=True)

# 시각화를 위해 melt후 저장
all_data = pd.melt(
    all_data,
    id_vars=['사용일자', '호선', '역명',"갱신일"],
    value_vars=['승차', '하차'],
    var_name='구분',
    value_name='인원수'
)

# 'subway_stats'에 데이터 적재
# 무조건 append!!!
all_data.to_sql("subway_stats", engine, if_exists="append", index=False)  
