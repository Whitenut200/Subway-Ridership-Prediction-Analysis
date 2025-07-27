import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine

# API 인증키
service_key = "4e705665466b75793935526d785256"  # 여기에 실제 API 키 입력

# 오늘 날짜 기준 3일 전 (데이터가 2~3일 뒤 올라오니까)
target_date = datetime.today() - timedelta(days=4)
target_date_str = target_date.strftime("%Y%m%d")

# 요청 URL
url = f'http://openapi.seoul.go.kr:8088/{service_key}/json/CardSubwayStatsNew/1/999/{target_date_str}'

try:
    response = requests.get(url)
    data = response.json()

    if 'CardSubwayStatsNew' in data and 'row' in data['CardSubwayStatsNew']:
        new_data = pd.DataFrame(data['CardSubwayStatsNew']['row'])

        # 컬럼명 변경 (전처리)
        new_data.rename(columns={
            'USE_YMD': '사용일자',
            'SBWY_ROUT_LN_NM': '호선',
            'SBWY_STNS_NM': '역명',
            'GTON_TNOPE': '승차',
            'GTOFF_TNOPE': '하차',
            'REG_YMD': '갱신일'
        }, inplace=True)

        print(f"{target_date_str} → {new_data.shape[0]}건 수집 완료")

        # DB 연결 및 append 저장
        engine = create_engine("postgresql+psycopg2://postgres:0514@localhost:5432/postgres")
        new_data.to_sql("subway_stats", engine, if_exists="append", index=False)
        print("DB에 데이터 추가 완료")

    else:
        print(f"{target_date_str} → 데이터 없음")

except Exception as e:
    print(f"{target_date_str} → 오류 발생: {e}")
