import requests
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

# 기상청 API 인증키 (URL 디코딩된 값 사용)
SERVICE_KEY = "MQaSm5CQsZEOOBbH0HANe2OElNuJQEri/3nFw6pOW3ecMhoKLzclwwsJr9kujnNktOk7Tp7Uqr3mEQFzCSX2Ig=="  # 디코딩된 인증키 입력

# 날짜 및 시간 설정
base_date = datetime.today().strftime('%Y%m%d')
base_times = [f"{h:02}00" for h in range(24)]  # 0시~23시

# 서울 격자 좌표
nx, ny = "60", "127"

url = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"

# 항목 코드 → 한글 이름 매핑
category_map = {
    "T1H": "기온",
    "RN1": "강수",
    "REH": "습도",
    "PTY": "강수 형태",
    "WSD": "풍속",
    "VEC": "풍향",
}

# 전체 결과 저장
weather_records = []

for base_time in base_times:
    params = {
        "serviceKey": SERVICE_KEY,
        "dataType": "JSON",
        "base_date": base_date,
        "base_time": base_time,
        "nx": nx,
        "ny": ny,
        "numOfRows": "100"
    }

    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        if data['response']['header']['resultCode'] == '00':
            items = data['response']['body']['items']['item']
            weather_dict = {"날짜": base_date, "시간": base_time}

            for item in items:
                cat = item['category']
                val = item['obsrValue']
                if cat in category_map:
                    weather_dict[category_map[cat]] = val

            weather_records.append(weather_dict)
        else:
            print(f"{base_time} 시 데이터 없음 또는 에러")
    else:
        print(f"{base_time} 시 요청 실패, 코드: {response.status_code}")

# DataFrame으로 변환
weather_df = pd.DataFrame(weather_records)
# Melt 작업 (Long format으로 변환)
weather_melt_df = pd.melt(weather_df, 
                  id_vars=['날짜', '시간'],      # 고정할 컬럼들
                  value_vars=['기온','강수','습도','강수 형태','풍속','풍향'],  # 변환할 컬럼들
                  var_name='구분',               # 변수명 컬럼 이름
                  value_name='값')          # 값 컬럼 이름
print(weather_melt_df)



# DB 연결 및 append 저장
engine = create_engine("postgresql+psycopg2://postgres:0514@localhost:5432/postgres")
weather_melt_df.to_sql("weather_stats", engine, if_exists="append", index=False)
print("DB에 데이터 추가 완료")