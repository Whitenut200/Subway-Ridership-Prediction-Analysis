import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from sqlalchemy import text  
import holidays

# 환경 변수 또는 직접 키
SUBWAY_KEY = "지하철 API 키"  # 지하철 API 키
WEATHER_KEY = "날씨 API 키"  # 날씨 API 키

# RDS 설정
DB_USER = ""
DB_PASSWORD = ""
DB_HOST = ""
DB_PORT = "5432"
DB_NAME = "subway"
TABLE_NAME = "pred_data"

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")


def is_data_exists(table_name, date_column, target_date):
    # date_column::date 형변환 처리
    query = text(f"SELECT 1 FROM {table_name} WHERE {date_column}::date = :target_date LIMIT 1")
    with engine.connect() as conn:
        result = conn.execute(query, {"target_date": target_date})
        return result.fetchone() is not None
      
# 지하철 승하차 데이터
def fetch_subway_data():
    target_date = datetime.today() - timedelta(days=4)
    target_date_str = target_date.strftime("%Y%m%d")

    url = f"http://openapi.seoul.go.kr:8088/{SUBWAY_KEY}/json/CardSubwayStatsNew/1/999/{target_date_str}"
    response = requests.get(url)
    data = response.json()

    
    if 'CardSubwayStatsNew' in data and 'row' in data['CardSubwayStatsNew']:
        df = pd.DataFrame(data['CardSubwayStatsNew']['row'])
        df.rename(columns={
            'USE_YMD': '사용일자',
            'SBWY_ROUT_LN_NM': '호선',
            'SBWY_STNS_NM': '역명',
            'GTON_TNOPE': '승차',
            'GTOFF_TNOPE': '하차',
            'REG_YMD': '갱신일'
        }, inplace=True)

        # 날짜 타입 변환
        df['사용일자'] = pd.to_datetime(df['사용일자'], format="%Y%m%d")
        df['갱신일'] = pd.to_datetime(df['갱신일'], format="%Y%m%d")
        df = pd.melt(
            df,
            id_vars=['사용일자', '호선', '역명',"갱신일"],
            value_vars=['승차', '하차'],
            var_name='구분',
            value_name='인원수'
        )
        
        return df
    else:
        raise ValueError(f"{target_date_str} 지하철 데이터가 없습니다")

# 날씨 데이터
def fetch_weather_data():
    base_date = datetime.today().strftime('%Y%m%d')
    base_times = [f"{h:02}00" for h in range(24)]
    nx, ny = "60", "127"
    url = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"

    category_map = {
        "T1H": "기온",
        "RN1": "강수",
        "REH": "습도",
        "PTY": "강수 형태",
        "WSD": "풍속",
        "VEC": "풍향",
    }

    records = []

    for base_time in base_times:
        params = {
            "serviceKey": WEATHER_KEY,
            "dataType": "JSON",
            "base_date": base_date,
            "base_time": base_time,
            "nx": nx,
            "ny": ny,
            "numOfRows": "100"
        }
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                if data['response']['header']['resultCode'] == '00':
                    items = data['response']['body']['items']['item']
                    record = {"날짜": base_date, "시간": base_time}
                    for item in items:
                        cat = item['category']
                        val = item['obsrValue']
                        if cat in category_map:
                            record[category_map[cat]] = val
                    records.append(record)
                else:
                    print(f"{base_time}시 에러: {data['response']['header']['resultMsg']}")
            else:
                print(f"요청 실패 {base_time}시, 상태코드: {response.status_code}")
        except Exception as e:
            print(f"예외 발생 {base_time}시: {e}")

    if not records:
        raise ValueError("기상 데이터가 없습니다")

    df = pd.DataFrame(records)

    # melt 변환
    melt_df = pd.melt(df,
                      id_vars=['날짜', '시간'],
                      value_vars=['기온', '강수', '습도', '강수 형태', '풍속', '풍향'],
                      var_name='구분',
                      value_name='값')

    # 타입 전처리
    melt_df['날짜'] = pd.to_datetime(melt_df['날짜'], format="%Y%m%d")
    melt_df['시간'] = melt_df['시간'].astype(str)

    return melt_df

# 공휴일 데이터터
def fetch_holiday_data():
    today = datetime.today().date()
    weekday = today.strftime('%a')
    weekday_kor = {
        'Mon': '월', 'Tue': '화', 'Wed': '수',
        'Thu': '목', 'Fri': '금', 'Sat': '토', 'Sun': '일'
    }[weekday]

    kr_holidays = holidays.KR(years=[today.year])
    is_holiday = today in kr_holidays
    holiday_name = kr_holidays.get(today, None)

    df = pd.DataFrame([{
        '날짜': today,
        '요일': weekday_kor,
        '공휴일여부': is_holiday,
        '공휴일이름': holiday_name
    }])
    return df


# 중복 방지 추가
# 같은 날짜는 DB에 적재되지 않게 설정정
def lambda_handler(event, context): 
    try:
        # 지하철 (사용일자 = 오늘 - 4)
        subway_date = (datetime.today() - timedelta(days=4)).date()
        if not is_data_exists("subway_stats", "사용일자", subway_date):
            subway_df = fetch_subway_data()
            subway_df.to_sql("subway_stats", engine, if_exists="append", index=False)
        else:
            print(f"지하철 {subway_date} 데이터는 이미 존재합니다.")

        # 날씨 (날짜 = 오늘)
        weather_date = datetime.today().date()
        if not is_data_exists("weather_stats", "날짜", weather_date):
            weather_df = fetch_weather_data()
            weather_df.to_sql("weather_stats", engine, if_exists="append", index=False)
        else:
            print(f"날씨 {weather_date} 데이터는 이미 존재합니다.")

        # 공휴일 (날짜 = 오늘)
        holiday_date = datetime.today().date()
        if not is_data_exists("holidays_stats", "날짜", holiday_date):
            holiday_df = fetch_holiday_data()
            holiday_df.to_sql("holidays_stats", engine, if_exists="append", index=False)
        else:
            print(f"공휴일 {holiday_date} 데이터는 이미 존재합니다.")

        return {
            "statusCode": 200,
            "body": "지하철, 날씨, 공휴일 데이터 저장 완료 (중복 체크 완료)"
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"에러 발생: {str(e)}"
        }
