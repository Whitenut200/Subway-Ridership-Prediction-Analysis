import os
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
import traceback

# RDS 정보
DB_USER = ""
DB_PASSWORD = ""
DB_HOST = ""
DB_PORT = "5432"
DB_NAME = "subway"
engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def process_and_store_weather(base_folder, engine, table_name, year_month_folder):
    month_path = os.path.join(base_folder, year_month_folder)
    if not os.path.exists(month_path):
        print(f"폴더 없음: {month_path}")
        return
    
    combined_df = pd.DataFrame()
    
    for file in os.listdir(month_path):
        if not file.endswith(".csv"):
            continue
        
        try:
            print(f" 처리중: {file}")
            
            # 파일명 파싱
            parts = file.replace(".csv", "").split("_")
            if len(parts) < 3:
                print(f"파일명 이상: {file}")
                continue
            
            구분 = parts[1]
            기준연월 = parts[2]
            기준연도 = int(기준연월[:4])
            기준월 = int(기준연월[4:6])
            
            file_path = os.path.join(month_path, file)
            
            # CSV 읽기
            try:
                df = pd.read_csv(file_path, encoding='utf-8', header=None)
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, encoding='euc-kr', header=None)
            
            if df.empty or df.shape[1] < 4:
                continue
            
            # 열 이름 변경
            df = df.iloc[:, [0, 1, 3]]  # forecast 제거
            df.columns = ['일', '시간', '값']
            
            # 날짜 전처리
            df['일'] = df['일'].astype(str)
            
            # "start:..." 형태 제거
            df = df[~df['일'].str.lower().str.contains("start")].copy()
            
            # 숫자형 일자만 남기기
            df['일'] = pd.to_numeric(df['일'], errors='coerce')
            df = df[df['일'].notna()]
            df['일'] = df['일'].astype(int)
            df = df[(df['일'] >= 1) & (df['일'] <= 31)]
            
            # 날짜 생성
            df['날짜'] = df['일'].apply(
                lambda d: f"{기준연도:04d}{기준월:02d}{d:02d}"
            )
            
            # datetime 검증
            df['날짜_dt'] = pd.to_datetime(df['날짜'], format="%Y%m%d", errors='coerce')
            df = df[df['날짜_dt'].notna()]
            
            # 구분 추가
            df['구분'] = 구분
            
            # 값 형변환
            df['값'] = pd.to_numeric(df['값'], errors='coerce')
            df = df[df['값'].notna()]
            
            if df.empty:
                print(f"처리 결과 없음: {file}")
                continue
            
            df = df[['날짜', '시간', '구분', '값']]
            combined_df = pd.concat([combined_df, df], ignore_index=True)
            print(f"처리 완료")
        
        except Exception as e:
            print(f"{file} 처리 중 오류: {e}")
            traceback.print_exc()
    
    # SQL 저장
    if not combined_df.empty:
        try:
            print(f"SQL 저장 시작")
            combined_df.to_sql(
                name=table_name,
                con=engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            print(f"✅ SQL 저장 완료")
        except Exception as e:
            print(f"❌ SQL 저장 실패: {e}")
            traceback.print_exc()
    else:
        print("저장할 데이터 없음")

# 실행
if __name__ == "__main__":
    try:
        base_folder = "/0. Raw data"

        # 실행
        process_and_store_weather(
            base_folder=base_folder,
            engine=engine,
            table_name="weather_stats",
            year_month_folder="2025.8"
        )
        
    except Exception as e:
        print(f"실행 실패: {e}")
        traceback.print_exc()
