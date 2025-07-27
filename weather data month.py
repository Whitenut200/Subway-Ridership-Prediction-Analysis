import os
import pandas as pd
from datetime import datetime
import re
from sqlalchemy import create_engine, text
import traceback

def clean_and_append_sql_monthly(base_folder, engine, table_name, year_month_folder):
    """
    월 단위 폴더 데이터 처리 함수
    year_month_folder: 예) "2025.7"
    """
    total_processed = 0
    
    # 월 단위 폴더 경로
    month_path = os.path.join(base_folder, year_month_folder)
    if not os.path.exists(month_path):
        print(f"📁 {year_month_folder} 폴더 없음")
        return
        
    print(f"\n🔄 {year_month_folder} 처리 시작...")
    combined_df = pd.DataFrame()
    files_processed = 0
    
    for file in os.listdir(month_path):
        if not file.endswith(".csv"):
            continue
            
        try:
            print(f"📄 처리중: {file}")
            
            # 1. 파일명에서 구분과 기준연월 추출
            parts = file.replace(".csv", "").split("_")
            if len(parts) < 3:
                print(f"⚠️ 파일명 형식 이상: {file}")
                continue
                
            구분 = parts[1]  # 두 번째 부분
            기준연월 = parts[2]  # 세 번째 부분
            
            try:
                # 기준연월이 6자리 형태인지 확인
                if len(기준연월) == 6:
                    기준년 = int(기준연월[:4])
                    기준월 = int(기준연월[4:])
                elif len(기준연월) == 8:
                    # YYYYMMDD 형태인 경우 (시작일-종료일 형태)
                    기준년 = int(기준연월[:4])
                    기준월 = int(기준연월[4:6])
                else:
                    print(f"⚠️ 기준연월 형식 불명: {기준연월} (길이: {len(기준연월)})")
                    continue
                
                print(f"   📅 파싱된 기준: {기준년}년 {기준월}월")
            except (ValueError, IndexError):
                print(f"⚠️ 기준연월 파싱 실패: {기준연월}")
                continue
            
            # CSV 파일 로드
            file_path = os.path.join(month_path, file)
            print(f"   📂 파일 경로: {file_path}")
            
            if not os.path.exists(file_path):
                print(f"❌ 파일이 존재하지 않음: {file_path}")
                continue
            
            # CSV 읽기 (인코딩 처리)
            try:
                df = pd.read_csv(file_path, encoding="utf-8", header=None)
                print(f"   📊 로드된 행 수: {len(df)}")
            except UnicodeDecodeError:
                try:
                    df = pd.read_csv(file_path, encoding="euc-kr", header=None)
                    print(f"   📊 로드된 행 수 (euc-kr): {len(df)}")
                except:
                    print(f"❌ 인코딩 오류: {file}")
                    continue
            except Exception as e:
                print(f"❌ CSV 읽기 실패: {file}, 오류: {e}")
                continue
            
            if df.empty:
                print(f"⚠️ 빈 파일: {file}")
                continue
            
            # 2. 열 이름 변경 (첫 번째 행이 헤더인지 확인)
            첫행 = df.iloc[0].astype(str).str.lower()
            
            if any('hour' in str(val).lower() for val in 첫행):
                # 첫 번째 행이 헤더인 경우
                print(f"   🏷️ 헤더 발견 - 첫 번째 행을 열 이름으로 사용")
                
                if len(df.columns) == 4:
                    # 4개 열: day, hour, forecast, value
                    df.columns = ['일', '시간', 'forecast', '값']
                    df = df.drop(columns=['forecast'])
                    print(f"   🗑️ forecast 열 제거")
                elif len(df.columns) == 3:
                    # 3개 열: day, hour, value  
                    df.columns = ['일', '시간', '값']
                else:
                    print(f"⚠️ 예상치 못한 컬럼 수: {len(df.columns)} in {file}")
                    continue
                
                # 헤더 행 제거
                df = df.drop(index=0).reset_index(drop=True)
                print(f"   🏷️ 헤더 행 제거 후 행수: {len(df)}")
                
            else:
                # 첫 번째 행이 헤더가 아닌 경우 (기본 열 이름 지정)
                if len(df.columns) == 4:
                    df.columns = ['일', '시간', 'forecast', '값']
                    df = df.drop(columns=['forecast'])
                    print(f"   🏷️ 4개 열 -> forecast 제거")
                elif len(df.columns) == 3:
                    df.columns = ['일', '시간', '값']
                    print(f"   🏷️ 3개 열 -> 이름 변경")
                else:
                    print(f"⚠️ 예상치 못한 컬럼 수: {len(df.columns)} in {file}")
                    continue
            
            # 3. 중복 'hour' 체크 (두 번째 등장부터는 중복 데이터)
            hour_indices = df[df['시간'].astype(str).str.contains('hour', case=False, na=False)].index.tolist()
            
            if hour_indices:
                # 첫 번째 'hour' 발견 시점부터 모든 데이터 제거 (중복 데이터)
                cut_idx = hour_indices[0]
                df = df.loc[:cut_idx-1]
                print(f"   🧹 중복 데이터 제거: {cut_idx}번째 행부터 삭제, 남은 행수: {len(df)}")
            else:
                print(f"   ✅ 중복 없음 - 정상 데이터")
            
            if df.empty:
                print(f"⚠️ 중복 제거 후 빈 데이터: {file}")
                continue
            
            # 4. '일' 변수에서 텍스트가 있는 행 제거 (월 단위 데이터 전용)
            print(f"   🧹 텍스트 행 제거 시작...")
            원본_행수 = len(df)
            
            # 숫자로 변환 가능한 행만 유지
            numeric_mask = pd.to_numeric(df['일'], errors='coerce').notna()
            df = df[numeric_mask].copy()
            
            제거된_행수 = 원본_행수 - len(df)
            print(f"   🗑️ 텍스트 행 제거: {제거된_행수}개 행 제거, 남은 행수: {len(df)}")
            
            if df.empty:
                print(f"⚠️ 텍스트 제거 후 빈 데이터: {file}")
                continue
            
            # 5. 날짜 변수 생성 (월 단위는 Start 패턴 없이 단순 처리)
            print(f"   📅 날짜 생성 시작 (기준: {기준년}년 {기준월}월)")
            날짜목록 = []
            
            for i, row in df.iterrows():
                일_값 = row['일']
                
                try:
                    일_숫자 = int(float(일_값))
                    if 1 <= 일_숫자 <= 31:
                        try:
                            날짜객체 = datetime(기준년, 기준월, 일_숫자)
                            날짜_문자열 = 날짜객체.strftime('%Y%m%d')  # YYYYMMDD 형식
                            날짜목록.append(날짜_문자열)
                        except ValueError:
                            # 유효하지 않은 날짜 (예: 2월 30일)
                            print(f"     ⚠️ 유효하지 않은 날짜: {기준년}-{기준월}-{일_숫자}")
                            날짜목록.append(None)
                    else:
                        print(f"     ⚠️ 범위 벗어난 일자: {일_숫자}")
                        날짜목록.append(None)
                except (ValueError, TypeError):
                    print(f"     ⚠️ 일자 변환 실패: '{일_값}'")
                    날짜목록.append(None)
            
            # 날짜 열 추가
            df['날짜'] = 날짜목록
            
            # 유효하지 않은 날짜 행 제거
            원본_행수 = len(df)
            df = df[df['날짜'].notna()].copy()
            print(f"   🗑️ 유효하지 않은 날짜 제거: {원본_행수} -> {len(df)}행")
            
            if df.empty:
                print(f"⚠️ 유효한 날짜 데이터 없음: {file}")
                continue
            
            # 추가 열 정보
            df['구분'] = 구분
            df['기준연월'] = 기준연월
            
            # 데이터 타입 정리
            df['값'] = pd.to_numeric(df['값'], errors='coerce')
            df = df.dropna(subset=['값'])  # 값이 None인 행 제거
            
            if not df.empty:
                combined_df = pd.concat([combined_df, df], ignore_index=True)
                files_processed += 1
                print(f"   ✅ 처리 완료: {len(df)}행 추가")
                
                # 샘플 데이터 출력 (처음 3행)
                print("   📋 샘플 데이터:")
                sample_df = df[['날짜', '시간', '구분', '값']].head(3)
                for _, row in sample_df.iterrows():
                    print(f"     날짜:{row['날짜']} 시간대:{row['시간']} 구분:{row['구분']} 값:{row['값']}")
            else:
                print(f"⚠️ 최종 데이터 없음: {file}")
                
        except Exception as e:
            print(f"❌ {file} 처리 실패: {e}")
            traceback.print_exc()
    
    # SQL로 저장
    if not combined_df.empty:
        try:
            print(f"\n💾 {year_month_folder} SQL 저장 시작... (총 {len(combined_df)}행)")
            
            # 컬럼 순서 정리 (원하는 4개 컬럼만)
            combined_df = combined_df[['날짜', '시간', '구분', '값']]
            
            
            # 데이터 타입 확인
            print(f"   📊 데이터 요약:")
            print(f"     - 총 행수: {len(combined_df)}")
            print(f"     - 날짜 범위: {min(combined_df['날짜'])} ~ {max(combined_df['날짜'])}")
            print(f"     - 구분 종류: {combined_df['구분'].unique()}")
            print(f"     - 시간 범위: {min(combined_df['시간'])} ~ {max(combined_df['시간'])}")
            print(f"     - 컬럼: {list(combined_df.columns)}")
            
            # SQL 저장
            combined_df.to_sql(
                name=table_name, 
                con=engine, 
                if_exists='append', 
                index=False,
                method='multi',
                chunksize=1000
            )
            
            total_processed += len(combined_df)
            print(f"✅ {year_month_folder} 데이터 SQL 저장 완료 ({files_processed}개 파일, {len(combined_df)}행)")
            
        except Exception as e:
            print(f"❌ {year_month_folder} SQL 저장 실패: {e}")
            traceback.print_exc()
    else:
        print(f"⚠️ {year_month_folder} 저장할 데이터 없음")
    
    print(f"\n🎉 {year_month_folder} 처리 완료! 총 {total_processed}행 처리됨")

# 사용법
if __name__ == "__main__":
    try:
        base_folder = "D:/SY 업무/기타/개인과제/subway/기후/0. Raw data"
        engine = create_engine("postgresql+psycopg2://postgres:0514@localhost:5432/postgres")
        
        # 연결 테스트
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("✅ DB 연결 성공")
        
        # 월 단위 함수 실행 (2025.7 폴더)
        clean_and_append_sql_monthly(
            base_folder=base_folder, 
            engine=engine, 
            table_name="weather_data_before",
            year_month_folder="2025.7"
        )
        
    except Exception as e:
        print(f"❌ 실행 실패: {e}")
        traceback.print_exc()