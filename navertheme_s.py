'''
import pandas as pd

date_before = "20250107"
date_after = "20250107"
file_before = f'C:\\Users\\kos47\\Desktop\\galaxybook5\\code\\navertheme\\{date_before}.xlsx'
file_after = f'C:\\Users\\kos47\\Desktop\\galaxybook5\\code\\navertheme\\{date_after}.xlsx'
navertheme_df_all_before = pd.read_excel(file_before).iloc[:,2:]
navertheme_df_all_after = pd.read_excel(file_after).iloc[:,2:]
grouped_before = navertheme_df_all_before.groupby('THEME')['종목명'].count().reset_index()
grouped_after = navertheme_df_all_after.groupby('THEME')['종목명'].count().reset_index()
grouped_df = pd.merge(grouped_after, grouped_before, on = ['THEME'], how = 'left').fillna(0)
grouped_df.columns = ['THEME', 'after', 'before']
grouped_df['after'] = grouped_df['after'].astype(int)
grouped_df['before'] = grouped_df['before'].astype(int)
grouped_df['change'] = grouped_df['after'] - grouped_df['before']
grouped_df_chg = grouped_df[grouped_df['change'] != 0] # 변한 종목수

merged_df = pd.merge(navertheme_df_all_before, navertheme_df_all_after, on = ['THEME', '종목명', 'Code'], how = 'outer', indicator = True)
in_rows = merged_df[merged_df['_merge'] == 'right_only'].iloc[:,:2] # 새로생긴 종목
out_rows = merged_df[merged_df['_merge'] == 'left_only'].iloc[:,:2] # 사라진 종목
'''

import streamlit as st
import pandas as pd

def main():
    st.title("네이버테마 변화 분석")
    
    # 예시 날짜 리스트 및 문자열화
    example_dates = ['20250107','20250108','20250113','20250114','20250115','20250116','20250117','20250120','20250123','20250124','20250203','20250204','20250207','20250210','20250211','20250212','20250213','20250214','20250217','20250219','20250220','20250221','20250224','20250225','20250226','20250227','20250404','20250407','20250408']

    # 날짜 선택 입력: selectbox는 클릭 시 드롭다운으로 세로에 옵션이 나타납니다.
    date_before = st.selectbox("이전 날짜 입력", example_dates, index=0)
    date_after = st.selectbox("이후 날짜 입력", example_dates, index=0)
    
    # "조회" 버튼 클릭 시 실행
    if st.button("조회"):
        # 파일 경로 구성
        file_before = f'C:\\Users\\kos47\\Desktop\\galaxybook5\\code\\navertheme\\{date_before}.xlsx'
        file_after = f'C:\\Users\\kos47\\Desktop\\galaxybook5\\code\\navertheme\\{date_after}.xlsx'
        
        try:
            # Excel 파일 읽기, 필요한 컬럼만 사용 (세번째 컬럼부터 끝까지)
            navertheme_df_all_before = pd.read_excel(file_before).iloc[:, 2:]
            navertheme_df_all_after = pd.read_excel(file_after).iloc[:, 2:]
            
            # THEME 별 종목 수를 그룹화하여 집계
            grouped_before = navertheme_df_all_before.groupby('THEME')['종목명'].count().reset_index()
            grouped_after = navertheme_df_all_after.groupby('THEME')['종목명'].count().reset_index()
            
            # 두 결과를 병합
            grouped_df = pd.merge(grouped_after, grouped_before, on='THEME', how='left').fillna(0)
            grouped_df.columns = ['THEME', 'after', 'before']
            grouped_df['after'] = grouped_df['after'].astype(int)
            grouped_df['before'] = grouped_df['before'].astype(int)
            grouped_df['change'] = grouped_df['after'] - grouped_df['before']
            
            # 변화가 있는 THEME만 추출 (변경된 종목수)
            grouped_df_chg = grouped_df[grouped_df['change'] != 0]
            grouped_df_chg.reset_index(drop=True)
            
            # 두 데이터프레임을 종목명과 Code, THEME 기준으로 outer join
            merged_df = pd.merge(navertheme_df_all_before,
                                 navertheme_df_all_after,
                                 on=['THEME', '종목명', 'Code'],
                                 how='outer',
                                 indicator=True)
            
            # 오른쪽에만 있는 행: 새로 생긴 종목
            in_rows = merged_df[merged_df['_merge'] == 'right_only'].iloc[:, :2]
            in_rows.reset_index(drop=True)
            # 왼쪽에만 있는 행: 사라진 종목
            out_rows = merged_df[merged_df['_merge'] == 'left_only'].iloc[:, :2]
            out_rows.reset_index(drop=True)
            
            # 결과 데이터프레임 출력
            st.subheader("변경된 종목 수")
            st.dataframe(grouped_df_chg)
            
            st.subheader("새로 생긴 종목")
            st.dataframe(in_rows)
            
            st.subheader("사라진 종목")
            st.dataframe(out_rows)
            
        except Exception as e:
            st.error(f"오류가 발생했습니다: {e}")

if __name__ == "__main__":
    main()