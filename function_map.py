import pandas as pd

def load_data(file_path):
    tri_qw = pd.read_excel(file_path, sheet_name='QW')
    # tri_qw_price = pd.read_excel(file_path, sheet_name='QW2')
    tri_qw_price = pd.read_excel(file_path, sheet_name='QW22')
    # tri_qw_eps = pd.read_excel(file_path, sheet_name='QW3')
    tri_qw_eps = pd.read_excel(file_path, sheet_name='QW33')

    tri_qw_price = tri_qw_price[11:]
    tri_qw_price.rename(columns = tri_qw_price.iloc[0], inplace = True)
    tri_qw_price = tri_qw_price[1:].drop(tri_qw_price.columns[1:3], axis=1)
    tri_qw_price.columns = (
        ['Code'] +
        [f'시가총액{i}' for i in range(len(tri_qw_price.columns)-1, 0, -1)]
        )
    tri_qw_eps = tri_qw_eps[11:]
    tri_qw_eps.rename(columns = tri_qw_eps.iloc[0], inplace = True)
    tri_qw_eps = tri_qw_eps[1:].drop(tri_qw_eps.columns[1:3], axis=1)
    tri_qw_eps.columns = (
        ['Code'] +
        [f'12MF지배당기순이익{i}' for i in range(len(tri_qw_eps.columns)-1, 0, -1)]
        )
    num = len(tri_qw_price.columns)-1

    return tri_qw_eps, tri_qw_price, num

def load_data_pms(file_path, tri_qw_eps, tri_qw_price):
    allstock = pd.read_excel(file_path, sheet_name='PMS')

    allstock['종목'] = allstock['종목'].apply(lambda x: 'A' + str(x).zfill(6) if not x == '코드' else x)
    allstock.rename(columns = allstock.iloc[0], inplace = True)
    allstock = allstock[1:].iloc[:,[2,3,4,7]]
    col = allstock.columns.to_numpy()
    col2 = col[[1,2,0,3]]
    allstock = allstock[col2]
    allstock.rename(columns={allstock.columns[0]: 'Code'}, inplace=True)
    allstock.rename(columns={allstock.columns[2]: 'PMS'}, inplace=True)

    allstock_eps = pd.merge(allstock, tri_qw_eps, how='left', on='Code')
    allstock_eps = allstock_eps.dropna() # 12MF지배당기순이익이 존재하는 종목들로만 
    col = allstock_eps.columns.to_numpy()
    col2 = col[[0,2,3,1]]
    allstock_eps = pd.concat([allstock_eps[col2] ,allstock_eps.iloc[:,4:]], axis = 1)

    allstock_price = pd.merge(allstock, tri_qw_price, how='left', on='Code')
    allstock_price = allstock_price.dropna()
    allstock_price = allstock_price.drop(allstock_price.columns[[1,2,3]], axis=1)

    return allstock_eps, allstock_price


def process_data(eps_data, price_data, num):
    data = pd.merge(eps_data, price_data, how='left', on='Code')
    data = pd.concat([data.iloc[:, 1:4], data.iloc[:, 4:4 + num * 2].apply(pd.to_numeric)], axis=1)
    return data

def process_data2(benchmark, data, num):
    data.rename(columns={data.columns[0]: '그룹명'}, inplace=True)
    data = pd.concat([benchmark, data])
    data_sum = data.groupby('그룹명').sum(numeric_only=True).reset_index(drop=False)

    for i in range(1, num + 1):
        data_sum['12MFPER' + str(num + 1 - i)] = (
            data_sum['시가총액' + str(num + 1 - i)] / 
            data_sum['12MF지배당기순이익' + str(num + 1 - i)] / 
            1000
            )
        data_sum.replace([float('inf'), -float('inf')], '', inplace=True)

    benchmark_row = data_sum[data_sum['그룹명'] == '벤치마크']
    benchmark_rest = data_sum[data_sum['그룹명'] != '벤치마크']
    data_sum = pd.concat([benchmark_row, benchmark_rest], ignore_index=True)
    
    return data_sum

def process_data3(data, num):
    # EPS 처리
    ind_sum_eps = pd.concat([data.iloc[:, 0], data.iloc[:, 1:num+1]], axis=1).T
    ind_sum_eps.rename(columns=ind_sum_eps.iloc[0], inplace=True)
    ind_sum_eps = ind_sum_eps[1:]
    # 시가총액 처리
    ind_sum_price = pd.concat([data.iloc[:, 0], data.iloc[:, 1+num:num*2+1]], axis=1).T
    ind_sum_price.rename(columns=ind_sum_price.iloc[0], inplace=True)
    ind_sum_price = ind_sum_price[1:]
    # # PER 처리 제외
    ind_sum_per = pd.concat([data.iloc[:, 0], data.iloc[:, 1+num*2:num*3+1]], axis=1).T
    ind_sum_per.rename(columns=ind_sum_per.iloc[0], inplace=True)
    ind_sum_per = ind_sum_per[1:]

    return ind_sum_eps, ind_sum_price, ind_sum_per


def format_top3(group):
    return pd.concat(
        [
            group.nlargest(3, col)[['종목명', col]]
                .assign(**{col: lambda x: x['종목명'] + "(" + x[col].round(1).astype(str) + ")"})
                .drop(columns=['종목명'])
                .reset_index(drop=True)
            for col in group.columns if 'Chg' in col
        ],
        axis=1
    )

def format_bottom3(group):
    return pd.concat(
        [
            group.nsmallest(3, col)[['종목명', col]]
                .assign(**{col: lambda x: x['종목명'] + "(" + x[col].round(1).astype(str) + ")"})
                .drop(columns=['종목명'])
                .reset_index(drop=True)
            for col in group.columns if 'Chg' in col
        ],
        axis=1
    )

def process_data_tb(data, num):
    data.rename(columns={data.columns[0]: '그룹명'}, inplace=True)
    # sector_tb에서 따로 per 구하지말고 sector_df에서 해도 되는데 process_data2 함수에서는 어차피 벤치마크 까지 더해서 per 구해야 하니까까
    for i in range(1, num + 1):
        data['12MFPER' + str(num + 1 - i)] = (
            data['시가총액' + str(num + 1 - i)] / 
            data['12MF지배당기순이익' + str(num + 1 - i)] / 
            1000
            )
        data.replace([float('inf'), -float('inf')], '', inplace=True)
    
    # # EPS 처리
    columns_to_calculate = ['12MF지배당기순이익6', '12MF지배당기순이익21', '12MF지배당기순이익61', '12MF지배당기순이익121', '12MF지배당기순이익181', '12MF지배당기순이익241']
    sector_eps = pd.concat([data.iloc[:, [0,2]], data.iloc[:, 3:num+3]], axis=1)
    for col in columns_to_calculate:
        new_col_name = f'{col}_Chg'
        sector_eps[new_col_name] = (sector_eps['12MF지배당기순이익1'] - sector_eps[col]) / sector_eps[col] * 100
    sector_eps = pd.concat([sector_eps.iloc[:,[0,1]], sector_eps.iloc[:,[-6,-5,-4,-3,-2,-1]]], axis=1)
    sector_top3_eps = sector_eps.groupby('그룹명').apply(format_top3).reset_index(level=1, drop=True)
    sector_bottom3_eps = sector_eps.groupby('그룹명').apply(format_bottom3).reset_index(level=1, drop=True)
    
    # 시가총액 처리
    columns_to_calculate = ['시가총액6', '시가총액21', '시가총액61', '시가총액121', '시가총액181', '시가총액241']
    sector_price = pd.concat([data.iloc[:, [0,2]], data.iloc[:, 3+num:num*2+3]], axis=1)
    for col in columns_to_calculate:
        new_col_name = f'{col}_Chg'
        sector_price[new_col_name] = (sector_price['시가총액1'] - sector_price[col]) / sector_price[col] * 100
    sector_price = pd.concat([sector_price.iloc[:,[0,1]], sector_price.iloc[:,[-6,-5,-4,-3,-2,-1]]], axis=1)
    sector_top3_price = sector_price.groupby('그룹명').apply(format_top3).reset_index(level=1, drop=True)
    sector_bottom3_price = sector_price.groupby('그룹명').apply(format_bottom3).reset_index(level=1, drop=True)

    # PER 처리
    columns_to_calculate = ['12MFPER6', '12MFPER21', '12MFPER61', '12MFPER121', '12MFPER181', '12MFPER241']
    sector_per = pd.concat([data.iloc[:, [0,2]], data.iloc[:, 3+num*2:num*3+3]], axis=1)
    for col in columns_to_calculate:
        new_col_name = f'{col}_Chg'
        sector_per[new_col_name] = (sector_per['12MFPER1'] - sector_per[col]) / sector_per[col] * 100
    sector_per = pd.concat([sector_per.iloc[:,[0,1]], sector_per.iloc[:,[-6,-5,-4,-3,-2,-1]]], axis=1)
    sector_top3_per = sector_per.groupby('그룹명').apply(format_top3).reset_index(level=1, drop=True)
    sector_bottom3_per = sector_per.groupby('그룹명').apply(format_bottom3).reset_index(level=1, drop=True)

    return sector_top3_eps, sector_bottom3_eps, sector_top3_price, sector_bottom3_price, sector_top3_per, sector_bottom3_per


# 이동 평균 계산
# 5개씩 이동 평균을 계산하는 함수
# 주단위 말고 일단위 에서는 20~30개씩 까지 늘려서도 시험
def moving_average(data_list, window_size):
    moving_averages = []
    for i in range(len(data_list) - window_size + 1):
        window = data_list[i:i + window_size]  # 현재 위치에서 5개의 값을 추출
        window_average = sum(window) / window_size  # 평균 계산
        moving_averages.append(window_average)
    return moving_averages

def process_rs(data, benchmark_data, tickers, window, window_ma):
    rs_tickers = []
    rsr_tickers = []
    rsr_roc_tickers = []
    rsm_tickers = []

    for i in range(len(tickers)):
        # 벤치마크대비 비중 계산
        rs_tickers.append(100 * (data[tickers[i]] / benchmark_data['벤치마크']))
        # 표준화
        rsr_tickers.append((100 + (rs_tickers[i] - rs_tickers[i].rolling(window=window).mean()) / rs_tickers[i].rolling(window=window).std(ddof=0)).dropna())
        # rsr_roc_tickers.append(100 * ((rsr_tickers[i] / rsr_tickers[i].iloc[1]) - 1))  
        # rsr_roc_tickers.append(100 * ((rsr_tickers[i] / 1000) - 1)) 그냥 한 값으로 나눈것 이상의 의미가 없는데 
        moving_averages2 = []
        window2 = 40
        for j in range(len(rsr_tickers[i]) - window2 + 1):
            win = rsr_tickers[i][j:j + window2]  # 현재 위치에서 5개의 값을 추출
            window_average = sum(win) / window2  # 평균 계산
            moving_averages2.append(window_average)
        rsr_roc_tickers.append(100 * ((rsr_tickers[i][window2-1:].values/moving_averages2) -1))
        rsr_roc_tickers[i] = pd.Series(rsr_roc_tickers[i])
        rsr_tickers[i] = rsr_tickers[i][window2+window-2:] # rsr 구할때 window만큼 사용되고, rsm구할때 window2 + window만큼 사용됨
        # 첫 번째 값을 분모로 나누는 방식
        # 데이터 길이에 따라 시작점이 달라는 것 보완 필요
        rsm_tickers.append((100 + ((rsr_roc_tickers[i] - rsr_roc_tickers[i].rolling(window=window).mean()) / rsr_roc_tickers[i].rolling(window=window).std(ddof=0))).dropna())
        # rsr_tickers[i] = rsr_tickers[i][rsr_tickers[i].index.isin(rsm_tickers[i].index)]
        # rsm_tickers[i] = rsm_tickers[i][rsm_tickers[i].index.isin(rsr_tickers[i].index)]
    
    rsr_tickers_ma = []
    for series in rsr_tickers:
        rsr_tickers_ma.append(moving_average(series, window_ma))

    rsm_tickers_ma = []
    for series in rsm_tickers:
        rsm_tickers_ma.append(moving_average(series, window_ma))

    return rsr_tickers_ma, rsm_tickers_ma

def navertheme(a):
    a = a 
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from webdriver_manager.chrome import ChromeDriverManager
    
    import time
    import pandas as pd
    import requests
    from bs4 import BeautifulSoup

    options = Options()
    options.headless = False  # 브라우저 창을 띄우지 않고 실행, True해도 창이 뜨는데...
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    # 네이버 테마명
    result_theme = []
    for i in range(1, 600):
        url = f'https://finance.naver.com/sise/sise_group_detail.naver?type=theme&no={i}'
        driver.get(url)
        # time.sleep(1)
        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "#contentarea_left > table > tbody")))
        themes = driver.find_elements(By.CSS_SELECTOR, '#contentarea_left > table > tbody > tr:nth-child(4) > td:nth-child(1)')

        theme = []
        for k in themes:
            theme.append(k.text)

        theme_text = theme[0].split('\n')[0]
        result_theme.append((i, theme_text))

    naver_themes = pd.DataFrame(result_theme, columns=['class', 'theme'])

    # 네이버 테마명별 종목명
    result_stock = []
    for i in range(1, 600):

        url = f'https://finance.naver.com/sise/sise_group_detail.naver?type=theme&no={i}'
        driver.get(url)
        # time.sleep(1)
        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "#contentarea_left > table > tbody")))
        titles = driver.find_elements(By.CSS_SELECTOR, '#contentarea > div:nth-child(5) > table > tbody > tr > td.name > div > a')

        title = []
        for k in titles:
            title.append(k.text)

        result_stock.append((i, title))

    naver_themes_stock = pd.DataFrame(result_stock, columns=['class', 'title'])
    naver_themes_stock= naver_themes_stock.explode('title')
    naver_themes_stock['title'] = naver_themes_stock['title'].fillna('')

    # 코스피 종목코드
    stock_n = []
    stock_c = []
    for i in range(1, 50):
        url = f'https://finance.naver.com/sise/sise_market_sum.nhn?sosok=0&page={i}'
        response = requests.get(url)
        response.raise_for_status()  # 오류 발생시 예외 발생시킴
        soup = BeautifulSoup(response.text, 'html.parser')

        stocks = []
        for tr in soup.find_all('tr', {'onmouseover': 'mouseOver(this)'}):  # 특정 속성을 가진 tr 태그 찾기
            tds = tr.find_all('td')
            if len(tds) > 1:
                stock_name = tds[1].get_text(strip=True)  # 종목명
                stock_link = tds[1].a['href']  # 링크에서 종목코드 추출
                stock_code = stock_link.split('=')[-1]
                stock_n.append(stock_name)
                stock_c.append(stock_code)

    stock_n_df = pd.DataFrame(stock_n, columns=['title'])
    stock_c_df = pd.DataFrame(stock_c, columns=['code'])
    stockcode_kospi = pd.concat([stock_n_df,stock_c_df], axis=1)

    # 코스닥 종목코드
    stock_n = []
    stock_c = []
    for i in range(1, 50):
        url = f'https://finance.naver.com/sise/sise_market_sum.nhn?sosok=1&page={i}'
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        stocks = []
        for tr in soup.find_all('tr', {'onmouseover': 'mouseOver(this)'}):  # 특정 속성을 가진 tr 태그 찾기
            tds = tr.find_all('td')
            if len(tds) > 1:
                stock_name = tds[1].get_text(strip=True)  # 종목명
                stock_link = tds[1].a['href']  # 링크에서 종목코드 추출
                stock_code = stock_link.split('=')[-1]
                stock_n.append(stock_name)
                stock_c.append(stock_code)

    stock_n_df = pd.DataFrame(stock_n, columns=['title'])
    stock_c_df = pd.DataFrame(stock_c, columns=['code'])
    stockcode_kosdaq = pd.concat([stock_n_df,stock_c_df], axis=1)

    # 테마 데이터프레임
    navertheme = pd.merge(naver_themes, naver_themes_stock, on='class', how='left')
    navertheme = navertheme[navertheme['title'] != '']
    stockcode = pd.concat([stockcode_kospi,stockcode_kosdaq])
    stockcode['code'] = stockcode['code'].apply(lambda x: 'A' + str(x).zfill(6))
    navertheme_df = pd.merge(navertheme, stockcode, on='title', how='left')
    navertheme_df.columns = [col.replace('class', 'Class').replace('theme', 'THEME').replace('title', '종목명').replace('code', 'Code') for col in navertheme_df.columns]

    return navertheme_df
