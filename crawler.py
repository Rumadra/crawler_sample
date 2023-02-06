import datetime
import os
import sys
from selenium import webdriver
from selenium.webdriver.common.by import By
from googleapiclient.discovery import build
from time import sleep
from bs4 import BeautifulSoup

import json
import pandas as pd
import re
import numpy as np

import warnings
warnings.simplefilter('ignore', FutureWarning)

'''
    usage: python crawler.py <keyword> <keyword2> ...
'''

GOOGLE_API_KEY = 'XXX'
CUSTOM_SEARCH_ENGINE_ID = 'YYY'

# Google Custom Search APIを使わず、ローカルにある検索結果jsonを読み込んで実施する場合はTrue
OFFLINE_MODE = False
# オフラインモードで使用するレスポンスjsonファイルパス
JSON_PATH = './response_20230117.json'

# 結果格納先
DATA_DIR = 'results'


def make_dir(path):
    ''' 引数に指定されたパスのディレクトリを作成 '''
    os.makedirs(path, exist_ok=True)


def get_search_response(keyword):
    '''
        Google検索を Custom Search APIによって実施し、レスポンスをjson形式で保存する
        取得する件数は最初の10件のみ
    '''
    
    today = datetime.datetime.today().strftime('%Y%m%d')
    timestamp = datetime.datetime.today().strftime("%Y/%m/%d %H:%M:%S")
    
    service = build("customsearch", 'v1', developerKey=GOOGLE_API_KEY)
    
    page_limit = 3
    start_index = 1
    
    response = []
    
    for n_page in range(0, page_limit):
    
        try:
            sleep(1)
            response.append(service.cse().list(
                q=keyword,
                cx=CUSTOM_SEARCH_ENGINE_ID,
                lr='lang_ja',
                num=10,
                start=start_index
            ).execute())
            start_index = response[n_page].get("queries").get("nextPage")[0].get("startIndex")
        except Exception as e:
            print(e)
            break
    
    
    
    # レスポンスをjson形式で保存
    out = {'snapshot_ymd': today, 'snapshot_timestamp': timestamp, 'response': []}
    out['response'] = response
    jsonstr = json.dumps(out, ensure_ascii=False)
    with open(os.path.join(DATA_DIR, 'response_' + today + '.json'), mode='w', encoding='utf-8') as response_file:
        response_file.write(jsonstr)
        
    return jsonstr


def make_search_results(res_json, keyword):
    '''
        Google検索結果jsonをtsvファイル形式に成形してファイル保存する
    '''
    response_json = json.loads(res_json)
    ymd = response_json['snapshot_ymd']
    response = response_json['response']
    results = []
    
    res_cols = ['ymd', 'no', 'display_link', 'title', 'link', 'snippet']
    res_df = pd.DataFrame(columns=res_cols)
    
    cnt = 0
    
    for one_res in range(len(response)):
        if 'items' in response[one_res] and len(response[one_res]['items']) > 0:
            for i in range(len(response[one_res]['items'])):
                cnt += 1
                display_link = response[one_res]['items'][i]['displayLink'].replace('\xa0', '')
                title        = response[one_res]['items'][i]['title'].replace('\xa0', '')
                link         = response[one_res]['items'][i]['link'].replace('\xa0', '')
                snippet      = response[one_res]['items'][i]['snippet'].replace('\n', '').replace('\xa0', '')
                record = pd.Series([ymd, cnt, display_link, title, link, snippet], index=res_cols)
                res_df = res_df.append(record, ignore_index=True)
                sleep(2)
    
    # 複数キーワードの場合を考慮してスペースを_に変換
    keyword = keyword.replace(" ", "_")
    keyword = keyword.replace("　", "_")
    keyword = keyword.replace('\xa0', '')
    res_df.to_csv(os.path.join(DATA_DIR, 'results_' + keyword + "_" + ymd + '.tsv'), sep='\t', index=False, encoding="utf-8")

    return res_df


if __name__ == '__main__':
    # 実施結果格納先ディレクトリを作成
    make_dir(DATA_DIR)

    # 複数キーワードを結合するためのキーワード変数
    concat_keyword = ""
    
    # 都道府県 正規表現
    raw_prefecture_pattern = r'(...??[都道府県])((?:旭川|伊達|石狩|盛岡|奥州|田村|南相馬|那須塩原|東村山|武蔵村山|羽村|十日町|上越|富山|野々市|大町|蒲郡|四日市|姫路|大和郡山|廿日市|下松|岩国|田川|大村|宮古|富良野|別府|佐伯|黒部|小諸|塩尻|玉野|周南)市|(?:余市|高市|[^市]{2,3}?)郡(?:玉村|大町|.{1,5}?)[町村]|(?:.{1,4}市)?[^町]{1,4}?区|.{1,7}?[市町村])(.+)'    
    prefecture_pattern = re.compile(raw_prefecture_pattern)
    
    # 電話番号 正規表現
    raw_tel_pattern = r'[\(]{0,1}[0-9]{2,4}[\)\-\(]{0,1}[0-9]{2,4}[\)\-]{0,1}[0-9]{3,4}'
    tel_pattern = re.compile(raw_tel_pattern)

    # 引数から単発/複数キーワードを取得
    for idx in range(1, len(sys.argv)):
        if concat_keyword == "":
            concat_keyword = sys.argv[idx]
        else:
            concat_keyword = concat_keyword + " " + sys.argv[idx]

    # オフラインモードなら、jsonファイルを読み込む
    if OFFLINE_MODE:
        with open(JSON_PATH, encoding="utf-8") as json_open:
            di = json.load(json_open)
        res_json = json.dumps(di)
        
    else:
        # 指定キーワードについて、Google Custom Search APIで上位10件を検索し、検索結果を保存
        res_json = get_search_response(concat_keyword)
    # 検索結果jsonをDataFrame型に変換
    response_df = make_search_results(res_json, concat_keyword)

    # Chrome Driverを設定
    options = webdriver.ChromeOptions()
    # ヘッドレスモードで実行
    options.add_argument('--headless')
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    driver = webdriver.Chrome("./chromedriver.exe", options=options)
    
    # クローラ結果格納DataFrame
    res_cols = ['サイトタイトル', 'URL', '住所', '電話番号']
    result_df = pd.DataFrame(data=None, columns=res_cols)

    # 以下、検索結果の件数分ループで実施
    for index, data in response_df.iterrows():
        # 検索結果のタイトルを取得
        title = str(data['title'])
        # 検索結果のアドレスを取得
        url = str(data['display_link'])
        if not "http" in url:
            url = "https://" + url
        print(url)
        
        # Chrome Driverのアドレス欄に、取得した検索結果のアドレスを入力して遷移
        try:
            driver.get(url)
            # 旧URLで、リダイレクトされるサイト用の対策
            sleep(30)
        except Exception:
            print("このサイトにアクセスできませんでした。")
            continue
        
        # 住所取得フラグ
        has_address = False
        # 住所取得結果格納変数
        result_address = ""
        # 電話番号取得フラグ
        has_phone_number = False
        # 電話番号取得結果格納変数
        result_phone_number = ""
        
        # ========== 住所情報取得 ==========
        # アクセスしたサイトのページに、「会社概要」または「会社情報」または「company」または「アクセス」または「access」の単語が含まれるタグのリンクを取得
        for address_keyword in ['会社概要', '会社情報', 'company', 'アクセス', 'access']:
            try:
                access_link = driver.find_element(by=By.PARTIAL_LINK_TEXT, value=address_keyword)
                access_link.click()
                sleep(5)
                
                # 取得したページのhtmlを取得
                html = driver.page_source
                
                # htmlをBeautifulSoupオブジェクトに変換                
                soup = BeautifulSoup(html, "lxml")
                # scriptやstyle及びその他タグの除去
                for s in soup(['script', 'style']):
                    s.decompose()
                    
                html_series = pd.Series(data=soup.stripped_strings)
                print(html_series.to_string())
                
                # すでに住所取得済みならスキップ
                if not has_address:
                    for data in html_series:
                        # 正規表現で住所情報を取得
                        match = prefecture_pattern.search(data)
                        if match or "〒" in data:
                            has_address = True
                            result_address = data
                            print('住所該当アリ: {}'.format(data))
                
                # すでに電話番号取得済みならスキップ
                if not has_phone_number:
                    # 電話番号を取得する
                    for data in html_series:
                        # 正規表現で住所情報を取得
                        match = tel_pattern.search(data)
                        if "代表者" in data or "代表取締" in data or "FAX" in data or "Copyright" in data:
                            continue
                        if match or "(代表)" in data or "TEL" in data:
                            has_phone_number = True
                            result_phone_number = data
                            print('電話番号該当アリ: {}'.format(data))
                
                # 住所&電話番号が取得済みなら、ループから抜ける
                if has_address and has_phone_number:
                    break
            
            except Exception as exception:
                print(exception)
                print("このサイトに「{}」は見つからなかった".format(address_keyword))
                pass
        
        # 取得した情報をDataFrameに格納
        record = pd.Series([title, url, result_address, result_phone_number], index=res_cols)
        
        # 検索結果タイトル、アドレス、住所情報、電話番号をpandas.DataFrameに格納(NaN許容)
        result_df = result_df.append(record, ignore_index=True)
        sleep(2)

    # Chrome Driverを閉じる
    driver.close()

    # 結果をcsv出力
    concat_keyword = concat_keyword.replace(" ", "_")
    result_df.to_excel(os.path.join(DATA_DIR, "{}_クローリング結果.xlsx".format(concat_keyword)), header=True, index=False, encoding="cp932")