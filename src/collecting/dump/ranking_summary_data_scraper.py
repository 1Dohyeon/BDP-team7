import os
import re
import time
from datetime import datetime

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


class RankingSummaryDataScraper:
    def __init__(self):
        # ChromeDriver 경로 설정
        self.CHROME_DRIVER_PATH = "./chromedriver-linux64/chromedriver"
        
        # ChromeOptions 설정
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless") 
        chrome_options.add_argument("--verbose")

        self.driver = webdriver.Chrome(executable_path=self.CHROME_DRIVER_PATH, options=chrome_options)



    """ChromeDriver 실행"""
    def start_chromedriver(self):
        if not os.path.exists(self.CHROME_DRIVER_PATH):
            raise FileNotFoundError(f"ChromeDriver 경로를 확인하세요: {self.CHROME_DRIVER_PATH}")
        self.driver = webdriver.Chrome(executable_path=self.CHROME_DRIVER_PATH)
        print(f"ChromeDriver 실행: {self.CHROME_DRIVER_PATH}")
        return self.driver

    """ChromeDriver 종료"""
    def end_chromedriver(self):
        self.driver.quit()
        print("ChromeDriver 서비스 종료.")

    """URL 목록 가져오기"""
    @staticmethod
    def get_urls():
        # 무신사 랭킹 페이지 URL
        ranking_urls = {
            "clothes_top": "https://www.musinsa.com/main/musinsa/ranking?skip_bf=Y&storeCode=musinsa&sectionId=200&categoryCode=001000",
            "outers": "https://www.musinsa.com/main/musinsa/ranking?skip_bf=Y&storeCode=musinsa&sectionId=200&categoryCode=002000",
            "pants": "https://www.musinsa.com/main/musinsa/ranking?skip_bf=Y&storeCode=musinsa&sectionId=200&categoryCode=003000",
            "bags": "https://www.musinsa.com/main/musinsa/ranking?skip_bf=Y&storeCode=musinsa&sectionId=200&categoryCode=004000",
            "skirts": "https://www.musinsa.com/main/musinsa/ranking?skip_bf=Y&storeCode=musinsa&sectionId=200&categoryCode=100000",
            "fashion_accessories": "https://www.musinsa.com/main/musinsa/ranking?skip_bf=Y&storeCode=musinsa&sectionId=200&categoryCode=101000",
            "digital_life": "https://www.musinsa.com/main/musinsa/ranking?skip_bf=Y&storeCode=musinsa&sectionId=200&categoryCode=102000",
            "shoes": "https://www.musinsa.com/main/musinsa/ranking?skip_bf=Y&storeCode=musinsa&sectionId=200&categoryCode=103000",
            "beauty_items": "https://www.musinsa.com/main/musinsa/ranking?skip_bf=Y&storeCode=musinsa&sectionId=200&categoryCode=104000",
            "sportswears": "https://www.musinsa.com/main/musinsa/ranking?skip_bf=Y&storeCode=musinsa&sectionId=200&categoryCode=017000",
            "underwears": "https://www.musinsa.com/main/musinsa/ranking?skip_bf=Y&storeCode=musinsa&sectionId=200&categoryCode=026000",
            "kids": "https://www.musinsa.com/main/musinsa/ranking?skip_bf=Y&storeCode=musinsa&sectionId=200&categoryCode=106000"
        }
        return ranking_urls

    """파일 경로 생성"""
    @staticmethod
    def get_file_paths(ranking_urls):
        file_paths = {
            key: f"./data/raw/ranking-{key}-summary.csv"
            for key in ranking_urls.keys()
        }
        return file_paths

    """상품명에 색상있으면 color 컬럼으로 추출"""
    @staticmethod
    def extract_colors(product_name):
        # 영어, 한글 색상 리스트
        colors_list = [
            "black", "brown", "blue", "green", "grey", "charcoal", "white", "navy",
            "블랙", "브라운", "블루", "그린", "그레이", "차콜", "화이트", "네이비"
        ]
        # productName에서 색상을 찾고 구분자로 결합
        found_colors = [color for color in colors_list if color in product_name.lower()]
        return " | ".join(found_colors) if found_colors else np.nan

    # 숫자+문자 데이터에서 숫자만 출력
    @staticmethod
    def extract_currently_value(text):
        if isinstance(text, float) and np.isnan(text):  # NaN 값 처리
            return 0
        
        if isinstance(text, list):  # 리스트가 비어있으면 빈 문자열로 설정
            text = text[0] if text else "" 

        if not isinstance(text, str):  # 문자열이 아닌 경우 처리
            text = str(text)

        text = text.replace(" ", "")  # 공백 제거

        match = re.search(r"\d+(\.\d+)?", text)  # 숫자 검색
        if not match:  # 숫자가 없을 경우
            return np.nan
        number = float(match.group())  # 숫자 추출

        if "천" in text:  # "천" 단위 처리
            number *= 1000

        # 만 단위는 없었던 것 같음...

        return int(number)

    """HTML 요소에서 데이터 추출"""
    @staticmethod
    def extract_item_data(item):
        try:
            item_id = item.get("data-item-id", np.nan) # 상품id

            product_name_element = item.select_one('p.text-body_13px_reg') # 상품명
            product_name = product_name_element.text.strip() if product_name_element else np.nan # 공백 제거

            brand_name_element = item.select_one('p.text-etc_11px_semibold') # 브랜드명
            brand_name = brand_name_element.text.strip() if brand_name_element else np.nan # 공백 제거

            price = item.get("data-price", np.nan) # 가격
            discount_rate = item.get("data-discount-rate", 0) # 할인율

            # 현재 조회 중
            viewing_span = item.select_one('div.pt-2\\.5 > div > span:nth-child(1)')
            currently_viewing = viewing_span.text.strip() if viewing_span else 0

            # 현재 구매 중
            buying_span = item.select_one('div.pt-2\\.5 > div > span:nth-child(2)')
            currently_buying = buying_span.text.strip() if buying_span else 0

            # "조회 중"만 있고, "구매 중"이 없는 경우
            if viewing_span and not buying_span:
                currently_buying = 0

            # 급상승 여부
            trending_element = item.select_one('div.sc-1m4cyao-1.dYjLwF > div > span > span')
            trending = 1 if trending_element and "급상승" in trending_element.text else 0

            # 랭킹 정보
            ranking_element = item.select_one('div.sc-1m4cyao-1.dYjLwF > span > span')
            ranking = ranking_element.text.strip() if ranking_element else np.nan

            return {
                "productId": item_id,
                "brandName": brand_name,
                "productName": product_name,
                "price": price,
                "discountRate": discount_rate,
                "currentlyViewing": currently_viewing,
                "currentlyBuying": currently_buying,
                "ranking": ranking,
                "trending": trending,
            }
        # 예외 처리
        except Exception as e:
            print(f"error: {e}")
            return None

    """데이터프레임 생성 및 후처리"""
    def process_dataframe(self, data):
        df = pd.DataFrame(data)

        # 예외처리, productName 컬럼에 대해 에러가 뜨는 경우가 있었음.
        if "productName" not in df.columns:
            print("데이터프레임에 'productName' 컬럼이 없음.")
            return None

        # 색상 컬럼 추가
        df["colors"] = df["productName"].apply(self.extract_colors)

        # "현재 조회중", "구매중" 숫자만 추출하여 정수형으로 변환
        df["currentlyViewing"] = df["currentlyViewing"].apply(self.extract_currently_value)
        df["currentlyBuying"] = df["currentlyBuying"].apply(self.extract_currently_value)

        # 현재 날짜를 모든 row에 추가
        df["date"] = datetime.now().strftime('%Y%m%d%H%M')

        return df

    """데이터프레임을 CSV로 저장 (기존 데이터에 추가)"""
    @staticmethod
    def save_dataframe(df, file_path):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # 기존 파일이 있으면 병합(테스트 할 때는 덮는 방식으로)
        if os.path.exists(file_path):
            existing_df = pd.read_csv(file_path, encoding="utf-8-sig")
            df = pd.concat([existing_df, df], ignore_index=True)
            print(f"기존 파일에 데이터를 추가했습니다: {file_path}")
        else:
            print(f"새 파일 생성: {file_path}")

        # 병합된 데이터 저장
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        print(f"데이터가 {file_path}에 저장되었습니다.")

    """URL에 접근해 데이터를 스크랩하고 CSV로 저장"""
    def scrape_data(self, url, file_path):
        self.driver.get(url)
        time.sleep(3)  # 페이지 렌더링 대기

        # HTML 가져옴
        html = self.driver.page_source
        soup = BeautifulSoup(html, 'lxml')

        # 데이터 스크래핑
        items = soup.select("div.gtm-view-item-list")
        data = []

        for item in items:
            item_data = self.extract_item_data(item)
            if item_data:
                data.append(item_data)

        # 데이터가 비어있으면 종료
        if not data:
            print(f"데이터가 비어 있습니다. URL: {url}")
            return

        # 데이터프레임 처리
        df = self.process_dataframe(data)
        if df is None:
            return

        # 데이터 저장 
        self.save_dataframe(df, file_path)