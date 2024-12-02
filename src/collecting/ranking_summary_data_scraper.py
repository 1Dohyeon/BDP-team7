import os
import re
import time

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service


class RankingSummaryDataScraper:
    """초기화 chromedriver 위치, 수집 날짜, csv 저장 위치"""
    def __init__(self, driver_path, date, output_dir):
        self.driver_path = driver_path
        self.driver = None
        self.date = date
        self.output_dir = output_dir

    """ChromeDriver 실행"""
    def start_chromedriver(self):
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")  
        options.add_argument("--disable-gpu")  # GPU 비활성화
        options.add_argument("--disable-dev-shm-usage")  # /dev/shm 사용 비활성화 (메모리 부족 문제 방지)

        service = Service(self.driver_path)
        self.driver = webdriver.Chrome(service=service)
        print("Chrome 드라이버 실행.")
        print("=====================================================================")
        
    """ChromeDriver 종료"""
    def end_chromedriver(self):
        if self.driver:
            self.driver.quit()
            print("Chrome 드라이버를 닫음.")

    """URL 목록"""
    @staticmethod
    def get_urls():
        return {
            "clothes_top": "https://www.musinsa.com/main/musinsa/ranking?skip_bf=Y&storeCode=musinsa&sectionId=199&categoryCode=001000&period=MONTHLY",
            "outers": "https://www.musinsa.com/main/musinsa/ranking?skip_bf=Y&storeCode=musinsa&sectionId=199&categoryCode=002000&period=MONTHLY",
            "pants": "https://www.musinsa.com/main/musinsa/ranking?skip_bf=Y&storeCode=musinsa&sectionId=199&categoryCode=003000&period=MONTHLY",
            # "bags": "https://www.musinsa.com/main/musinsa/ranking?skip_bf=Y&storeCode=musinsa&sectionId=200&categoryCode=004000",
            # "skirts": "https://www.musinsa.com/main/musinsa/ranking?skip_bf=Y&storeCode=musinsa&sectionId=200&categoryCode=100000",
            # "fashion_accessories": "https://www.musinsa.com/main/musinsa/ranking?skip_bf=Y&storeCode=musinsa&sectionId=200&categoryCode=101000",
            # "digital_life": "https://www.musinsa.com/main/musinsa/ranking?skip_bf=Y&storeCode=musinsa&sectionId=200&categoryCode=102000",
            "shoes": "https://www.musinsa.com/main/musinsa/ranking?skip_bf=Y&storeCode=musinsa&sectionId=199&categoryCode=103000&period=MONTHLY",
            # "beauty_items": "https://www.musinsa.com/main/musinsa/ranking?skip_bf=Y&storeCode=musinsa&sectionId=200&categoryCode=104000",
            # "sportswears": "https://www.musinsa.com/main/musinsa/ranking?skip_bf=Y&storeCode=musinsa&sectionId=200&categoryCode=017000",
            # "underwears": "https://www.musinsa.com/main/musinsa/ranking?skip_bf=Y&storeCode=musinsa&sectionId=200&categoryCode=026000",
            # "kids": "https://www.musinsa.com/main/musinsa/ranking?skip_bf=Y&storeCode=musinsa&sectionId=200&categoryCode=106000"
        }

    """파일 경로 생성"""
    def get_file_paths(self, ranking_urls):
        return {
            key: f"{self.output_dir}/ranking-{key.replace('_', '-')}-summary.csv"
            for key in ranking_urls.keys()
        }


    """상품명에서 색상 추출"""
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

    """숫자만 추출"""
    @staticmethod
    def extract_number_value(text):
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

        if "만" in text:  # "만" 단위 처리
            number *= 10000

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

            # 전체 판매량 ㅉ
            total_sales_element = item.select_one('.text-etc_11px_reg.sc-1m4cyao-7.eYXbyJ.font-pretendard')
            total_sales = total_sales_element.text.strip() if total_sales_element else np.nan


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
                "totalSales": total_sales
            }
        # 예외 처리
        except Exception as e:
            print(f"데이터 추출 오류: {e}")
            return None

    """데이터프레임 생성 및 처리"""
    def process_dataframe(self, data, category):
        df = pd.DataFrame(data)

        # 예외처리, productName 컬럼에 대해 에러가 뜨는 경우가 있었음.
        if "productName" not in df.columns:
            print("데이터프레임에 'productName' 컬럼이 없음.")
            return None

        # 색상 컬럼 추가
        df["colors"] = df["productName"].apply(self.extract_colors)

        # "현재 조회중", "구매중" 숫자만 추출하여 정수형으로 변환
        df["currentlyViewing"] = df["currentlyViewing"].apply(self.extract_number_value)
        df["currentlyBuying"] = df["currentlyBuying"].apply(self.extract_number_value)
        df["totalSales"] = df["totalSales"].apply(self.extract_number_value)

        # 현재 날짜를 모든 row에 추가
        df["date"] = self.date

        # 카테고리 컬럼 추가
        df["category"] = category
        
        return df

    """데이터 저장"""
    @staticmethod
    def save_to_csv(df, file_path):
        if os.path.exists(file_path):
            existing_df = pd.read_csv(file_path, encoding="utf-8-sig")
            df = pd.concat([existing_df, df], ignore_index=True)
            print(f"기존 파일에 데이터를 추가: {file_path}")
        else:
            print(f"CSV 파일 새로 생성: {file_path}")

        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        print(f"데이터 저장 완료: {file_path}")
        print("=====================================================================")
    
    """데이터 스크랩"""
    def scrape_data(self, url, file_path, category):
        self.driver.get(url)
        time.sleep(3)
        soup = BeautifulSoup(self.driver.page_source, 'lxml')
        items = soup.select("div.gtm-view-item-list")
        
        # 데이터 스크래핑 시작
        data = [self.extract_item_data(item) for item in items if self.extract_item_data(item)]
        if not data:
            print(f"데이터가 없음, URL: {url}")
            return
        df = self.process_dataframe(data, category)

        # 데이터 저장 호출
        self.save_to_csv(df, file_path)

    