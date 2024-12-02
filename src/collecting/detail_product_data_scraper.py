import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


class DetailedProductDataScraper:
    def __init__(self, driver_path, date, input_dir, output_dir, max_threads=4, row_num=10):
        self.driver_path = driver_path
        self.driver = None
        self.date = date
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.max_threads = max_threads
        self.row_num = row_num

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

    """숫자만 추출"""
    @staticmethod
    def extract_currently_value(text):
        if isinstance(text, float) and np.isnan(text):
            return 0
        if isinstance(text, list):
            text = text[0] if text else ""
        if not isinstance(text, str):
            text = str(text)

        # 쉼표 제거
        text = text.replace(",", "").replace(" ", "")

        match = re.search(r"\d+(\.\d+)?", text)
        if not match:
            return np.nan
        number = float(match.group())
        if "백" in text:
            number *= 100
        if "천" in text:
            number *= 1000
        if "만" in text:
            number *= 10000
        return int(number)


    """상품 상세 데이터 추출"""
    def extract_product_details(self, product_id):
        try:
            product_url = f"https://www.musinsa.com/products/{product_id}"
            self.driver.get(product_url)
            time.sleep(1)
            # # 최대 10초 대기: 특정 요소가 로드되면 즉시 진행
            # try:
            #     WebDriverWait(self.driver, 10).until(
            #         EC.presence_of_element_located((By.CSS_SELECTOR, "span.text-xs.font-medium.font-pretendard"))
            #     )
            # except TimeoutException:
            #     print(f"페이지 로드 타임아웃 발생 (ID: {product_id}). 데이터를 수집하지 않고 넘어감.")
            #     return None

            # 조회수
            soup = BeautifulSoup(self.driver.page_source, 'lxml')
            views = self.extract_text_by_label(soup, "조회수")

            # 누적 판매량
            total_sales = self.extract_text_by_label(soup, "누적판매")

            # 좋아요 수
            likes_element = soup.select_one("span.text-xs.font-medium.font-pretendard")
            likes = self.extract_currently_value(likes_element.text.strip()) if likes_element else None

            # 평점, 리뷰 개수
            rating = np.nan
            rating_count = np.nan
            rating_element = soup.select_one("span.font-medium.text-black")
            if rating_element:
                rating_text = rating_element.text.strip()
                if rating_text.replace(".", "", 1).isdigit():
                    rating = float(rating_text)
                sibling_element = rating_element.find_next_sibling("span")
                if sibling_element:
                    rating_count = self.extract_currently_value(sibling_element.text.strip())

            details = {
                "productId": product_id,
                "date": self.date,
                "views": self.extract_currently_value(views),
                "likes": likes,
                "rating": rating,
                "ratingCount": rating_count,
                "totalSales": self.extract_currently_value(total_sales),
            }
            
            # 데이터 확인(평점만)
            print(f"Rating: {rating}")

            return details
        except Exception as e:
            print(f"상품 세부 정보 추출 오류 (ID: {product_id}): {e}")
            return None

    """라벨 텍스트로 값 추출"""
    @staticmethod
    def extract_text_by_label(soup, label):
        label_element = soup.find("span", string=label)
        if label_element:
            value_element = label_element.find_next("span")
            if value_element:
                return value_element.text.strip()
        return np.nan

    def process_csv_file(self, input_csv, category):
        try:
            # CSV 파일 읽기
            df = pd.read_csv(input_csv)
            if "productId" not in df.columns or "date" not in df.columns:
                print(f"'{input_csv}'에 'productId' 또는 'date' 열이 없습니다.")
                return

            # date는 YYYYmmddHH형태로 입력받지만 혹시 '분'까지 입력 받을 수 있으므로 YYYYmmddHH 부분만 추출
            target_datetime = str(self.date)[:10]

            # date 필터링 (YYYYmmddHH 부분이 동일한 데이터만 필터링)
            if "date" in df.columns:
                df["datetime"] = df["date"].astype(str).str[:10]  # 데이터프레임에서 YYYYmmddHH 부분 추출
                filtered_df = df[df["datetime"] == target_datetime]  # YYYYmmddHH가 동일한 행만 필터링
            else:
                print(f"'{input_csv}'에 'date' 열이 없습니다.")
                return

            if filtered_df.empty:
                print(f"'{category}' 카테고리의 {self.date}와 동일한 시간(YYYYmmddHH)에 해당하는 데이터가 없습니다.")
                return

            # 이미 수집된 detail 데이터 읽기
            detail_file = os.path.join(self.output_dir, f"{category}_details.csv")
            if os.path.exists(detail_file):
                detail_df = pd.read_csv(detail_file)
                if "productId" in detail_df.columns and "date" in detail_df.columns:
                    # YYYYmmddHH 부분만 추출하여 중복 확인
                    detail_df["datetime"] = detail_df["date"].astype(str).str[:10]
                    existing_data = detail_df[["productId", "datetime"]].drop_duplicates()

                    # ranking 데이터와 detail 데이터의 교집합 여부 확인
                    merged_df = pd.merge(
                        filtered_df,
                        existing_data,
                        on=["productId", "datetime"],
                        how="inner"
                    )

                    # 중복된 데이터가 하나라도 있으면 해당 데이터를 제외
                    if not merged_df.empty:
                        print(f"'{category}' 카테고리의 일부 데이터가 이미 수집되었습니다. 중복 데이터 제외 후 진행합니다.")
                        filtered_df = filtered_df[~filtered_df["productId"].isin(merged_df["productId"])]

            # 중복 제거 후 모든 productId에 대하여 처리
            product_ids = filtered_df["productId"].unique()[:self.row_num]
            detailed_data = []

            count = 1

            # 상품 데이터 수집
            for product_id in product_ids:
                result = self.extract_product_details(product_id)
                if result:
                    detailed_data.append(result)
                    print(count, category)
                    count += 1

            # 데이터를 카테고리별 CSV 파일에 저장
            output_csv = os.path.join(self.output_dir, f"{category}_details.csv")
            self.save_to_csv(pd.DataFrame(detailed_data), output_csv)
        except Exception as e:
            print(f"파일 처리 중 오류 발생: {e}")

    def save_to_csv(self, df, file_path):
        """데이터를 CSV 파일에 저장. 기존 파일이 있으면 행을 추가."""
        if os.path.exists(file_path):
            try:
                # 기존 파일 읽기
                existing_df = pd.read_csv(file_path, encoding="utf-8-sig")
                # 기존 데이터와 새로운 데이터를 병합, 중복 제거
                df = pd.concat([existing_df, df]).drop_duplicates(ignore_index=True)
                print(f"기존 파일에 데이터를 추가: {file_path}")
            except Exception as e:
                print(f"기존 파일 읽기 오류: {e}. 새로 파일을 생성합니다.")

        # 데이터 저장
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        print(f"CSV 파일 저장 완료: {file_path}")
        print("=====================================================================")

    """입력 디렉토리 처리"""
    def process_detailed_data(self, target_categories=None): 
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        # 멀티스레딩으로 파일 처리
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = []
            for csv_file in os.listdir(self.input_dir):
                if csv_file.endswith(".csv"):
                    category = self.extract_category_from_filename(csv_file)
                    if target_categories and category not in target_categories:
                        print(f"건너뛴 카테고리: {category}")
                        continue

                    input_csv = os.path.join(self.input_dir, csv_file)
                    print(f"처리 중인 파일: {input_csv}, 카테고리: {category}")
                    # 스레드에 작업 추가
                    futures.append(executor.submit(self.process_csv_file, input_csv, category))

            # 스레드 작업 완료 확인
            for future in as_completed(futures):
                try:
                    future.result()  # 예외 확인
                except Exception as e:
                    print(f"스레드 작업 중 오류 발생: {e}")


    """파일 이름에서 카테고리 추출"""
    def extract_category_from_filename(self, filename):
        match = re.search(r"ranking-(.*?)-summary\.csv", filename)
        return match.group(1) if match else "unknown"
