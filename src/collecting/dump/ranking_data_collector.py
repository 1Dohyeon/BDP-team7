import os
from datetime import datetime

import numpy as np
import pandas as pd
import requests


class RankingDataCollector:
    print_line_60 = "=" * 60
    """초기화"""
    def __init__(self, api_url, output_dir):
        self.api_url = api_url
        self.output_dir = output_dir
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        os.makedirs(self.output_dir, exist_ok=True)

    """api 요청으로 데이터 가져옴"""
    def fetch_data(self):
        response = requests.get(self.api_url, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"""{self.print_line_60}
                    fail: request api
                    status code: {response.status_code}
{self.print_line_60}""")
            return None

    """급상승 데이터 여부 처리"""
    @staticmethod
    def parse_labels(labels):
        trending = 0
        sales_text = np.nan
        for label in labels:
            if label.get('text') == "급상승":
                trending = 1
            else:
                sales_text = label.get('text', np.nan)
        return trending, sales_text

    """현재 조회중, 구매중 데이터에서 숫자만 추출"""
    @staticmethod
    def extract_number_from_viewing_and_purchasing(additional_info):
        viewing = 0
        purchasing = 0
        for info in additional_info:
            text = info.get('text', '')
            if '명이 보는 중' in text:
                viewing = int(
                    float(text.replace('명이 보는 중', '').replace('.', '').strip('천만')) *
                    (1000 if '천' in text else 10000 if '만' in text else 1)
                )
            elif '명이 구매 중' in text:
                purchasing = int(
                    float(text.replace('명이 구매 중', '').replace('.', '').strip('천만')) *
                    (1000 if '천' in text else 10000 if '만' in text else 1)
                )
        return viewing, purchasing

    """sales 데이터에서 판매량 숫자만 가져옴"""
    @staticmethod
    def extract_number_from_sales(sales):
        if pd.isna(sales):
            return np.nan
        if '천개' in sales:
            return int(float(sales.replace('판매 ', '').replace('천개', '')) * 1000)
        if '만개' in sales:
            return int(float(sales.replace('판매 ', '').replace('만개', '')) * 10000)
        return np.nan

    """데이터 요소에서 상품 데이터 파싱"""
    def parse_product(self, item):
        # 데이터 수집 날짜(분까지)        
        date = datetime.now().strftime('%Y%m%d%H%M')
        
        
        product_id = item.get('id', np.nan) # 상품id
        rank = item['image']['rank'] # 랭킹
        product_name = item['info']['productName'] # 상품 이름
        brand_name = item['info']['brandName'] # 브랜드 이름
        product_url = item['onClick']['url'] # 상품 url
        image_url = item['image'].get('url', np.nan) # 상품 이미지 url

        # 리뷰 수, 리뷰 평균 평점(100으로 환산)    
        review_count = int(item.get('onClick', {}).get('eventLog', {}).get('amplitude', {}).get('payload', {}).get('reviewCount', 0))
        review_score = float(item.get('onClick', {}).get('eventLog', {}).get('amplitude', {}).get('payload', {}).get('reviewScore', np.nan))

        # 기존 가격, 할인 가격, 할인율
        original_price = int(item['onClick']['eventLog']['ga4']['payload'].get('original_price', np.nan))
        final_price = int(item['onClick']['eventLog']['ga4']['payload'].get('price', np.nan))
        discount_ratio = int(item['onClick']['eventLog']['ga4']['payload'].get('discount_rate', 0))

        # 현재 조회중, 구매중 수
        viewing, purchasing = self.extract_number_from_viewing_and_purchasing(item['info'].get('additionalInformation', []))
        
        # 급상승 여부
        trending, sales_text = self.parse_labels(item['image'].get('labels', []))

        return {
            'date': date,
            'product_id': product_id,
            'product_name': product_name,
            'brand_name': brand_name,
            'original_price': original_price,
            'final_price': final_price,
            'discount_rate': discount_ratio,
            'trending': trending,
            'sold_count': sales_text,
            'viewing': viewing,
            'purchasing': purchasing,
            'review_count': review_count,
            'review_score': review_score,
            'product_url': product_url,
            'image_url': image_url,
            'rank': rank,
        }

    """상품 정보 가져와서 데이터 프레임 형태로 반환"""
    def get_product_info(self):
        data = self.fetch_data()
        if data is None:
            return None

        products = []
        for module in data['data']['modules']:
            if 'items' in module:
                for item in module['items']:
                    if 'image' in item and 'rank' in item['image']:
                        product = self.parse_product(item)
                        products.append(product)

        df = pd.DataFrame(products)

        # df 반환 전 sold_count에서 숫자만 남게함
        df['sold_count'] = df['sold_count'].apply(self.extract_number_from_sales)

        return df

    """데이터 csv 파일로 저장(덮어씌움)"""
    def save_new_csv(self, df):
        output_path = os.path.join(self.output_dir, 'ranking-data.csv')
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"""{self.print_line_60}
success: save csv
csv path: {output_path}
{self.print_line_60}""")

    """기존 csv 파일이 있으면 행을 추가하는 방식으로 저장"""
    def append_to_csv(self, df):
        output_path = os.path.join(self.output_dir, 'ranking-data.csv')
        
        # 기존 파일이 존재하면 병합
        if os.path.exists(output_path):
            existing_df = pd.read_csv(output_path, encoding='utf-8-sig')
            df = pd.concat([existing_df, df], ignore_index=True)
            print(f"""{self.print_line_60}
                    success: save csv
                    csv path: {output_path}
{self.print_line_60}""")
        else:
            print(f"""{self.print_line_60}
                    success: save csv
                    csv path: {output_path}
{self.print_line_60}""")

        # 데이터 저장
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"""{self.print_line_60}
                success: save csv
                csv path: {output_path}
{self.print_line_60}""")



