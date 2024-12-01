from datetime import datetime

from detail_product_data_scraper import DetailedProductDataScraper


def main():
    # ChromeDriver 경로 설정
    driver_path = "C:/chromedriver-win64/chromedriver.exe"  # 실제 ChromeDriver 경로로 변경
    
    # 실행 날짜 설정 (yyyyMMddHH 형식)
    date = datetime.now().strftime('%Y%m%d%H')

    # 날짜 수동 설정 (yyyyMMddHH 형식)
    date = "2024120120"

    # 입력 디렉토리와 출력 디렉토리 설정
    input_dir = "../../data/raw/ranking"  
    output_dir = "../../data/raw" 

    # DetailedDataProcessor 클래스 인스턴스 생성
    processor = DetailedProductDataScraper(driver_path, date, input_dir, output_dir)

    # 원하는 카테고리의 detail 데이터 수집(수집 안 할 컬럼은 주석처리 하고 실행)
    target_categories = [
        # "bags",
        # "beauty-items",
        "clothes-top",
        "digital-life",
        # "fashion-accessories",
        # "kids",
        # "outers",
        # "pants",
        # "shoes",
        # "skirts",
        # "sportswears",
        # "underwears",
        ]  

    try:
        # 1. ChromeDriver 시작
        processor.start_chromedriver()

        # 2. 입력 데이터 처리 (특정 카테고리만 처리)
        processor.process_detailed_data(target_categories)

        # 3. ChromeDriver 종료
        processor.end_chromedriver()

    except Exception as e:
        print(f"프로세스 실행 중 오류 발생: {e}")
        processor.end_chromedriver()

if __name__ == "__main__":
    main()
