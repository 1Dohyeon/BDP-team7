from datetime import datetime

from detail_product_data_scraper import DetailedProductDataScraper
from ranking_summary_data_scraper import RankingSummaryDataScraper


def main():
    driver_path = "C:/chromedriver-win64/chromedriver.exe"
    
    # 실행 날짜 설정 (yyyyMMddHH 형식)
    date = datetime.now().strftime('%Y%m%d%H')
    
    # 날짜 수동 설정 (yyyyMMddHH 형식)
    # date = "yyyyMMddHH"

    # Summary 데이터 저장 디렉토리
    summary_output_dir = "../../data/raw/ranking"

    # Detail 데이터 저장 디렉토리
    detail_output_dir = "../../data/raw"

    # Summary 데이터 먼저 수집
    print("=== Summary 데이터 수집 시작 ===")
    summary_scraper = RankingSummaryDataScraper(driver_path, date, summary_output_dir)

    try:
        # ChromeDriver 시작
        summary_scraper.start_chromedriver()
        
        # 카테고리별 URL 가져오기
        urls = summary_scraper.get_urls()
        
        # 각 카테고리별 파일 경로 계산
        file_paths = summary_scraper.get_file_paths(urls)

        # 각 카테고리에 대해 summary 데이터 스크래핑 수행
        for category, url in urls.items():
            file_path = file_paths[category]
            print(f"Summary 스크래핑 진행중인 카테고리: {category}")
            summary_scraper.scrape_data(url, file_path, category)

    except Exception as e:
        print(f"Summary 데이터 수집 중 오류 발생: {e}")
    finally:
        # ChromeDriver 종료
        summary_scraper.end_chromedriver()
        print("=== Summary 데이터 수집 완료 ===")

    # Detail 데이터 수집
    print("=== Detail 데이터 수집 시작 ===")
    detail_scraper = DetailedProductDataScraper(driver_path, date, summary_output_dir, detail_output_dir)

    # Detail 데이터 수집 카테고리 설정
    target_categories = [
        "bags",
        "beauty-items",
        "clothes-top",
        "digital-life",
        "fashion-accessories",
        "kids",
        "outers",
        "pants",
        "shoes",
        "skirts",
        "sportswears",
        "underwears",
    ]

    try:
        # ChromeDriver 시작
        detail_scraper.start_chromedriver()

        # 입력 데이터 처리 (특정 카테고리만 처리)
        detail_scraper.process_detailed_data(target_categories)

    except Exception as e:
        print(f"Detail 데이터 수집 중 오류 발생: {e}")
    finally:
        # ChromeDriver 종료
        detail_scraper.end_chromedriver()
        print("=== Detail 데이터 수집 완료 ===")


if __name__ == "__main__":
    main()
