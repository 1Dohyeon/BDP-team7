from datetime import datetime

from ranking_summary_data_scraper import RankingSummaryDataScraper


def main():
    driver_path = "C:/chromedriver-win64/chromedriver.exe"
    
    # 실행 날짜 설정 (yyyyMMddHH 형식)
    date = datetime.now().strftime('%Y%m%d%H')
    
    # 날짜 수동 설정 (yyyyMMddHH 형식)
    # date = "yyyyMMddHH"
    
    # output 저장 디렉토리
    output_dir = "../../data/raw/ranking/temp" 

    scraper = RankingSummaryDataScraper(driver_path, date, output_dir)
    
    scraper.start_chromedriver()
    urls = scraper.get_urls()
    file_paths = scraper.get_file_paths(urls)

    # 각 카테고리에 대해 스크래핑 수행
    for category, url in urls.items():
        file_path = file_paths[category]
        print(f"스크래핑 진행중인 카테고리: {category}")
        scraper.scrape_data(url, file_path, category)

    scraper.end_chromedriver()


if __name__ == "__main__":
    main()
