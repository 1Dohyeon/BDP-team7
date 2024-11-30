from ranking_summary_data_scraper import RankingSummaryDataScraper


def main():
    scraper = RankingSummaryDataScraper("C:/chromedriver-win64/chromedriver.exe")
    scraper.start_chromedriver()
    urls = scraper.get_urls()
    file_paths = scraper.get_file_paths(urls)

    # 각 카테고리에 대해 스크래핑 수행
    for category, url in urls.items():
        file_path = file_paths[category]
        print(f"스크래핑 진행중인 카테고리: {category}")
        scraper.scrape_data(url, file_path)

    scraper.end_chromedriver()


if __name__ == "__main__":
    main()
