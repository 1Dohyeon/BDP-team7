from ranking_data_collector import RankingDataCollector


def main():
    api_url = "https://api.musinsa.com/api2/hm/v1/pans/ranking?storeCode=musinsa"
    output_dir = '../../data/raw' # csv 저장할 디렉토리

    collector = RankingDataCollector(api_url, output_dir)
    df = collector.get_product_info()

    if df is not None:
        collector.save_new_csv(df) # 테스트 용
        # collector.append_to_csv(df)


if __name__ == "__main__":
    main()
