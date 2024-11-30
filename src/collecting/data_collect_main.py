import subprocess  # 추가된 부분
from ranking_data_collector import RankingDataCollector

def upload_to_hdfs(local_path, hdfs_path):
    """
    HDFS에 파일 업로드
    """
    try:
        # subprocess를 사용해 HDFS에 업로드
        subprocess.run(["hdfs", "dfs", "-put", "-f", local_path, hdfs_path], check=True)
        print(f"성공적으로 업로드: {local_path} -> {hdfs_path}")
    except subprocess.CalledProcessError as e:
        print(f"HDFS 업로드 실패: {e}")

def main():
    api_url = "https://api.musinsa.com/api2/hm/v1/pans/ranking?storeCode=musinsa"
    output_dir = "./data/raw"  # 로컬 CSV 저장 디렉토리
    hdfs_output_dir = "/user/maria_dev/bdp_team/"  # HDFS 디렉토리

    # RankingDataCollector 객체 생성
    collector = RankingDataCollector(api_url, output_dir)
    
    # 데이터 수집
    df = collector.get_product_info()

    if df is not None:
        # 로컬에 새 CSV 파일 저장
        local_csv_path = f"{output_dir}/ranking-data.csv"
        collector.save_new_csv(df)
        
        # HDFS로 업로드
        upload_to_hdfs(local_csv_path, hdfs_output_dir)

if __name__ == "__main__":
    main()
