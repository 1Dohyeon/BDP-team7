import os
import subprocess


def upload_raw_data_to_hdfs(local_dir, hdfs_dir):
    """
    로컬 디렉토리의 raw CSV 파일을 HDFS에 업로드. 이미 존재하는 경우 덮어씀.
    """
    # HDFS 디렉토리가 없으면 생성
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=True)
    
    # 로컬 디렉토리에서 파일 읽기
    for file_name in os.listdir(local_dir):
        if file_name.endswith(".csv"):  # CSV 파일만 처리
            local_path = os.path.join(local_dir, file_name)
            hdfs_path = os.path.join(hdfs_dir, file_name)
            
            print(f"Uploading {local_path} to {hdfs_path}")
            
            # HDFS로 파일 업로드 (기존 파일 덮어씌우기)
            subprocess.run(["hdfs", "dfs", "-put", "-f", local_path, hdfs_path], check=True)
            
    print("success uploading!")


if __name__ == "__main__":
    # 로컬 디렉토리 경로와 HDFS 디렉토리 경로 설정
    local_directory = "../data/processed"  # 현재 디렉토리 기준 상대 경로
    hdfs_directory = "/user/maria_dev/term_project_data/processed"  # HDFS 내 업로드 경로
    
    try:
        upload_raw_data_to_hdfs(local_directory, hdfs_directory)
    except subprocess.CalledProcessError as e:
        print(f"error uploading: {e}")
