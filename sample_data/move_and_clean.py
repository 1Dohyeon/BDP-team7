import os
import shutil


base_dir = "."

for dir_name in os.listdir(base_dir):
	dir_path = os.path.join(base_dir, dir_name)

	if os.path.isdir(dir_path):
		for file_name in os.listdir(dir_path):
			if file_name.endswith(".csv"):
				file_path = os.path.join(dir_path, file_name)
				shutil.move(file_path, base_dir)
				print(f"Moved: {file_path} -> {base_dir}")

		if not os.listdir(dir_path):  
			os.rmdir(dir_path)
			print(f"Removed empty directory: {dir_path}")

print("CSV 파일 이동 및 빈 디렉토리 삭제 완료")

