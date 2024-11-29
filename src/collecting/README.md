## Data Collecting

### 디렉토리 구조

```
dump/                      # 사용되지 않는 코드 폴더
data_collect_main.py       # 실행 코드
ranking_data_collector.py  # API를 통해서 랭킹 데이터 가져오는 코드
README.md
example_raw_data.json      # api url에서 확인할 수 있는 raw ranking data 예제
```

---

### API

- API URL: https://api.musinsa.com/api2/hm/v1/pans/ranking?storeCode=musinsa

### Example Data

- api로부터 가져온 데이터 샘플: example_raw_data.json
- **추출한 데이터**:
  - product_id
  - product_name
  - brand_name
  - original_price
  - finial_price
  - trending
  - sold_count
  - viewing
  - purchasing
  - review_count
  - review_score
  - product_url
  - image_url
  - rank
- **추가한 데이터**:
  - date

---
