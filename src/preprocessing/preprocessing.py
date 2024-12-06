import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    split,
    expr,
    regexp_replace,
    col,
    udf,
    explode,
    collect_set,
    collect_list,
    trim,
    size
)

class ProductRankingProcessor:
    def __init__(self):
        self.spark = SparkSession.builder.appName("BDP").getOrCreate()

    def load_data(self):
        # CSV 파일 읽기
        self.ranking_clothes_top_data = self.spark.read.csv("../../data/raw/ranking-clothes-top-summary.csv", header=True, inferSchema=True)
        self.ranking_pants_data = self.spark.read.csv("../../data/raw/ranking-pants-summary.csv", header=True, inferSchema=True)
        self.ranking_outers_data = self.spark.read.csv("../../data/raw/ranking-outers-summary.csv", header=True, inferSchema=True)
        self.ranking_shoes_data = self.spark.read.csv("../../data/raw/ranking-shoes-summary.csv", header=True, inferSchema=True)
        self.detail_clothes_top_data = self.spark.read.csv("../../data/raw/detail-clothes-top.csv", header=True, inferSchema=True)
        self.detail_pants_data = self.spark.read.csv("../../data/raw/detail-pants.csv", header=True, inferSchema=True)
        self.detail_outers_data = self.spark.read.csv("../../data/raw/detail-outers.csv", header=True, inferSchema=True)
        self.detail_shoes_data = self.spark.read.csv("../../data/raw/detail-shoes.csv", header=True, inferSchema=True)

    def process_ranking_data(self):
        # Ranking 데이터 합치기
        self.ranking_data = self.ranking_clothes_top_data.union(self.ranking_pants_data)\
                                                          .union(self.ranking_outers_data)\
                                                          .union(self.ranking_shoes_data)

        # Detail 데이터 합치기
        self.detail_data = self.detail_clothes_top_data.union(self.detail_pants_data)\
                                                        .union(self.detail_outers_data)\
                                                        .union(self.detail_shoes_data)

        # 합쳐진 데이터 확인
        self.ranking_data = self.ranking_data.drop("currentlyViewing", "currentlyBuying")
        
    def process_product_data(self):
        # productId, productName만 따로 빼서 새로운 변수에 저장
        self.product_data = self.ranking_data.select("productId", "productName").dropDuplicates(["productId", "productName"])

        # " " 기준으로 productName을 split하여 keywords 컬럼 추가
        self.product_data = self.product_data.withColumn(
            "keywords",
            expr("""
                filter(
                    split(productName, ' '), 
                    x -> (x RLIKE '[가-힣]' OR x RLIKE '^[a-zA-Z]+$') AND NOT x RLIKE '[a-zA-Z].*[0-9]|[0-9].*[a-zA-Z]'
                )
            """)
        )

        # 빈 키워드 데이터 제거
        empty_keyword_data = self.product_data.filter(col("keywords").isNull() | (size(col("keywords")) == 0))
        self.product_data = self.product_data.subtract(empty_keyword_data)

        # 1. 키워드를 개별 행으로 펼치기
        self.flattened_product_data = (
            self.product_data.withColumn("keyword", explode(col("keywords")))  # 배열을 개별 행으로 분리
        )

        # 2. 중첩된 배열 표현 제거 및 공백 제거
        self.flattened_product_data = (
            self.flattened_product_data.withColumn("keyword", expr("regexp_replace(keyword, '\\[|\\]', '')"))  # [ ] 제거
                                        .withColumn("keyword", trim(col("keyword")))  # 공백 제거
                                        .filter(col("keyword") != "")  # 빈 키워드 제거
        )

        # 3. 다시 배열로 결합
        self.flattened_product_data = (
            self.flattened_product_data.groupBy("productId", "productName")  # 원래 데이터 복구
                                        .agg(collect_list("keyword").alias("keywords"))  # 배열로 재결합
        )

        # 4. 특수 기호 기준으로 split
        self.processed_product_data = (
            self.flattened_product_data.withColumn("keyword", explode(col("keywords")))  # 배열을 개별 행으로 분리
        )

        self.processed_product_data = (
            self.processed_product_data.withColumn("keyword", explode(expr("split(keyword, '[^가-힣a-zA-Z0-9]')")))  # 특수 기호 기준 분리
                                      .withColumn("keyword", trim(col("keyword")))  # 공백 제거
                                      .filter(col("keyword") != "")  # 빈 키워드 제거
        )

        # 5. 다시 배열로 결합
        self.processed_product_data = (
            self.processed_product_data.groupBy("productId", "productName")  # 원래 데이터 복구
                                        .agg(collect_list("keyword").alias("keywords"))  # 배열로 재결합
        )

    def join_data(self):
        # Join 데이터
        self.joined_ranking_detail_data = self.ranking_data.join(
            self.detail_data, 
            on=["productId", "date"], 
            how="inner"
        )

    def calculate_conversion_rate(self):
        # 전환율 계산 (totalSales / views)
        self.joined_ranking_detail_data = self.joined_ranking_detail_data.withColumn(
            "conversionRate", 
            F.when(F.col("views") != 0, F.col("totalSales") / F.col("views")).otherwise(0)
        )

        # 전환율이 1을 초과하는 경우, 1 미만 중에서 최댓값으로 설정
        max_conversion_rate = self.joined_ranking_detail_data.filter(F.col("conversionRate") < 1).agg(
            F.max("conversionRate").alias("max_conversion_rate")
        ).collect()[0]["max_conversion_rate"]

        self.joined_ranking_detail_data = self.joined_ranking_detail_data.withColumn(
            "conversionRate", 
            F.when(F.col("conversionRate") > 1, max_conversion_rate).otherwise(F.col("conversionRate"))
        )

    def save_data(self):
        # pandas로 설정
        ranking_data_pd = self.joined_ranking_detail_data.toPandas()
        processed_product_data_pd = self.processed_product_data.toPandas()

        # 디렉토리 경로
        output_dir = "../../data/processed/"

        # 디렉토리 생성 및 권한 여부
        os.makedirs(output_dir, exist_ok=True)

        # 파일 경로 설정
        ranking_file = os.path.join(output_dir, "product_rankings.csv")
        keywords_file = os.path.join(output_dir, "product_keywords.csv")

        # 데이터 저장 (덮어쓰기)
        ranking_data_pd.to_csv(ranking_file, index=False, encoding="utf-8-sig")
        processed_product_data_pd.to_csv(keywords_file, index=False, encoding="utf-8-sig")

        print("Success data save")

    def run(self):
        # 데이터 처리 실행
        self.load_data()
        self.process_ranking_data()
        self.process_product_data()
        self.join_data()
        self.calculate_conversion_rate()
        self.save_data()


if __name__ == "__main__":
    processor = ProductRankingProcessor()
    processor.run()
