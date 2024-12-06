import os

from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer, OneHotEncoder, StringIndexer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, desc, explode, lower, regexp_replace, row_number, when
from pyspark.sql.window import Window


class ProductRecommendation:
    def __init__(self, product_rankings_path, product_keywords_path, output_dir):
        self.spark = SparkSession.builder.appName("BDP").getOrCreate()
        self.product_rankings = self.spark.read.csv(product_rankings_path, header=True, inferSchema=True)
        self.product_keywords = self.spark.read.csv(product_keywords_path, header=True, inferSchema=True)
        self.output_dir = output_dir

    def preprocess_data(self):
        # productId를 기준으로 date 내림차순 정렬
        window_spec = Window.partitionBy("productId").orderBy(desc("date"))
        
        # 각 productId 그룹에서 가장 최신 date인 행을 선택
        self.product_rankings = (
            self.product_rankings.withColumn("row_number", row_number().over(window_spec))
                                .filter(col("row_number") == 1)  # row_number가 1인 행만 선택
                                .drop("row_number")  # 임시 컬럼 제거
        )
        
        # 상위 33%인 100위를 기준으로 분리
        self.product_rankings = self.product_rankings.withColumn(
            "recommend",
            when(col("ranking") <= 100, 1).otherwise(0)
        )

        # ranking 컬럼 삭제
        self.product_rankings = self.product_rankings.drop("ranking")
        
        # DataFrame 조인
        self.joined_data = self.product_rankings.join(self.product_keywords, on="productId", how="inner")

        # 결측값 처리
        self.joined_data = self.joined_data.fillna({"colors": "unknown", "keywords": "", "rating": 0, "ratingCount": 0})

        # 범주형 데이터 인코딩
        brand_indexer = StringIndexer(inputCol="brandName", outputCol="brand_index")
        colors_indexer = StringIndexer(inputCol="colors", outputCol="colors_index")
        brand_encoder = OneHotEncoder(inputCol="brand_index", outputCol="brand_ohe")
        colors_encoder = OneHotEncoder(inputCol="colors_index", outputCol="colors_ohe")

        # 인코더 모델 피팅 및 변환
        pipeline = Pipeline(stages=[brand_indexer, colors_indexer, brand_encoder, colors_encoder])
        self.joined_data = pipeline.fit(self.joined_data).transform(self.joined_data)

        # keywords 열을 배열로 변환
        self.joined_data = self.joined_data.withColumn("keywords", F.split(F.col("keywords"), ", "))

    def vectorize_keywords(self):
        # Keywords 열을 단어로 분리
        keywords_exploded = self.joined_data.withColumn("keyword", explode(col("keywords")))
        
        # 모든 단어를 소문자로 변환하고 특수문자 제거
        keywords_cleaned = keywords_exploded.withColumn(
            "keyword",
            regexp_replace(lower(col("keyword")), "[^가-힣a-zA-Z]", "")
        )
        
        # 키워드 등장 횟수 계산
        top_keywords_df = (
            keywords_cleaned.groupBy("keyword")
            .count()
            .orderBy(col("count").desc())
            .limit(100)  # 상위 100개의 키워드 추출
        )

        top_keywords_list = [row["keyword"] for row in top_keywords_df.collect()]
        
        # 상위 100개의 키워드만 포함한 새 컬럼 생성
        top_keywords_list = [f'"{kw}"' for kw in top_keywords_list]
        keywords_expr = ', '.join(top_keywords_list)
        filtered_keywords = self.joined_data.withColumn(
            "filtered_keywords",
            F.expr(f"filter(keywords, x -> array_contains(array({keywords_expr}), x))")
        )

        # CountVectorizer를 사용하여 키워드 벡터화
        vectorizer = CountVectorizer(inputCol="filtered_keywords", outputCol="keyword_features")
        vectorized_model = vectorizer.fit(filtered_keywords)
        self.vectorized_data = vectorized_model.transform(filtered_keywords)

def main():
    # product_rankings_path, product_keywords_path
    product_rankings_path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/term_project_data/processed/product_rankings.csv"
    product_keywords_path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/term_project_data/processed/product_keywords.csv"
    output_dir = "../../data/output/"

    recommendation = ProductRecommendation(product_rankings_path, product_keywords_path, output_dir) 
    
    recommendation.preprocess_data()
    recommendation.vectorize_keywords()

if __name__ == "__main__":
    main()