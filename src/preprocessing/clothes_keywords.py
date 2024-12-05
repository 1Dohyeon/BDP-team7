# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, when, to_timestamp
import sys
reload(sys)
sys.setdefaultencoding('utf8')
from pyspark.sql.functions import regexp_replace, lower, split, col
from pyspark.ml.feature import StopWordsRemover

spark = SparkSession.builder.appName("clothes_top").getOrCreate()

result_with_season = spark.read.csv("term_project_data/result_with_season.csv", header=True, inferSchema=True)

result_cleaned = result_with_season.withColumn(
    "cleanedProductName",
    regexp_replace(lower(col("productName")), "[^a-zA-Z가-힣0-9\\s]", "")
)

result_tokenized = result_cleaned.withColumn(
    "tokenizedProductName",
    split(col("cleanedProductName"), "\\s+")
)

stopwords_remover = StopWordsRemover(inputCol="tokenizedProductName", outputCol="keywords")
result_keywords = stopwords_remover.transform(result_tokenized)

for row in result_keywords.select("productId", "keywords").collect():
    print(
        str(row['productId']) + '\t' + 
        ",".join(row['keywords'])
    )

from pyspark.sql.functions import concat_ws

result_keywords_str = result_keywords.withColumn(
    "keywords", concat_ws(",", col("keywords"))
)

result_keywords_str.select("productId", "keywords").write.csv(
    "term_project_data/keywords_as_string", header=True, mode="overwrite"
)
