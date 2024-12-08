# -*- coding: utf-8 -*-

#ranking : productId, date, brandName, productName, price, ranking, totalSales, colors, category, discount_rate
#detail : view, likes, rating, ratingCount, totalSales
#productId, date, brandName, productName, colors, ranking, price, originalPrice, discountRate, totalSales, category, views, likes, rating, ratingCount, conversionRate, season

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, when, to_timestamp
import sys
reload(sys)
sys.setdefaultencoding('utf8')

spark = SparkSession.builder.appName("clothes_top").getOrCreate()

ranking = spark.read.csv("term_project_data/ranking-clothes-top-summary.csv", header=True, inferSchema=True)
detail = spark.read.csv("term_project_data/detail-clothes-top.csv", header=True, inferSchema=True)

ranking.createOrReplaceTempView("ranking")
detail.createOrReplaceTempView("detail")

result = spark.sql("""
    SELECT DISTINCT
        r.productId, r.date, r.brandName, r.productName, r.colors, r.ranking, r.price, r.discountRate,
        r.totalSales,  r.category, d.views, d.likes, d.rating, d.ratingCount,
        CASE 
            WHEN d.views IS NULL OR r.totalSales IS NULL THEN NULL
            WHEN d.views > 0 THEN r.totalSales / d.views
            ELSE 0
        END AS conversionRate
    FROM ranking r
    JOIN detail d
    ON r.productId = d.productId AND r.date = d.date
    ORDER BY r.ranking DESC
""")

'''conversion_rate = result.withColumn(
    "conversionRate",
    when((col("views").isNull()) | (col("totalSales").isNull()), None)
    .when(col("views") > 0, col("totalSales") / col("views"))
    .otherwise(None)
)'''

result_with_original_price = result.withColumn(
    "originalPrice",
    when((col("discountRate") > 1), col("price") / (1 - (col("discountRate") / 100))) 
    .when((col("discountRate") > 0) & (col("discountRate") <= 1), col("price") / (1 - col("discountRate")))  
    .otherwise(col("price")) 
)

result_with_timestamp = result_with_original_price.withColumn("date", to_timestamp(col("date").cast("string"), "yyyyMMddHH"))

result_with_season = result_with_timestamp.withColumn(
    "season", when((month(col("date")).isin(3, 4, 5)), "spring")
    .when((month(col("date")).isin(6, 7, 8, 9)), "summer")
    .when((month(col("date")).isin(10, 11)), "fall")
    .when((month(col("date")).isin(12, 1, 2)), "winter")
)

#result_df.show(truncate=False)

for row in result_with_season.collect():
    print(
        str(row['productId']) + '\t' + 
        str(row['date']) + '\t' + 
        str(row['brandName']) + '\t' + 
        str(row['productName']) + '\t' +         
        str(row['colors']) + '\t' +        
        str(row['ranking']) + '\t' + 
        str(row['price']) + '\t' +          
        str(row['originalPrice']) + '\t' +         
        "{:.2f}".format(row['discountRate']) + '\t' + 
        str(row['totalSales']) + '\t' + 
        str(row['category']) + '\t' + 
        str(row['views']) + '\t' + 
        str(row['likes']) + '\t' + 
        str(row['rating']) + '\t' + 
        str(row['ratingCount']) + '\t' +
        (str(row['conversionRate']) if row['conversionRate'] is not None else "None") + '\t' +
        str(row['season'])
    )
#productId, date, brandName, productName, colors, ranking, price, originalPrice, discountRate, totalSales, category, views, likes, rating, ratingCount, conversionRate, season

from pyspark.sql.functions import regexp_replace, lower, split, col
from pyspark.ml.feature import StopWordsRemover

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

result_keywords.select("productId", "keywords").show(truncate=False)
