from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, regexp_replace, to_date
from pyspark.sql.types import IntegerType, FloatType


spark = SparkSession.builder.appName("RetailDataProcessing").getOrCreate()


def standardize_columns(df):
    return df.toDF(*[col.lower().replace(" ", "_") for col in df.columns])

def trim_columns(df):
    for col_name in df.columns:
        df = df.withColumn(col_name, trim(col(col_name)))
    return df


customers_df = spark.read.csv("../data/customers.csv", 
    header=True, 
    inferSchema=True, 
    multiLine=True,  # ✅ Handles multi-line fields properly
    escape='"',      # ✅ Ensures quoted values are correctly interpreted
    sep=",")
products_df = spark.read.csv("../data/products.csv", header=True, inferSchema=True)
transactions_df = spark.read.csv("../data/transactions.csv", header=True, inferSchema=True)
reviews_df = spark.read.csv("../data/reviews.csv", header=True, inferSchema=True)
stores_df = spark.read.csv("../data/store_locations.csv", header=True, inferSchema=True)
customers_df.show(5)

customers_df = standardize_columns(customers_df)
products_df = standardize_columns(products_df)
transactions_df = standardize_columns(transactions_df)
reviews_df = standardize_columns(reviews_df)
stores_df = standardize_columns(stores_df)
customers_df.show(5)

#customers_df = customers_df.dropDuplicates().dropna()
products_df = products_df.dropDuplicates().dropna()
transactions_df = transactions_df.dropDuplicates().dropna()
reviews_df = reviews_df.dropDuplicates().dropna()
stores_df = stores_df.dropDuplicates().dropna()
customers_df.show(5)

customers_df = customers_df.drop("address")
customers_df = trim_columns(customers_df)
products_df = trim_columns(products_df)
transactions_df = trim_columns(transactions_df)
reviews_df = trim_columns(reviews_df)
stores_df = trim_columns(stores_df)
reviews_df.show()
customers_df.show(5)
reviews_df=reviews_df.withColumn("review_date", to_date(col("review_date")))
transactions_df = transactions_df.withColumn("quantity", col("quantity").cast(IntegerType())) \
                                 .withColumn("total_amount", col("total_amount").cast(FloatType()))

reviews_df = reviews_df.withColumn("rating", col("rating").cast(IntegerType())) \
                       .withColumn("review_date", to_date(col("review_date"), "yyyy-MM-dd'T'HH:mm:ss"))


customers_df = customers_df.withColumn("email", lower(col("email")))  # Convert emails to lowercase
customers_df = customers_df.withColumn("phone", regexp_replace(col("phone"), "[^0-9]", ""))  # Remove special chars from phone numbers




customers_df.cache()

customers_df.write.mode("overwrite").option("header", "true").csv("../output/cleaned_customers.csv")
products_df.write.mode("overwrite").option("header", "true").csv("../output/cleaned_products.csv")
transactions_df.write.mode("overwrite").option("header", "true").csv("../output/cleaned_transactions.csv")
reviews_df.write.mode("overwrite").option("header", "true").csv("../output/cleaned_reviews.csv")
stores_df.write.mode("overwrite").option("header", "true").csv("../output/cleaned_stores.csv")



spark.stop()
