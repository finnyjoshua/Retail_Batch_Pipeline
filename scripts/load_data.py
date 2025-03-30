from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, round
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv  # ✅ Import dotenv

# ✅ Load environment variables from .env file
load_dotenv()

# ✅ Initialize Spark Session
spark = SparkSession.builder.appName("GoldLayerProcessing").getOrCreate()

# ✅ Read cleaned data
customers_df = spark.read.csv("../output/cleaned_customers.csv", header=True, inferSchema=True)
products_df = spark.read.csv("../output/cleaned_products.csv", header=True, inferSchema=True)
transactions_df = spark.read.csv("../output/cleaned_transactions.csv", header=True, inferSchema=True)
reviews_df = spark.read.csv("../output/cleaned_reviews.csv", header=True, inferSchema=True)
reviews_df = reviews_df.drop("review_text")
stores_df = spark.read.csv("../output/cleaned_stores.csv", header=True, inferSchema=True)

# ✅ Validate data
customers_df.show()
products_df.show()
transactions_df.show()
reviews_df.show()
stores_df.show()

# ✅ Rename columns for consistency
expected_cols = ["review_id", "customer_id", "product_id", "rating", "review_date"]
reviews_df = reviews_df.toDF(*expected_cols)

# ✅ Aggregate product reviews
product_reviews_df = reviews_df.groupBy("product_id").agg(
    round(avg("rating"), 2).alias("avg_rating"),
    count("review_id").alias("total_reviews")
)

# ✅ Convert Spark DataFrames to Pandas
customers_pd = customers_df.toPandas()
products_pd = products_df.toPandas()
transactions_pd = transactions_df.toPandas()
reviews_pd = reviews_df.toPandas()
stores_pd = stores_df.toPandas()

# ✅ Get database credentials from environment variables
db_user = os.getenv("MYSQL_USER", "root")
db_password = os.getenv("MYSQL_PASSWORD", "")
db_host = os.getenv("MYSQL_HOST", "mysql")  # ✅ Change `localhost` to `mysql`
db_name = os.getenv("MYSQL_DATABASE", "retail_db")

# ✅ Connect to MySQL
engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}")

# ✅ Load data into MySQL
customers_pd.to_sql("customers_v1", con=engine, if_exists="replace", index=False)
products_pd.to_sql("products_v1", con=engine, if_exists="replace", index=False)
transactions_pd.to_sql("transactions_v1", con=engine, if_exists="replace", index=False)
reviews_pd.to_sql("reviews_v1", con=engine, if_exists="replace", index=False)
stores_pd.to_sql("stores_v1", con=engine, if_exists="replace", index=False)

print("✅ All DataFrames uploaded successfully to MySQL!")

# ✅ Stop Spark Session
spark.stop()
