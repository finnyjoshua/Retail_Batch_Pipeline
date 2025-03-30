from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("RetailDataProcessing").getOrCreate()

DATA_PATH = "../data/"
files = {
    "customers": "customers.csv",
    "products": "products.csv",
    "transactions": "transactions.csv",
    "store_locations": "store_locations.csv",
    "reviews": "reviews.csv"
}

# Read CSV files into DataFrames
customers_df = spark.read.csv(DATA_PATH + files["customers"], header=True, inferSchema=True)
products_df = spark.read.csv(DATA_PATH + files["products"], header=True, inferSchema=True)
transactions_df = spark.read.csv(DATA_PATH + files["transactions"], header=True, inferSchema=True)
store_locations_df = spark.read.csv(DATA_PATH + files["store_locations"], header=True, inferSchema=True)
reviews_df = spark.read.csv(DATA_PATH + files["reviews"], header=True, inferSchema=True)

# Show Sample Data

customers_df.show(5)
