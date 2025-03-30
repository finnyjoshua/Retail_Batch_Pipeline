import pandas as pd
import pymysql
import sqlalchemy
import matplotlib.pyplot as plt
import seaborn as sns

db_user = "root"
db_password = "dragonemperor"
db_host = "localhost"
db_name = "retail_db"


engine = sqlalchemy.create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}")


query = """
WITH product_sales AS (
    SELECT s.city, s.state, p.product_name, 
           SUM(t.quantity) AS total_units_sold
    FROM transactions_v1 t
    JOIN products_v1 p ON t.product_id = p.product_id
    JOIN stores_v1 s ON t.store_id = s.store_id
    GROUP BY s.city, s.state, p.product_name
),
avg_sales AS (
    SELECT AVG(total_units_sold) AS avg_units_sold FROM product_sales
)
SELECT ps.city, ps.state, ps.product_name, ps.total_units_sold
FROM product_sales ps, avg_sales a
WHERE ps.total_units_sold < a.avg_units_sold
LIMIT 10;
"""


try:
    df = pd.read_sql(query, con=engine)
    print(df.head())  # Print sample data
except Exception as e:
    print(f"Error: {e}")
    exit()


if df.empty:
    print("No data returned from query!")
    exit()

# ✅ Plot the Least Performing Products
plt.figure(figsize=(12, 6))
sns.barplot(x="product_name", y="total_units_sold", hue="city", data=df, dodge=True)
plt.xticks(rotation=45, ha="right")
plt.xlabel("Product Name")
plt.ylabel("Total Units Sold")  # Fixed label
plt.title("📉 Underperforming Products by Location")
plt.legend(title="City", bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.show()
