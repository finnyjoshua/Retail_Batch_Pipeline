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
WITH sales_growth AS (
    SELECT s.city, s.state, p.product_name, 
           MONTH(t.transaction_date) AS month,
           SUM(t.quantity) AS total_units_sold
    FROM transactions_v1 t
    JOIN products_v1 p ON t.product_id = p.product_id
    JOIN stores_v1 s ON t.store_id = s.store_id
    GROUP BY s.city, s.state, p.product_name, month
)
SELECT city, state, product_name, 
       CASE 
           WHEN MIN(total_units_sold) = 0 THEN NULL 
           ELSE (MAX(total_units_sold) - MIN(total_units_sold)) / MIN(total_units_sold) * 100 
       END AS growth_percentage
FROM sales_growth
GROUP BY city, state, product_name
ORDER BY growth_percentage DESC
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


plt.figure(figsize=(12, 6))
sns.barplot(x="product_name", y="growth_percentage", hue="city", data=df, dodge=True)
plt.xticks(rotation=45, ha="right")
plt.xlabel("Product Name")
plt.ylabel("Growth Percentage")
plt.title("Top 10 High-Growth Products by Location")
plt.legend(title="City", bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.show()
