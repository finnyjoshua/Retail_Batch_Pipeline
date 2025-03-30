import pandas as pd
import pymysql
import sqlalchemy
import matplotlib.pyplot as plt
import seaborn as sns


db_user = "root"
db_password = "dragonemperor"
db_host = "localhost"
db_name = "retail_db"

# ✅ Create Connection
engine = sqlalchemy.create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}")

# ✅ SQL Query (Include product_name)
query = """
SELECT s.city, s.state, p.product_name, 
       SUM(t.quantity) AS total_units_sold, 
       SUM(t.total_amount) AS total_revenue 
FROM transactions_v1 t
JOIN products_v1 p ON t.product_id = p.product_id  
JOIN stores_v1 s ON t.store_id = s.store_id 
GROUP BY s.city, s.state, p.product_name 
ORDER BY total_units_sold DESC 
LIMIT 100;
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
sns.barplot(x="product_name", y="total_units_sold", hue="city", data=df, dodge=True)
plt.xticks(rotation=45, ha="right")
plt.xlabel("Product Name")
plt.ylabel("Total Units Sold")
plt.title("Top 100 Best-Selling Products by City")
plt.legend(title="City", bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.show()
