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
WITH MonthlySales AS (
    SELECT 
        MONTH(t.transaction_date) AS month,
        p.product_name,
        SUM(t.quantity) AS total_units_sold,
        RANK() OVER (PARTITION BY MONTH(t.transaction_date) ORDER BY SUM(t.quantity) DESC) AS rnk
    FROM transactions_v1 t
    JOIN products_v1 p ON t.product_id = p.product_id
    GROUP BY month, p.product_name
)
SELECT month, product_name, total_units_sold 
FROM MonthlySales
WHERE rnk = 1;
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
sns.barplot(x="month", y="total_units_sold", hue="product_name", data=df, dodge=True)
plt.xticks(rotation=45, ha="right")
plt.xlabel("Month")
plt.ylabel("Total Units Sold")
plt.title("📊 Best-Selling Product in Each Month")
plt.legend(title="Product", bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.show()
