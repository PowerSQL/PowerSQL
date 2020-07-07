from sqlalchemy import create_engine

import pandas as pd

df = pd.read_csv('product_sales.csv')
engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')
df.to_sql('product_sales', engine)
