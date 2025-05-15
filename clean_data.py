import pyarrow as pa
import pandas as pd

table = pd.read_parquet('file-staging/yellow_tripdata_2023-01.parquet')

df = pd.DataFrame(table)

print(df.head())

print(df.columns)

