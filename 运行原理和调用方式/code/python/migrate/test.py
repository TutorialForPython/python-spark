import pandas as pd
df = pd.read_parquet('output/users.parquet', engine='pyarrow')
print(df)