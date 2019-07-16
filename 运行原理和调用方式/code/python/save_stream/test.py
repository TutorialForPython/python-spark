import pandas as pd
df = pd.read_parquet('output/1554809760_1.parquet', engine='pyarrow')
print(df.count())