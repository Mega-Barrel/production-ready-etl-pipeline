import pandas as pd

filepath = "./dataset/"

df = pd.read_csv(f'{filepath}2022-01-03/2022-01-03_BINS_XETR08.csv')

print(df.head())