import pandas as pd

df = pd.read_csv('pattern.csv', encoding='utf-8')
print(df[df['total'] < 200])
df = df[df['total'] < 200]
df = df[['place_1', 'place_2', 'total']].reset_index(drop=True)
df.to_csv('df.csv')