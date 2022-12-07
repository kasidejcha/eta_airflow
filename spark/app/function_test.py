import pandas as pd

df = pd.read_csv('distance_station.csv')
df.to_csv('distance_station.csv', index=True)
