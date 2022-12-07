import pandas as pd


# Load vehicle file for merging
vehicles = pd.read_csv('vehicles/vehicles.csv')

# Merge
gps = pd.read_csv('gps/gps_concat.csv')
gps.rename(columns={'imei':'gps'}, inplace=True)
gps = gps.merge(vehicles, how='inner', on='gps')

# Save
gps.to_csv('gps_csv/gps.csv', index=False)