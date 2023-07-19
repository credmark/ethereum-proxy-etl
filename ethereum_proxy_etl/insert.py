import os
import sys
import pandas as pd

# from backfill import backfill


# ---- STEP 0 ---- #
# backfill()

# ---- STEP 1 ---- #
df_list = []
processed_csv = []
for x in os.listdir():
    if not x.endswith(".csv"):
        continue
    print(x)
    df_list.append(pd.read_csv(x))
    processed_csv.append(x)

df = pd.concat(df_list, axis=0)
df.to_parquet('combined.parquet', index=False)

for x in processed_csv:
    os.remove(x)

# ---- STEP 2 ---- #
# df = pd.read_parquet('combined.parquet')
df = df[['proxy_address', 'proxy_type', 'implementation_address', 'updated_at']]
print(df.groupby(['proxy_type'])['proxy_type'].count())

duplicate = df[df.duplicated(subset=['proxy_address'], keep=False)].sort_values(
    ['proxy_address', 'proxy_type'])

count = 0
for group, df in duplicate.groupby('proxy_address'):
    if len(df['implementation_address'].unique()) == 1:
        continue

    # Handled
    if (len(df) == 2
            and 'eip_1967_direct' in df['proxy_type'].values
            and 'eip_897' in df['proxy_type'].values):
        continue

    # Handled
    if (len(df) == 2
            and 'eip_1967_beacon' in df['proxy_type'].values
            and 'eip_897' in df['proxy_type'].values):
        continue

    count += 1
    print(group)
    print(df[['implementation_address', 'proxy_type']])

print(count)

if count > 0:
    print('Found unhandled duplicates')
    sys.exit(1)

# ---- STEP 3 ---- #
sort_key = {
    'eip_1967_beacon': 1,  # BEACON_SLOT -> implementation()
    'eip_897': 2,  # implementation()
    'eip_1967_direct': 3,  # EIP_1967_SLOT
    'eip_1167_minimal': 4,
    'oz': 5,
    'eip_1822': 6,
    'gnosis_safe': 7,
    'comptroller': 8,
    'ara': 9,
    'p_proxy': 10,
    'one_to_one': 11,
    'many_to_one': 12,
}
df = pd.read_parquet('combined.parquet')
df = df[['proxy_address', 'proxy_type', 'implementation_address', 'updated_at']]
df = df.sort_values('proxy_type', key=lambda x: x.map(sort_key))
df = df.drop_duplicates(subset=['proxy_address'], keep='first')
df.to_csv('combined.csv', index=False)
