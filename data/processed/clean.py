import pandas as pd
import os
import glob

print("=== HeatTrace Data Pipeline ===\n")

# 1. EIA
print("Step 1: Cleaning EIA energy data...")
eia = pd.read_csv('data/raw/Complete_SEDS.csv')
eia.columns = eia.columns.str.strip()
eia_filtered = eia[eia['MSN'].isin(['TETCB', 'TEPRB', 'TERCB', 'EMTCB'])].copy()
eia_filtered = eia_filtered[eia_filtered['Year'] >= 2019]
eia_filtered['metric'] = eia_filtered['MSN'].map({
    'TETCB': 'energy_consumption', 'TEPRB': 'energy_production',
    'TERCB': 'renewable_consumption', 'EMTCB': 'co2_emissions',
})
eia_agg = eia_filtered.groupby(['StateCode', 'Year', 'metric'])['Data'].sum().reset_index()
eia_pivot = eia_agg.pivot_table(index=['StateCode', 'Year'], columns='metric', values='Data', aggfunc='sum').reset_index()
eia_pivot.columns.name = None
eia_pivot.rename(columns={'StateCode': 'state', 'Year': 'year'}, inplace=True)
print(f"  EIA: {len(eia_pivot)} rows, columns: {list(eia_pivot.columns)}")

# 2. ZenPower
print("\nStep 2: Cleaning ZenPower solar data...")
solar_frames = []
rec = pd.read_csv('data/raw/records.csv')
rec = rec[['state', 'kilowatt_value', 'issue_date']].rename(columns={'kilowatt_value':'kw','issue_date':'date'})
rec['date'] = pd.to_datetime(rec['date'], errors='coerce', utc=True)
solar_frames.append(rec)
for fname, scol, dcol in [
    ('Sullivan-Solar.csv','STATE','PERMIT_DATE'),
    ('Titan_All_Addresses.csv','STATE','PERMIT_DATE'),
    ('solar-city-permits.csv','STATE','ISSUE_DATE')]:
    df = pd.read_csv(f'data/raw/{fname}')[[scol, dcol]].rename(columns={scol:'state', dcol:'date'})
    df['kw'] = None
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    solar_frames.append(df)
for fname in ['freedom-forever.csv', 'sunrun.csv']:
    df = pd.read_csv(f'data/raw/{fname}')
    df['state'] = df['PROJECT_ADDRESS'].str.strip().str[-8:-6].str.strip()
    df['date'] = pd.to_datetime(df.get('INSTALL_DATE'), errors='coerce')
    df['kw'] = None
    solar_frames.append(df[['state', 'kw', 'date']])
solar_all = pd.concat(solar_frames, ignore_index=True)
solar_all['date'] = pd.to_datetime(solar_all['date'], errors='coerce', utc=False).dt.tz_localize(None)
solar_all['year'] = solar_all['date'].dt.year
solar_all = solar_all.dropna(subset=['state', 'year'])
solar_all = solar_all[solar_all['year'] >= 2019]
solar_all['state'] = solar_all['state'].str.upper().str.strip()
solar_agg = solar_all.groupby(['state', 'year']).agg(solar_permits=('kw','count'), solar_kw_total=('kw','sum')).reset_index()
print(f"  Solar: {len(solar_agg)} rows, {solar_all['state'].nunique()} states")

# 3. EPA
print("\nStep 3: Cleaning EPA air quality data...")
epa = pd.read_csv('data/raw/annual_aqi_by_county_2024.csv')
epa.columns = epa.columns.str.strip()
state_abbrev = {'Alabama':'AL','Alaska':'AK','Arizona':'AZ','Arkansas':'AR','California':'CA','Colorado':'CO','Connecticut':'CT','Delaware':'DE','Florida':'FL','Georgia':'GA','Hawaii':'HI','Idaho':'ID','Illinois':'IL','Indiana':'IN','Iowa':'IA','Kansas':'KS','Kentucky':'KY','Louisiana':'LA','Maine':'ME','Maryland':'MD','Massachusetts':'MA','Michigan':'MI','Minnesota':'MN','Mississippi':'MS','Missouri':'MO','Montana':'MT','Nebraska':'NE','Nevada':'NV','New Hampshire':'NH','New Jersey':'NJ','New Mexico':'NM','New York':'NY','North Carolina':'NC','North Dakota':'ND','Ohio':'OH','Oklahoma':'OK','Oregon':'OR','Pennsylvania':'PA','Rhode Island':'RI','South Carolina':'SC','South Dakota':'SD','Tennessee':'TN','Texas':'TX','Utah':'UT','Vermont':'VT','Virginia':'VA','Washington':'WA','West Virginia':'WV','Wisconsin':'WI','Wyoming':'WY','District Of Columbia':'DC'}
epa['state'] = epa['State'].map(state_abbrev)
epa['year'] = epa['Year']
epa_agg = epa.groupby(['state','year']).agg(median_aqi=('Median AQI','mean'), max_aqi=('Max AQI','max'), unhealthy_days=('Unhealthy Days','sum'), good_days=('Good Days','sum')).reset_index()
print(f"  EPA: {len(epa_agg)} rows")

# 4. UCSD Heat Map
print("\nStep 4: Cleaning UCSD heat map data...")
campus_frames = []
for f in glob.glob('data/raw/heat_mapping/UCSD_Campus_Data/*.txt'):
    try:
        df = pd.read_csv(f, sep=r'\s+', header=None)
        df.columns = ['time','lat','lon','col4','temp_c','humidity']
        df['date'] = os.path.basename(f)[:8]
        df['type'] = 'bike' if 'Bike' in f else 'walk'
        df['temp_f'] = (df['temp_c'] * 9/5 + 32).round(2)
        campus_frames.append(df[['date','lat','lon','temp_c','temp_f','humidity','type']])
    except Exception as e:
        print(f"  Skipped {os.path.basename(f)}: {e}")
awn_frames = []
for f in glob.glob('data/raw/heat_mapping/AWN/*.csv'):
    try:
        df = pd.read_csv(f)[['Date','Outdoor Temperature (°F)','Humidity (%)']].copy()
        df.columns = ['datetime','temp_f','humidity']
        awn_frames.append(df)
    except Exception as e:
        print(f"  Skipped AWN {os.path.basename(f)}: {e}")
campus_df = pd.concat(campus_frames, ignore_index=True) if campus_frames else pd.DataFrame()
awn_df = pd.concat(awn_frames, ignore_index=True) if awn_frames else pd.DataFrame()
if not campus_df.empty:
    print(f"  Campus: {len(campus_df)} GPS readings, {campus_df['date'].nunique()} sessions")
if not awn_df.empty:
    print(f"  AWN: {len(awn_df)} readings")

# 5. Merge + Derived Metrics
print("\nStep 5: Merging and computing metrics...")
national = eia_pivot.merge(solar_agg, on=['state','year'], how='left')
national = national.merge(epa_agg, on=['state','year'], how='left')
national['clean_energy_ratio'] = (national['renewable_consumption'] / national['energy_consumption']).round(4)
national['emission_intensity'] = (national['co2_emissions'] / national['energy_consumption']).round(4)
national['flag'] = 'No flag'
national.loc[(national['solar_permits'] > national['solar_permits'].quantile(0.75)) & (national['co2_emissions'] > national['co2_emissions'].quantile(0.5)), 'flag'] = 'Solar growth but high emissions'
national.loc[(national['median_aqi'] > 50) & (national['clean_energy_ratio'] < national['clean_energy_ratio'].quantile(0.25)), 'flag'] = 'High pollution + low clean energy'
national.loc[(national['solar_permits'] > national['solar_permits'].quantile(0.75)) & (national['median_aqi'] < 50), 'flag'] = 'Fast adoption + good air quality'
print(f"  National: {len(national)} rows, {len(national.columns)} columns")
print(f"  Flags:\n{national['flag'].value_counts().to_string()}")

# 6. Save
print("\nStep 6: Saving outputs...")
os.makedirs('data/clean', exist_ok=True)
national.to_csv('data/clean/heattrace_national.csv', index=False)
print("  Saved: heattrace_national.csv")
if not campus_df.empty:
    campus_df.to_csv('data/clean/heattrace_campus.csv', index=False)
    print("  Saved: heattrace_campus.csv")
if not awn_df.empty:
    awn_df.to_csv('data/clean/heattrace_awn.csv', index=False)
    print("  Saved: heattrace_awn.csv")

print("\n=== Done! Hand these to your teammates: ===")
print("  heattrace_national.csv  -> national map + charts")
print("  heattrace_campus.csv    -> UCSD GPS heat points")
print("  heattrace_awn.csv       -> UCSD weather station")
