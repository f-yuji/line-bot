import pickle
import pandas as pd
import numpy as np

with open('outputs/rebound_grid_search/cands_2020_2026.pkl', 'rb') as f:
    data = pickle.load(f)
df = pd.DataFrame(data['candidates'])

h5 = df[
    (df['signal_probability'] >= 0.65) &
    (df['drop_from_20d_high_pct'] <= -8) &
    (df['market_regime'] != 'panic_selloff')
].copy()

print(f'H5 candidates: {len(h5)}')
null_count = h5['margin_ratio'].isna().sum()
null_pct = h5['margin_ratio'].isna().mean() * 100
print(f'margin_ratio null: {null_count} ({null_pct:.1f}%)')
mr = h5['margin_ratio'].dropna()
print('margin_ratio stats:')
print(f'  min={mr.min():.2f}  max={mr.max():.2f}  mean={mr.mean():.2f}  median={mr.median():.2f}')
for p in [10, 25, 50, 75, 90]:
    print(f'  P{p}={np.percentile(mr, p):.2f}')
print()
for t in [5, 10, 20, 30]:
    n = (mr <= t).sum()
    print(f'  <= {t:2d}x : {n} ({n/len(mr)*100:.1f}%)')
print(f'  total with data: {len(mr)}')
