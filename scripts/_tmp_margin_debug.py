import os, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
load_dotenv()

from jquants_client import get_weekly_margin_interest

# Try fetching ALL codes for a specific date
rows = get_weekly_margin_interest(date="2025-01-10")
print(f"Rows for date=2025-01-10: {len(rows)}")
if rows:
    import pandas as pd
    df = pd.DataFrame(rows)
    print(f"Columns: {df.columns.tolist()}")
    print(f"Unique codes: {df['Code'].nunique() if 'Code' in df.columns else 'N/A'}")
    print(f"Sample:\n{df.head(3)}")
