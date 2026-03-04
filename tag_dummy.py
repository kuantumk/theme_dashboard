import json
import pandas as pd

# Load the list of tickers that were just screened by steady_trend
tickers = pd.read_csv('screening_output/consolidated/_steady_trend_03032026.txt', header=None)[0].tolist()

# Load the existing themes database
with open('data/ticker_themes.json', 'r') as f:
    data = json.load(f)

# Inject them 
added = 0
for t in tickers:
    data[t] = ["Maritime Shipping / Energy Leaders"]
    added += 1

print(f"Added manual themes for {added} new tickers.")

with open('data/ticker_themes.json', 'w') as f:
    json.dump(data, f, indent=2)
