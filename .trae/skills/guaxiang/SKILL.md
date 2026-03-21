---
name: "guaxiang"
description: "Fetches Chinese futures market main contracts K-line data, calculates MACD indicators, labels them as 'guaxiang', and saves to CSV. Invoke when user asks for futures MACD analysis or guaxiang labeling."
---

# Guaxiang - 期货MACD卦象分析

This skill fetches K-line data for all main contracts in China's futures market, calculates MACD indicators, labels them according to specific rules, and saves results to CSV.

## When to Invoke

- User requests futures MACD analysis
- User asks for "卦象" (guaxiang) labeling
- User wants to analyze Chinese futures market with MACD indicators

## Prerequisites

1. **TqSdk Account**: Requires a valid account from https://account.shinnytech.com/
2. **Environment Variables**: Set up `.env` file with:
   ```
   TQSDK_USERNAME=your_username
   TQSDK_PASSWORD=your_password
   ```
3. **Dependencies**: Install required packages:
   ```
   pip install tqsdk pandas numpy
   ```

## How to Use

Run the script:
```bash
python tq_macd_analysis.py
```

## Output

- CSV file named `guaxiang_yyyymmdd.csv` containing:
  - 日期 (Date)
  - 合约 (Contract)
  - 日/周/月 收盘价, diff, dea, macd, 卦象

## Guaxiang Labeling Logic

```
if dea < 0 and diff > dea: return '1'
elif dea > 0 and diff > dea: return '2'
elif dea > 0 and diff < dea: return '3'
elif dea < 0 and diff < dea: return '4'
else: return '0'
```

## Supported Exchanges & Contracts

- **CFFEX**: IF, IC, IH, TF, T, TS
- **SHFE**: cu, al, zn, pb, ni, sn, au, ag, rb, hc, sp, bu, ru, fu
- **DCE**: m, y, p, c, cs, l, v, pp, j, jm, i, eg
- **CZCE**: TA, MA, ZC, SF, SM, RS, WH, RI, AP

## Key Features

1. Fetches daily, weekly K-lines (150 bars each)
2. Synthesizes monthly K-lines from weekly data (120 weeks for ~30 months)
3. Uses hourly K-line to update daily/weekly data in real-time
4. Calculates MACD using TqSdk's built-in MACD function
5. Labels each timeframe with guaxiang values
