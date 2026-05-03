"""
DJIA（ダウ平均）構成銘柄 30 社。
yfinance ティッカーそのまま（.T なし）。
2024/11 時点の構成（NVDA・SHW 追加、INTC・MMM 除外）。
"""

DOW30: dict[str, str] = {
    "AAPL":  "Apple",
    "AMGN":  "Amgen",
    "AMZN":  "Amazon",
    "AXP":   "American Express",
    "BA":    "Boeing",
    "CAT":   "Caterpillar",
    "CRM":   "Salesforce",
    "CSCO":  "Cisco",
    "CVX":   "Chevron",
    "DIS":   "Walt Disney",
    "DOW":   "Dow Inc.",
    "GS":    "Goldman Sachs",
    "HD":    "Home Depot",
    "HON":   "Honeywell",
    "IBM":   "IBM",
    "JNJ":   "Johnson & Johnson",
    "JPM":   "JPMorgan Chase",
    "KO":    "Coca-Cola",
    "MCD":   "McDonald's",
    "MRK":   "Merck",
    "MSFT":  "Microsoft",
    "NKE":   "Nike",
    "NVDA":  "Nvidia",
    "PG":    "Procter & Gamble",
    "SHW":   "Sherwin-Williams",
    "TRV":   "Travelers",
    "UNH":   "UnitedHealth",
    "V":     "Visa",
    "VZ":    "Verizon",
    "WMT":   "Walmart",
}
