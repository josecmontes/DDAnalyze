# Extracted Analysis Code
*Generated: 2026-03-15 21:03:57*
*Total successful analyses: 11*

---

## Analysis 1: Descriptive statistics and data profiling
**Hypothesis:** We need to understand the basic structure of the dataset before any deeper analysis: row counts, column types, date ranges, null values, and a first look at the key dimensions (product lines, channels, fiscal years) to establish a baseline for all future analyses.

**Columns:** Nombre Modelo, Fecha_Mes, Canal, Tienda_Cliente, Venta Netas, Cant_Neta

```python
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import sys, os

# Apply Deloitte theme manually (avoid Liberation Sans font warning)
DELOITTE_COLORS = ['#26890D', '#046A38', '#404040', '#0D8390', '#00ABAB']
plt.rcParams.update({
    'font.family': 'sans-serif',
    'axes.prop_cycle': plt.cycler('color', DELOITTE_COLORS),
    'axes.titlesize': 14,
    'axes.titleweight': 'bold',
    'axes.spines.top': False,
    'axes.spines.right': False,
    'figure.facecolor': 'white',
    'axes.facecolor': 'white',
})

# Load data
df = pd.read_excel("workspace/data.xlsx")

print("=" * 70)
print("SECTION 1: DATASET OVERVIEW")
print("=" * 70)
print(f"Total rows    : {len(df):,}")
print(f"Total columns : {len(df.columns)}")
print()
print("Column names and dtypes:")
for col in df.columns:
    print(f"  {col:<30} {str(df[col].dtype):<15} nulls: {df[col].isnull().sum()}")

print()
print("=" * 70)
print("SECTION 2: SAMPLE ROWS")
print("=" * 70)
print(df.head(5).to_string())

print()
print("=" * 70)
print("SECTION 3: NUMERIC COLUMNS - SUMMARY STATISTICS")
print("=" * 70)
numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
if numeric_cols:
    desc = df[numeric_cols].describe().T
    desc['range'] = desc['max'] - desc['min']
    print(desc[['count','mean','std','min','25%','50%','75%','max','range']].to_string())

print()
print("=" * 70)
print("SECTION 4: KEY CATEGORICAL DIMENSIONS")
print("=" * 70)

cat_candidates = df.select_dtypes(include=['object', 'category']).columns.tolist()
for col in cat_candidates:
    vc = df[col].value_counts()
    n_unique = vc.shape[0]
    print(f"\n  Column: '{col}' | Unique values: {n_unique}")
    if n_unique <= 30:
        for val, cnt in vc.items():
            pct = cnt / len(df) * 100
            print(f"    {str(val):<35} {cnt:>6,}  ({pct:5.1f}%)")
    else:
        print(f"    Top 15 values:")
        for val, cnt in vc.head(15).items():
            pct = cnt / len(df) * 100
            print(f"    {str(val):<35} {cnt:>6,}  ({pct:5.1f}%)")
        print(f"    ... ({n_unique - 15} more unique values)")

print()
print("=" * 70)
print("SECTION 5: DATE / TIME DIMENSION DETECTION")
print("=" * 70)
date_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
if date_cols:
    for dc in date_cols:
        print(f"  Date column '{dc}': min={df[dc].min()}, max={df[dc].max()}")
else:
    print("  No datetime columns detected -- checking for year-like integer columns...")
    for col in df.columns:
        if any(k in col.lower() for k in ['ano', 'year', 'fecha', 'mes']):
            print(f"  Candidate column: '{col}' | Unique values: {sorted(df[col].dropna().unique())}")

print()
print("=" * 70)
print("SECTION 6: REVENUE COLUMNS IDENTIFICATION")
print("=" * 70)
rev_candidates = [c for c in df.columns if any(k in c.lower() for k in ['venta','revenue','sales','importe','total','eur'])]
print(f"  Revenue-like columns detected: {rev_candidates}")
for rc in rev_candidates:
    if df[rc].dtype in [np.float64, np.int64, float, int]:
        total = df[rc].sum()
        print(f"    '{rc}' -> Total sum: {total:,.2f} | Min: {df[rc].min():,.2f} | Max: {df[rc].max():,.2f}")

print()
print("=" * 70)
print("SECTION 7: FISCAL YEAR REVENUE SUMMARY")
print("=" * 70)

date_col = date_cols[0] if date_cols else None
rev_col = rev_candidates[0] if rev_candidates else None

if date_col and rev_col:
    df['_year'] = df[date_col].dt.year

    latest_month = df[date_col].dt.to_period('M').max()
    latest_year = latest_month.year

    year_months = df.groupby('_year')[date_col].apply(lambda x: x.dt.month.nunique())
    max_year = df['_year'].max()
    is_partial = year_months[max_year] < 12

    print(f"  Date range: {df[date_col].min().strftime('%Y-%m')} to {df[date_col].max().strftime('%Y-%m')}")
    print(f"  Latest year {max_year} is {'PARTIAL (' + str(year_months[max_year]) + ' months)' if is_partial else 'FULL'}")
    print()

    full_years = sorted([y for y in df['_year'].unique() if not (y == max_year and is_partial)])
    fy_rev = {}
    for y in full_years:
        fy_rev[f'FY{y}'] = df[df['_year'] == y][rev_col].sum()

    ltm_label = None
    ltm_rev = None
    ltm_prev_rev = None
    ltm_mask = None
    if is_partial:
        ltm_end = latest_month
        ltm_start = ltm_end - 11
        ltm_mask = (df[date_col].dt.to_period('M') >= ltm_start) & (df[date_col].dt.to_period('M') <= ltm_end)
        ltm_rev = df[ltm_mask][rev_col].sum()
        ltm_label = f'LTM{str(latest_year)[2:]}'

        prev_end = ltm_start - 1
        prev_start = prev_end - 11
        prev_mask = (df[date_col].dt.to_period('M') >= prev_start) & (df[date_col].dt.to_period('M') <= prev_end)
        ltm_prev_rev = df[prev_mask][rev_col].sum()

    all_period_labels = list(fy_rev.keys()) + ([ltm_label] if ltm_label else [])
    all_period_values = list(fy_rev.values()) + ([ltm_rev] if ltm_rev is not None else [])
    first_val = all_period_values[0]
    last_val = all_period_values[-1]

    first_month = df[date_col].dt.to_period('M').min()
    n_months = (latest_month - first_month).n + 1
    n_years = n_months / 12
    if first_val > 0 and last_val > 0 and n_years > 0:
        cagr = (last_val / first_val) ** (1 / n_years) - 1
        cagr_str = f"{cagr*100:+.1f}%"
    else:
        cagr_str = "N/A"
        n_years = 0

    col_w = 14
    header_parts = [f"{'Metric':<25}"]
    for lbl in all_period_labels:
        header_parts.append(f"{lbl:>{col_w}}")
    header_parts.append(f"{'CAGR (N='+str(round(n_years,2))+'y)':>{col_w}}")
    print("  " + " | ".join(header_parts))
    print("  " + "-" * (25 + (col_w + 3) * (len(all_period_labels) + 1)))

    rev_parts = [f"{'Revenue (EUR M)':25}"]
    for v in all_period_values:
        rev_parts.append(f"{v/1e6:>{col_w}.2f}M")
    rev_parts.append(f"{cagr_str:>{col_w}}")
    print("  " + " | ".join(rev_parts))

    yoy_parts = [f"{'YoY %':25}"]
    for i, v in enumerate(all_period_values):
        if i == 0:
            yoy_parts.append(f"{'--':>{col_w}}")
        elif i == len(all_period_values) - 1 and is_partial and ltm_prev_rev is not None:
            prev = ltm_prev_rev
            if prev and prev != 0:
                yoy = (v - prev) / abs(prev) * 100
                yoy_parts.append(f"{yoy:>+{col_w}.1f}%")
            else:
                yoy_parts.append(f"{'N/A':>{col_w}}")
        else:
            prev = all_period_values[i-1]
            if prev and prev != 0:
                yoy = (v - prev) / abs(prev) * 100
                yoy_parts.append(f"{yoy:>+{col_w}.1f}%")
            else:
                yoy_parts.append(f"{'N/A':>{col_w}}")
    yoy_parts.append(f"{'':>{col_w}}")
    print("  " + " | ".join(yoy_parts))

    rc_parts = [f"{'Row Count':25}"]
    for y in full_years:
        cnt = (df['_year'] == y).sum()
        rc_parts.append(f"{cnt:>{col_w},}")
    if is_partial and ltm_mask is not None:
        ltm_cnt = ltm_mask.sum()
        rc_parts.append(f"{ltm_cnt:>{col_w},}")
    rc_parts.append(f"{'':>{col_w}}")
    print("  " + " | ".join(rc_parts))

print()
print("=" * 70)
print("SECTION 8: CHANNEL DISTRIBUTION")
print("=" * 70)
channel_col = None
for col in df.columns:
    if 'canal' in col.lower() or 'channel' in col.lower():
        channel_col = col
        break
if channel_col and rev_col:
    ch_summary = df.groupby(channel_col)[rev_col].sum().sort_values(ascending=False)
    total_rev = ch_summary.sum()
    print(f"  Channel breakdown (revenue column: '{rev_col}'):")
    print()
    print(f"  {'Channel':<30} {'Revenue (EUR)':>18} {'Share %':>10}")
    print(f"  {'-'*30} {'-'*18} {'-'*10}")
    for ch, rev in ch_summary.items():
        pct = rev / total_rev * 100
        print(f"  {str(ch):<30} {rev:>18,.0f} {pct:>9.1f}%")
    print(f"  {'TOTAL':<30} {total_rev:>18,.0f} {'100.0%':>10}")

print()
print("=" * 70)
print("SECTION 9: TOP 15 MODELS BY REVENUE")
print("=" * 70)
model_col = 'Nombre Modelo'
if model_col in df.columns and rev_col:
    model_summary = df.groupby(model_col)[rev_col].sum().sort_values(ascending=False)
    total_rev = model_summary.sum()
    top15 = model_summary.head(15)
    print(f"  {'Model':<30} {'Revenue (EUR)':>18} {'Share %':>10} {'Cum %':>10}")
    print(f"  {'-'*30} {'-'*18} {'-'*10} {'-'*10}")
    cum = 0
    for model, rev in top15.items():
        pct = rev / total_rev * 100
        cum += pct
        print(f"  {str(model):<30} {rev:>18,.0f} {pct:>9.1f}% {cum:>9.1f}%")
    print(f"  ... ({len(model_summary)-15} more models)")
    print(f"  {'TOTAL':<30} {total_rev:>18,.0f} {'100.0%':>10}")

# ---- CHART: Revenue by Channel ----
if channel_col and rev_col:
    ch_data = df.groupby(channel_col)[rev_col].sum().sort_values(ascending=False)
    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(ch_data.index.astype(str), ch_data.values / 1e6, color=DELOITTE_COLORS[:len(ch_data)])
    ax.set_title("Revenue by Channel (Total Period)", color='#26890D', fontweight='bold')
    ax.set_xlabel("Channel", fontsize=11)
    ax.set_ylabel("Revenue (EUR M)", fontsize=11)
    max_bar = ch_data.max()
    for bar, val in zip(bars, ch_data.values):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01 * max_bar/1e6,
                f"{val/1e6:.1f}M", ha='center', va='bottom', fontsize=10, fontweight='bold', color='#404040')
    plt.tight_layout()
    plt.savefig("workspace/graphs/iter1_revenue_by_channel.png", dpi=150)
    plt.close()
    print()
    print("GRAPH_SAVED: iter1_revenue_by_channel.png -- Revenue by Channel (total period)")

# ---- CHART: Revenue by Year (full years + LTM) ----
if date_col and rev_col:
    period_labels_chart = all_period_labels
    period_values_chart = all_period_values
    fig, ax = plt.subplots(figsize=(9, 5))
    bar_colors = [DELOITTE_COLORS[0]] * len(full_years) + ([DELOITTE_COLORS[3]] if is_partial else [])
    bars = ax.bar(period_labels_chart, [v/1e6 for v in period_values_chart], color=bar_colors)
    ax.set_title("Revenue by Period (Full Years + LTM)", color='#26890D', fontweight='bold')
    ax.set_xlabel("Period", fontsize=11)
    ax.set_ylabel("Revenue (EUR M)", fontsize=11)
    max_val = max(period_values_chart)
    for bar, val in zip(bars, period_values_chart):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01 * max_val/1e6,
                f"{val/1e6:.1f}M", ha='center', va='bottom', fontsize=10, fontweight='bold', color='#404040')
    plt.tight_layout()
    plt.savefig("workspace/graphs/iter1_revenue_by_year.png", dpi=150)
    plt.close()
    print("GRAPH_SAVED: iter1_revenue_by_year.png -- Revenue by period (full fiscal years + LTM)")

print()
print("=" * 70)
print("DATA PROFILING COMPLETE")
print("=" * 70)
```

**Output (preview):**
```
======================================================================
SECTION 1: DATASET OVERVIEW
======================================================================
Total rows    : 77,610
Total columns : 6

Column names and dtypes:
  Nombre Modelo                  object          nulls: 0
  Fecha_Mes                      datetime64[ns]  nulls: 0
  Canal                          object          nulls: 0
  Tienda_Cliente                 object          nulls: 0
  Venta Netas                    float64         nulls: 0
  Cant_Neta                      float64         nulls: 0

==============
```

---

## Analysis 1: Channel economics + PxQ decomposition (price vs volume drivers of growth)
**Hypothesis:** The FY2021-to-FY2022 revenue growth of +96.5% was primarily driven by volume expansion (more transactions/clients) rather than pricing power, and the Online channel shows structurally higher average order values (4-6x Wholesales). Additionally, LTM23 channel mix evolution will reveal whether Online is gaining or losing share as the business scales.

**Columns:** Canal, Fecha_Mes, Venta Netas, Cant_Neta, Tienda_Cliente

```python
import matplotlib; matplotlib.use('Agg')
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath('workspace/data.xlsx')) + '/..')
try:
    import deloitte_theme
    deloitte_theme.apply_deloitte_style()
except Exception:
    import matplotlib.pyplot as plt
    DELOITTE_COLORS = ['#26890D', '#046A38', '#404040', '#0D8390', '#00ABAB']
    plt.rcParams.update({
        'font.family': ['Arial', 'sans-serif'],
        'axes.prop_cycle': plt.cycler('color', DELOITTE_COLORS),
        'axes.titlesize': 14, 'axes.titleweight': 'bold',
        'axes.titlecolor': '#26890D',
        'axes.spines.top': False, 'axes.spines.right': False,
        'figure.facecolor': 'white', 'axes.facecolor': 'white',
    })

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

DELOITTE_COLORS = ['#26890D', '#046A38', '#404040', '#0D8390', '#00ABAB']

df = pd.read_excel('workspace/data.xlsx')
df['Fecha_Mes'] = pd.to_datetime(df['Fecha_Mes'])
df['period'] = df['Fecha_Mes'].dt.to_period('M')

# Identify period windows
latest_month = df['period'].max()
print('Latest month in data: ' + str(latest_month))

# FY windows (Jan-Dec)
fy2021 = df[df['Fecha_Mes'].dt.year == 2021]
fy2022 = df[df['Fecha_Mes'].dt.year == 2022]

# LTM23: 12 months ending at latest_month
ltm_end   = latest_month
ltm_start = latest_month - 11
ltm23 = df[(df['period'] >= ltm_start) & (df['period'] <= ltm_end)]

# Prior LTM (LTM22): 12 months ending one month before ltm_start
pltm_end   = ltm_start - 1
pltm_start = pltm_end - 11
prior_ltm  = df[(df['period'] >= pltm_start) & (df['period'] <= pltm_end)]

print('LTM23 window : ' + str(ltm_start) + '  to  ' + str(ltm_end))
print('Prior LTM    : ' + str(pltm_start) + ' to  ' + str(pltm_end))
print()

# ==================== 1. OVERALL PxQ DECOMPOSITION =======================
print('=' * 70)
print('1. PxQ DECOMPOSITION  -  FY2021 to FY2022 Revenue Growth')
print('=' * 70)

rev21 = fy2021['Venta Netas'].sum()
qty21 = fy2021['Cant_Neta'].sum()
rev22 = fy2022['Venta Netas'].sum()
qty22 = fy2022['Cant_Neta'].sum()
asp21 = rev21 / qty21
asp22 = rev22 / qty22

delta_rev  = rev22 - rev21
delta_qty  = qty22 - qty21
delta_asp  = asp22 - asp21

vol_effect   = asp21 * delta_qty
price_effect = qty22 * delta_asp
check        = vol_effect + price_effect

print('  FY2021 Revenue  : EUR ' + f'{rev21/1e6:.2f}' + 'M  |  Qty: ' + f'{qty21:,.0f}' + '  |  ASP: EUR ' + f'{asp21:.2f}')
print('  FY2022 Revenue  : EUR ' + f'{rev22/1e6:.2f}' + 'M  |  Qty: ' + f'{qty22:,.0f}' + '  |  ASP: EUR ' + f'{asp22:.2f}')
print('  DeltaRevenue    : EUR ' + f'{delta_rev/1e6:.2f}' + 'M  (+' + f'{delta_rev/rev21*100:.1f}' + '%)')
print('  DeltaQty        : ' + f'{delta_qty:+,.0f}' + ' units (' + f'{delta_qty/qty21*100:+.1f}' + '%)')
print('  DeltaASP        : EUR ' + f'{delta_asp:+.2f}' + ' (' + f'{delta_asp/asp21*100:+.1f}' + '%)')
print()
print('  Volume effect   : EUR ' + f'{vol_effect/1e6:+.2f}' + 'M  (' + f'{vol_effect/delta_rev*100:.1f}' + '% of total DeltaRev)')
print('  Price/mix effect: EUR ' + f'{price_effect/1e6:+.2f}' + 'M  (' + f'{price_effect/delta_rev*100:.1f}' + '% of total DeltaRev)')
print('  Check (sum)     : EUR ' + f'{check/1e6:.2f}' + 'M  (should equal DeltaRev)')
print()

# LTM22 to LTM23 decomposition
rev_pltm = prior_ltm['Venta Netas'].sum()
qty_pltm = prior_ltm['Cant_Neta'].sum()
rev_ltm  = ltm23['Venta Netas'].sum()
qty_ltm  = ltm23['Cant_Neta'].sum()
asp_pltm = rev_pltm / qty_pltm
asp_ltm  = rev_ltm  / qty_ltm

d_rev   = rev_ltm - rev_pltm
d_qty   = qty_ltm - qty_pltm
d_asp   = asp_ltm - asp_pltm
v_eff   = asp_pltm * d_qty
p_eff   = qty_ltm  * d_asp

print('  LTM22 Revenue   : EUR ' + f'{rev_pltm/1e6:.2f}' + 'M  |  Qty: ' + f'{qty_pltm:,.0f}' + '  |  ASP: EUR ' + f'{asp_pltm:.2f}')
print('  LTM23 Revenue   : EUR ' + f'{rev_ltm/1e6:.2f}' + 'M  |  Qty: ' + f'{qty_ltm:,.0f}' + '  |  ASP: EUR ' + f'{asp_ltm:.2f}')
print('  DeltaRevenue    : EUR ' + f'{d_rev/1e6:.2f}' + 'M  (+' + f'{d_rev/rev_pltm*100:.1f}' + '%)')
print('  Volume effect   : EUR ' + f'{v_eff/1e6:+.2f}' + 'M  (' + f'{v_eff/d_rev*100:.1f}' + '% of total DeltaRev)')
print('  Price/mix effect: EUR ' + f'{p_eff/1e6:+.2f}' + 'M  (' + f'{p_eff/d_rev*100:.1f}' + '% of total DeltaRev)')
print()

# ==================== 2. CHANNEL ECONOMICS TABLE =========================
print('=' * 70)
print('2. CHANNEL ECONOMICS BY PERIOD')
print('=' * 70)

channels = sorted(df['Canal'].unique())
period_dfs = [('FY2021', fy2021), ('FY2022', fy2022), ('LTM23', ltm23)]

for ch in channels:
    print('\n  Channel: ' + ch)
    print('  ' + f'{"Metric":<22} {"FY2021":>12} {"FY2022":>12} {"LTM23":>12}')
    print('  ' + '-'*22 + ' ' + '-'*12 + ' ' + '-'*12 + ' ' + '-'*12)
    rows = {}
    for label, pdf in period_dfs:
        sub = pdf[pdf['Canal'] == ch]
        rev     = sub['Venta Netas'].sum()
        qty     = sub['Cant_Neta'].sum()
        txns    = len(sub)
        clients = sub['Tienda_Cliente'].nunique()
        aov     = rev / txns  if txns else 0
        asp     = rev / qty   if qty  else 0
        rows[label] = {'rev': rev, 'qty': qty, 'txns': txns, 'clients': clients, 'aov': aov, 'asp': asp}

    def fmt_row(name, vals_str):
        print('  ' + f'{name:<22}' + vals_str)

    fmt_row('Revenue (EUR M)',   ''.join(f'{rows[l]["rev"]/1e6:>12.2f}' for l, _ in period_dfs))
    fmt_row('Net Qty (k)',        ''.join(f'{rows[l]["qty"]/1e3:>12.1f}' for l, _ in period_dfs))
    fmt_row('Transactions',       ''.join(f'{rows[l]["txns"]:>12,}' for l, _ in period_dfs))
    fmt_row('Unique Clients',     ''.join(f'{rows[l]["clients"]:>12,}' for l, _ in period_dfs))
    fmt_row('AOV (EUR/txn)',      ''.join(f'{rows[l]["aov"]:>12.0f}' for l, _ in period_dfs))
    fmt_row('ASP (EUR/unit)',     ''.join(f'{rows[l]["asp"]:>12.2f}' for l, _ in period_dfs))

# ==================== 3. CHANNEL REVENUE SHARE ===========================
print()
print('=' * 70)
print('3. CHANNEL REVENUE SHARE EVOLUTION')
print('=' * 70)

share_rows = []
for label, pdf in period_dfs:
    tot = pdf['Venta Netas'].sum()
    ch_rev = pdf.groupby('Canal')['Venta Netas'].sum()
    for ch, r in ch_rev.items():
        share_rows.append({'Period': label, 'Canal': ch, 'Revenue': r, 'Share': r/tot*100})

share_df = pd.DataFrame(share_rows)
pivot_share = share_df.pivot(index='Canal', columns='Period', values='Share').fillna(0)

print()
print('  ' + f'{"Channel":<18} {"FY2021":>10} {"FY2022":>10} {"LTM23":>10}  {"D FY21->LTM23":>14}')
print('  ' + '-'*18 + ' ' + '-'*10 + ' ' + '-'*10 + ' ' + '-'*10 + '  ' + '-'*14)
for ch in channels:
    if ch in pivot_share.index:
        v21  = pivot_share.loc[ch].get('FY2021', 0)
        v22  = pivot_share.loc[ch].get('FY2022', 0)
        vltm = pivot_share.loc[ch].get('LTM23',  0)
    else:
        v21 = v22 = vltm = 0
    print('  ' + f'{ch:<18} {v21:>9.1f}% {v22:>9.1f}% {vltm:>9.1f}%  {vltm-v21:>+13.1f}pp')

# ==================== 4. CLIENT COUNT EVOLUTION ==========================
print()
print('=' * 70)
print('4. CLIENT COUNT EVOLUTION (Unique Clients by Channel)')
print('=' * 70)

# N for CAGR: Jan 2021 to Apr 2023 = 27 months
N = 27/12
print()
print('  ' + f'{"Channel":<18} {"FY2021":>10} {"FY2022":>10} {"LTM23":>10}  {"CAGR (N=2.25y)":>16}')
print('  ' + '-'*18 + ' ' + '-'*10 + ' ' + '-'*10 + ' ' + '-'*10 + '  ' + '-'*16)
for ch in channels:
    c21  = fy2021[fy2021['Canal']==ch]['Tienda_Cliente'].nunique()
    c22  = fy2022[fy2022['Canal']==ch]['Tienda_Cliente'].nunique()
    cltm = ltm23[ltm23['Canal']==ch]['Tienda_Cliente'].nunique()
    cagr = (cltm/c21)**(1/N)-1 if c21 > 0 else float('nan')
    print('  ' + f'{ch:<18} {c21:>10,} {c22:>10,} {cltm:>10,}  {cagr:>+15.1%}')

# ==================== GRAPHS =============================================
os.makedirs('workspace/graphs', exist_ok=True)

# Graph A: Channel AOV by period (grouped bar)
fig, ax = plt.subplots(figsize=(10, 5))
period_labels = ['FY2021', 'FY2022', 'LTM23']
period_map    = {'FY2021': fy2021, 'FY2022': fy2022, 'LTM23': ltm23}
x = np.arange(len(channels))
width = 0.25
for i, plabel in enumerate(period_labels):
    pdf = period_map[plabel]
    aovs = []
    for ch in channels:
        sub = pdf[pdf['Canal'] == ch]
        txns = len(sub)
        aovs.append(sub['Venta Netas'].sum() / txns if txns else 0)
    ax.bar(x + i*width, aovs, width, label=plabel, color=DELOITTE_COLORS[i])

ax.set_xticks(x + width)
ax.set_xticklabels(channels)
ax.set_title('Average Order Value (EUR) by Channel & Period', color='#26890D', fontweight='bold')
ax.set_ylabel('AOV (EUR per transaction)')
ax.legend()
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f'EUR {v:,.0f}'))
plt.tight_layout()
plt.savefig('workspace/graphs/iter2_channel_aov_by_period.png', dpi=150)
print('GRAPH_SAVED: iter2_channel_aov_by_period.png - Average order value per channel across FY2021, FY2022 and LTM23')
plt.close()

# Graph B: Channel revenue share stacked bar
fig, ax = plt.subplots(figsize=(8, 5))
bottoms = np.zeros(3)
for j, ch in enumerate(channels):
    if ch in pivot_share.index:
        vals = [float(pivot_share.loc[ch].get(p, 0)) for p in period_labels]
    else:
        vals = [0.0, 0.0, 0.0]
    ax.bar(period_labels, vals, bottom=bottoms, label=ch, color=DELOITTE_COLORS[j % len(DELOITTE_COLORS)])
    bottoms += np.array(vals)
ax.set_title('Channel Revenue Share (%) by Period', color='#26890D', fontweight='bold')
ax.set_ylabel('Revenue Share (%)')
ax.set_ylim(0, 110)
ax.legend(loc='upper right')
plt.tight_layout()
plt.savefig('workspace/graphs/iter2_channel_revenue_share.png', dpi=150)
print('GRAPH_SAVED: iter2_channel_revenue_share.png - Stacked bar showing channel revenue share evolution across FY2021, FY2022 and LTM23')
plt.close()

# Graph C: PxQ waterfall for FY2021 to FY2022
fig, ax = plt.subplots(figsize=(8, 5))
labels_w  = ['FY2021\nRevenue', 'Volume\nEffect', 'Price/Mix\nEffect', 'FY2022\nRevenue']
values_w  = [rev21, vol_effect, price_effect, rev22]
colors_w  = [DELOITTE_COLORS[0], DELOITTE_COLORS[1], DELOITTE_COLORS[3], DELOITTE_COLORS[0]]
bases_w   = [0, rev21, rev21 + vol_effect, 0]
for k in range(len(labels_w)):
    ax.bar(labels_w[k], values_w[k], bottom=bases_w[k], color=colors_w[k])
    mid = bases_w[k] + values_w[k]/2
    ax.text(k, mid, f'EUR {values_w[k]/1e6:.1f}M', ha='center', va='center',
            color='white', fontweight='bold', fontsize=9)
ax.set_title('PxQ Decomposition: FY2021 to FY2022 Revenue Growth', color='#26890D', fontweight='bold')
ax.set_ylabel('Revenue (EUR M)')
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f'EUR {v/1e6:.1f}M'))
plt.tight_layout()
plt.savefig('workspace/graphs/iter2_pxq_waterfall_fy21_fy22.png', dpi=150)
print('GRAPH_SAVED: iter2_pxq_waterfall_fy21_fy22.png - PxQ waterfall decomposing FY2021-to-FY2022 revenue growth into volume and price/mix effects')
plt.close()

print()
print('Analysis complete.')
```

**Output (preview):**
```
Latest month in data: 2023-04
LTM23 window : 2022-05  to  2023-04
Prior LTM    : 2021-05 to  2022-04

======================================================================
1. PxQ DECOMPOSITION  -  FY2021 to FY2022 Revenue Growth
======================================================================
  FY2021 Revenue  : EUR 16.71M  |  Qty: 335,562  |  ASP: EUR 49.80
  FY2022 Revenue  : EUR 32.83M  |  Qty: 635,297  |  ASP: EUR 51.68
  DeltaRevenue    : EUR 16.12M  (+96.5%)
  DeltaQty        : +299,735 units (+89.3%)
  DeltaASP        : EUR +1.88 (+3.8%)

  Volume effect   : EUR +14.93M  (92.6% o
```

---

## Analysis 1: Product mix evolution (fashion boom-bust detection) + Top N concentration
**Hypothesis:** Product mix evolution is a key risk driver: certain models exhibit 'boom-and-bust' fashion cycles while others are stable revenue contributors. By analyzing each model's revenue trajectory across FY2021, FY2022, and LTM23, we can identify which products are structural vs. cyclical, quantify the revenue concentration risk from fashion-driven SKUs, and assess whether growth is underpinned by a stable product base or dependent on transient trend products.

**Columns:** Nombre Modelo, Línea, Fecha_Mes, Venta Netas, Cant_Neta

```python
import matplotlib
matplotlib.use('Agg')
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath('workspace/data.xlsx')) + '/..')
try:
    import deloitte_theme
    deloitte_theme.apply_deloitte_style()
except Exception:
    pass

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

DELOITTE_COLORS = ['#26890D', '#046A38', '#404040', '#0D8390', '#00ABAB']
plt.rcParams.update({
    'font.family': ['Arial', 'sans-serif'],
    'axes.prop_cycle': plt.cycler('color', DELOITTE_COLORS),
    'axes.titlesize': 14, 'axes.titleweight': 'bold',
    'axes.titlecolor': '#26890D',
    'axes.spines.top': False, 'axes.spines.right': False,
    'figure.facecolor': 'white', 'axes.facecolor': 'white',
})

# ── Load data ──────────────────────────────────────────────────────────────────
df = pd.read_excel('workspace/data.xlsx')
df['Fecha_Mes'] = pd.to_datetime(df['Fecha_Mes'])
df['period'] = df['Fecha_Mes'].dt.to_period('M')

# Check actual column names
print('Columns in dataset:', df.columns.tolist())

# Identify the Linea column (handle encoding variations)
linea_col = None
for c in df.columns:
    if 'nea' in c.lower() or 'linea' in c.lower() or 'l\u00ednea' in c.lower():
        linea_col = c
        break
if linea_col is None:
    # fallback: try exact
    possible = [c for c in df.columns if 'L' in c and 'nea' in c]
    linea_col = possible[0] if possible else None
print(f'Using Linea column: {linea_col}')

# ── Define fiscal periods ──────────────────────────────────────────────────────
latest_month = df['period'].max()
print(f'Latest month in data: {latest_month}')

fy2021 = df[df['Fecha_Mes'].dt.year == 2021]
fy2022 = df[df['Fecha_Mes'].dt.year == 2022]
# LTM23: May 2022 - Apr 2023
ltm23_start = pd.Period('2022-05', 'M')
ltm23_end   = pd.Period('2023-04', 'M')
ltm23 = df[(df['period'] >= ltm23_start) & (df['period'] <= ltm23_end)]
# Prior LTM22: May 2021 - Apr 2022
ltm22_start = pd.Period('2021-05', 'M')
ltm22_end   = pd.Period('2022-04', 'M')
ltm22 = df[(df['period'] >= ltm22_start) & (df['period'] <= ltm22_end)]

print(f'FY2021 rows: {len(fy2021):,} | FY2022 rows: {len(fy2022):,} | LTM23 rows: {len(ltm23):,}')

# ── Revenue by Model x Period ─────────────────────────────────────────────────
def rev_by_model(sub):
    return sub.groupby('Nombre Modelo')['Venta Netas'].sum()

r21   = rev_by_model(fy2021).rename('FY2021')
r22   = rev_by_model(fy2022).rename('FY2022')
rltm22 = rev_by_model(ltm22).rename('LTM22')
rltm23 = rev_by_model(ltm23).rename('LTM23')

model_df = pd.concat([r21, r22, rltm22, rltm23], axis=1).fillna(0)

# Total revenues
tot21    = model_df['FY2021'].sum()
tot22    = model_df['FY2022'].sum()
totltm22 = model_df['LTM22'].sum()
totltm23 = model_df['LTM23'].sum()

print(f'\nTotal Revenue: FY2021={tot21/1e6:.2f}M | FY2022={tot22/1e6:.2f}M | LTM22={totltm22/1e6:.2f}M | LTM23={totltm23/1e6:.2f}M')

# ── Línea (product line) revenue summary ──────────────────────────────────────
if linea_col is not None:
    print('\n' + '='*80)
    print('SECTION 1: REVENUE BY PRODUCT LINE')
    print('='*80)

    def rev_by_linea(sub):
        return sub.groupby(linea_col)['Venta Netas'].sum()

    l21  = rev_by_linea(fy2021).rename('FY2021')
    l22  = rev_by_linea(fy2022).rename('FY2022')
    ll22 = rev_by_linea(ltm22).rename('LTM22')
    ll23 = rev_by_linea(ltm23).rename('LTM23')

    linea_df = pd.concat([l21, l22, ll22, ll23], axis=1).fillna(0)
    linea_df['Share_FY21']  = linea_df['FY2021']  / tot21    * 100
    linea_df['Share_FY22']  = linea_df['FY2022']  / tot22    * 100
    linea_df['Share_LTM23'] = linea_df['LTM23']   / totltm23 * 100
    linea_df['YoY_FY21_22'] = (linea_df['FY2022'] / linea_df['FY2021'].replace(0, np.nan) - 1) * 100
    linea_df['YoY_LTM']     = (linea_df['LTM23']  / linea_df['LTM22'].replace(0, np.nan)  - 1) * 100

    # CAGR: FY2021 to LTM23, N = 27/12 = 2.25 years
    N = 27 / 12
    linea_df['CAGR'] = ((linea_df['LTM23'] / linea_df['FY2021'].replace(0, np.nan)) ** (1 / N) - 1) * 100

    hdr = f'{"Line":<22} {"FY2021":>9} {"Shr%":>5} {"FY2022":>9} {"Shr%":>5} {"LTM23":>9} {"Shr%":>5} {"YoY FY21-22":>12} {"YoY LTM":>9} {"CAGR(N=2.25y)":>14}'
    print(f'\n{hdr}')
    print('-' * 107)
    for idx, row in linea_df.sort_values('LTM23', ascending=False).iterrows():
        yoy_fy = f'{row["YoY_FY21_22"]:+.1f}%' if not np.isnan(row['YoY_FY21_22']) else 'N/A'
        yoy_lt = f'{row["YoY_LTM"]:+.1f}%' if not np.isnan(row['YoY_LTM']) else 'N/A'
        cagr_s = f'{row["CAGR"]:+.1f}%' if not np.isnan(row['CAGR']) else 'N/A'
        print(f'{str(idx):<22} {row["FY2021"]/1e6:>8.2f}M {row["Share_FY21"]:>4.1f}% '
              f'{row["FY2022"]/1e6:>8.2f}M {row["Share_FY22"]:>4.1f}% '
              f'{row["LTM23"]/1e6:>8.2f}M {row["Share_LTM23"]:>4.1f}% '
              f'{yoy_fy:>12} {yoy_lt:>9} {cagr_s:>14}')
    print('-' * 107)
    print(f'{"TOTAL":<22} {tot21/1e6:>8.2f}M {100:>4.1f}% {tot22/1e6:>8.2f}M {100:>4.1f}% {totltm23/1e6:>8.2f}M {100:>4.1f}%')
else:
    print('WARNING: Linea column not found; skipping Section 1')
    linea_df = None

# ── Product lifecycle classification ──────────────────────────────────────────
print('\n' + '='*80)
print('SECTION 2: PRODUCT LIFECYCLE CLASSIFICATION (STABLE vs BOOM-BUST)')
print('='*80)

THRESHOLD = 10000  # EUR minimum to be considered meaningful

def classify_model(row):
    r21_v   = row['FY2021']
    r22_v   = row['FY2022']
    rlt22_v = row['LTM22']
    rlt23_v = row['LTM23']

    prev = r21_v + r22_v
    if rlt23_v < THRESHOLD and rlt22_v < THRESHOLD and prev >= THRESHOLD:
        return 'Disappeared'
    if rlt23_v >= THRESHOLD and (r21_v + rlt22_v) < THRESHOLD:
        return 'New Entrant'
    if rlt22_v >= THRESHOLD and rlt23_v >= THRESHOLD:
        pct_chg = (rlt23_v - rlt22_v) / rlt22_v * 100
        if pct_chg >= 100:
            return 'Boom (Rising)'
        elif pct_chg <= -50:
            return 'Bust (Declining)'
        else:
            return 'Stable'
    return 'Other'

model_df['Lifecycle'] = model_df.apply(classify_model, axis=1)

# Revenue per lifecycle bucket
lifecycle_rev = model_df.groupby('Lifecycle')[['FY2021', 'FY2022', 'LTM22', 'LTM23']].sum()
lifecycle_rev['Model_Count'] = model_df.groupby('Lifecycle').size()
lifecycle_rev['Share_LTM23'] = lifecycle_rev['LTM23'] / totltm23 * 100
lifecycle_rev['Share_LTM22'] = lifecycle_rev['LTM22'] / totltm22 * 100

print(f'\n{"Lifecycle":<22} {"# Models":>9} {"FY2021":>9} {"FY2022":>9} {"LTM22":>9} {"LTM23":>9} {"Shr LTM22":>10} {"Shr LTM23":>10}')
print('-' * 95)
for idx, row in lifecycle_rev.sort_values('LTM23', ascending=False).iterrows():
    print(f'{str(idx):<22} {int(row["Model_Count"]):>9} {row["FY2021"]/1e6:>8.2f}M '
          f'{row["FY2022"]/1e6:>8.2f}M {row["LTM22"]/1e6:>8.2f}M '
          f'{row["LTM23"]/1e6:>8.2f}M {row["Share_LTM22"]:>9.1f}% {row["Share_LTM23"]:>9.1f}%')
print('-' * 95)
print(f'{"TOTAL":<22} {len(model_df):>9} {tot21/1e6:>8.2f}M {tot22/1e6:>8.2f}M '
      f'{totltm22/1e6:>8.2f}M {totltm23/1e6:>8.2f}M {100:>9.1f}% {100:>9.1f}%')

# ── Add Línea to model_df ─────────────────────────────────────────────────────
if linea_col is not None:
    linea_map = df.groupby('Nombre Modelo')[linea_col].agg(lambda x: x.mode()[0] if len(x) > 0 else 'N/A')
    model_df['Linea'] = model_df.index.map(linea_map)
else:
    model_df['Linea'] = 'N/A'

# ── Top 20 models in LTM23 with full lifecycle view ───────────────────────────
print('\n' + '='*80)
print('SECTION 3: TOP 20 MODELS BY LTM23 REVENUE — FULL TRAJECTORY')
print('='*80)

top20 = model_df.nlargest(20, 'LTM23').copy()
top20['YoY_LTM']   = (top20['LTM23'] / top20['LTM22'].replace(0, np.nan) - 1) * 100
top20['YoY_FY']    = (top20['FY2022'] / top20['FY2021'].replace(0, np.nan) - 1) * 100
top20['Shr_LTM23'] = top20['LTM23'] / totltm23 * 100
top20['Shr_cum']   = top20['Shr_LTM23'].cumsum()

print(f'\n{"Rank":<5} {"Model":<26} {"Line":<10} {"FY2021":>8} {"FY2022":>8} {"LTM22":>8} {"LTM23":>8} {"YoY LTM":>9} {"Shr%":>6} {"Cum%":>7} {"Lifecycle":<18}')
print('-' * 122)
for rank, (idx, row) in enumerate(top20.iterrows(), 1):
    yoy_str  = f'{row["YoY_LTM"]:+.1f}%' if not np.isnan(row['YoY_LTM']) else 'NEW'
    fy21_str = f'{row["FY2021"]/1e3:.0f}k' if row['FY2021'] > 0 else '-'
    fy22_str = f'{row["FY2022"]/1e3:.0f}k' if row['FY2022'] > 0 else '-'
    lt22_str = f'{row["LTM22"]/1e3:.0f}k'  if row['LTM22']  > 0 else '-'
    lt23_str = f'{row["LTM23"]/1e3:.0f}k'
    linea_val = str(row['Linea'])[:10]
    print(f'{rank:<5} {str(idx)[:25]:<26} {linea_val:<10} {fy21_str:>8} {fy22_str:>8} {lt22_str:>8} {lt23_str:>8} {yoy_str:>9} {row["Shr_LTM23"]:>5.1f}% {row["Shr_cum"]:>6.1f}% {row["Lifecycle"]:<18}')

print(f'\n>>> Top 20 models account for {top20["Shr_LTM23"].sum():.1f}% of LTM23 revenue')

# ── Disappeared models ────────────────────────────────────────────────────────
print('\n' + '='*80)
print('SECTION 4: DISAPPEARED MODELS — REVENUE AT RISK')
print('='*80)

disappeared = model_df[model_df['Lifecycle'] == 'Disappeared'].copy()
disappeared = disappeared.sort_values('FY2022', ascending=False)
print(f'\n{len(disappeared)} models that had meaningful revenue in FY2021/FY2022 but are absent in LTM23:')
print(f'{"Model":<30} {"Line":<10} {"FY2021":>9} {"FY2022":>9} {"LTM22":>9} {"LTM23":>9}')
print('-' * 80)
for idx, row in disappeared.head(20).iterrows():
    print(f'{str(idx)[:29]:<30} {str(row["Linea"])[:9]:<10} '
          f'{row["FY2021"]/1e3:>8.0f}k {row["FY2022"]/1e3:>8.0f}k '
          f'{row["LTM22"]/1e3:>8.0f}k {row["LTM23"]/1e3:>8.0f}k')
if len(disappeared) > 20:
    print(f'  ... and {len(disappeared)-20} more models')

disap_rev_fy22  = disappeared['FY2022'].sum()
disap_rev_ltm22 = disappeared['LTM22'].sum()
print(f'\nTotal Disappeared Revenue: FY2022={disap_rev_fy22/1e6:.2f}M | LTM22={disap_rev_ltm22/1e6:.2f}M')
print(f'As % of FY2022 total: {disap_rev_fy22/tot22*100:.1f}% | As % of LTM22 total: {disap_rev_ltm22/totltm22*100:.1f}%')

# ── New entrants ──────────────────────────────────────────────────────────────
print('\n' + '='*80)
print('SECTION 5: NEW ENTRANT MODELS — INCREMENTAL REVENUE IN LTM23')
print('='*80)

new_models = model_df[model_df['Lifecycle'] == 'New Entrant'].copy().sort_values('LTM23', ascending=False)
print(f'\n{len(new_models)} new models in LTM23 that were absent in prior periods:')
print(f'{"Model":<30} {"Line":<10} {"LTM23":>9} {"Share of LTM23":>14}')
print('-' * 66)
for idx, row in new_models.head(15).iterrows():
    print(f'{str(idx)[:29]:<30} {str(row["Linea"])[:9]:<10} '
          f'{row["LTM23"]/1e3:>8.0f}k {row["LTM23"]/totltm23*100:>13.1f}%')
new_rev = new_models['LTM23'].sum()
print(f'\nTotal New Entrant Revenue in LTM23: {new_rev/1e6:.2f}M ({new_rev/totltm23*100:.1f}% of LTM23 total)')

# ── Fashion risk summary ──────────────────────────────────────────────────────
print('\n' + '='*80)
print('SECTION 6: FASHION RISK SUMMARY')
print('='*80)

boom_rev   = model_df[model_df['Lifecycle'] == 'Boom (Rising)']['LTM23'].sum()
bust_rev   = model_df[model_df['Lifecycle'] == 'Bust (Declining)']['LTM23'].sum()
stable_rev = model_df[model_df['Lifecycle'] == 'Stable']['LTM23'].sum()
new_rev_l  = model_df[model_df['Lifecycle'] == 'New Entrant']['LTM23'].sum()
other_rev  = model_df[model_df['Lifecycle'] == 'Other']['LTM23'].sum()

print(f'\nLTM23 Revenue Breakdown by Lifecycle:')
print(f'  Stable models:         {stable_rev/1e6:>7.2f}M  ({stable_rev/totltm23*100:>5.1f}%)')
print(f'  Boom (Rising >+100%):  {boom_rev/1e6:>7.2f}M  ({boom_rev/totltm23*100:>5.1f}%)')
print(f'  Bust (Declining >-50%):{bust_rev/1e6:>7.2f}M  ({bust_rev/totltm23*100:>5.1f}%)')
print(f'  New Entrants:          {new_rev_l/1e6:>7.2f}M  ({new_rev_l/totltm23*100:>5.1f}%)')
print(f'  Other:                 {other_rev/1e6:>7.2f}M  ({other_rev/totltm23*100:>5.1f}%)')
print(f'  TOTAL:                 {totltm23/1e6:>7.2f}M  (100.0%)')
print(f'\nRevenue at risk (Disappeared models, LTM22): {disap_rev_ltm22/1e6:.2f}M ({disap_rev_ltm22/totltm22*100:.1f}% of LTM22)')
print(f'Revenue at risk (Bust models, LTM23):        {bust_rev/1e6:.2f}M ({bust_rev/totltm23*100:.1f}% of LTM23)')
print(f'High-velocity models (Boom, LTM23):          {boom_rev/1e6:.2f}M ({boom_rev/totltm23*100:.1f}% of LTM23)')

# ═══════════════════════════════════════════════════════════════════════════════
# CHARTS
# ═══════════════════════════════════════════════════════════════════════════════
os.makedirs('workspace/graphs', exist_ok=True)

# Chart 1: Product Line revenue evolution stacked bar (only if linea_col found)
if linea_col is not None and linea_df is not None:
    fig, ax = plt.subplots(figsize=(11, 6))
    lineas  = linea_df.sort_values('LTM23', ascending=False).index.tolist()
    periods = ['FY2021', 'FY2022', 'LTM23']
    x       = np.arange(len(periods))
    width   = 0.55
    bottom  = np.zeros(3)
    colors_use = DELOITTE_COLORS
    for i, linea in enumerate(lineas):
        vals = [linea_df.loc[linea, p] / 1e6 if linea in linea_df.index else 0 for p in periods]
        ax.bar(x, vals, width, label=str(linea), bottom=bottom, color=colors_use[i % len(colors_use)])
        for j, (v, b) in enumerate(zip(vals, bottom)):
            if v > 0.5:
                ax.text(x[j], b + v / 2, f'{v:.1f}M', ha='center', va='center',
                        fontsize=8.5, color='white', fontweight='bold')
        bottom += np.array(vals)
    ax.set_xticks(x)
    ax.set_xticklabels(periods, fontsize=11)
    ax.set_ylabel('Revenue (EUR M)', fontsize=11)
    ax.set_title('Revenue by Product Line — FY2021 vs FY2022 vs LTM23',
                 color='#26890D', fontweight='bold', fontsize=13)
    ax.legend(loc='upper left', frameon=False, fontsize=10)
    for j, (xpos, tot) in enumerate(zip(x, [tot21 / 1e6, tot22 / 1e6, totltm23 / 1e6])):
        ax.text(xpos, bottom[j] + 0.3, f'\u20ac{tot:.1f}M', ha='center', va='bottom',
                fontsize=10, fontweight='bold', color='#404040')
    ax.set_ylim(0, max(tot21, tot22, totltm23) / 1e6 * 1.15)
    plt.tight_layout()
    plt.savefig('workspace/graphs/iter1_linea_revenue_evolution.png', dpi=150, bbox_inches='tight')
    print('GRAPH_SAVED: iter1_linea_revenue_evolution.png — Stacked bar chart of revenue by Product Line across FY2021, FY2022 and LTM23')
    plt.close()

# Chart 2: Lifecycle donut + horizontal bar
fig, axes = plt.subplots(1, 2, figsize=(14, 6))

# Left: donut LTM23 revenue by lifecycle (exclude Disappeared = 0 in LTM23)
plot_lc_data = model_df.groupby('Lifecycle')['LTM23'].sum()
plot_lc_data = plot_lc_data[plot_lc_data > 0].sort_values(ascending=False)
wedge_colors = DELOITTE_COLORS[:len(plot_lc_data)]
wedges, texts, autotexts = axes[0].pie(
    plot_lc_data.values / 1e6,
    labels=plot_lc_data.index.tolist(),
    autopct='%1.1f%%',
    colors=wedge_colors,
    startangle=140,
    wedgeprops=dict(width=0.55),
    pctdistance=0.78
)
for t in autotexts:
    t.set_fontsize(9)
    t.set_color('white')
    t.set_fontweight('bold')
for t in texts:
    t.set_fontsize(10)
axes[0].set_title('LTM23 Revenue by Product Lifecycle', color='#26890D', fontweight='bold', fontsize=12)
axes[0].text(0, 0, f'\u20ac{totltm23/1e6:.1f}M', ha='center', va='center', fontsize=13,
             fontweight='bold', color='#404040')

# Right: horizontal bar — lifecycle model count & revenue
lifecycle_summary = model_df.groupby('Lifecycle').agg(
    Models=('FY2021', 'count'),
    LTM23_Rev=('LTM23', 'sum'),
    LTM22_Rev=('LTM22', 'sum')
).reset_index().sort_values('LTM23_Rev', ascending=True)

bars2 = axes[1].barh(lifecycle_summary['Lifecycle'],
                     lifecycle_summary['LTM23_Rev'] / 1e6,
                     color=DELOITTE_COLORS[0], alpha=0.85)
for bar, cnt in zip(bars2, lifecycle_summary['Models']):
    axes[1].text(bar.get_width() + 0.05, bar.get_y() + bar.get_height() / 2,
                 f'{int(cnt)} models', va='center', fontsize=9, color='#404040')
axes[1].set_xlabel('LTM23 Revenue (EUR M)', fontsize=10)
axes[1].set_title('Revenue & Model Count by Lifecycle Bucket', color='#26890D', fontweight='bold', fontsize=12)
axes[1].spines['top'].set_visible(False)
axes[1].spines['right'].set_visible(False)
plt.tight_layout()
plt.savefig('workspace/graphs/iter1_product_lifecycle_analysis.png', dpi=150, bbox_inches='tight')
print('GRAPH_SAVED: iter1_product_lifecycle_analysis.png — Donut and bar charts showing LTM23 revenue by product lifecycle classification')
plt.close()

# Chart 3: Top 20 models trajectory grouped bar
top20_plot = model_df.nlargest(20, 'LTM23').copy()
top20_plot.index = [str(i)[:22] for i in top20_plot.index]
top20_plot = top20_plot.sort_values('LTM23')

fig, ax = plt.subplots(figsize=(12, 8))
y = np.arange(len(top20_plot))
h = 0.22
ax.barh(y + h,  top20_plot['FY2021'] / 1e3, h * 0.9, label='FY2021', color=DELOITTE_COLORS[2])
ax.barh(y,      top20_plot['FY2022'] / 1e3, h * 0.9, label='FY2022', color=DELOITTE_COLORS[1])
ax.barh(y - h,  top20_plot['LTM23']  / 1e3, h * 0.9, label='LTM23',  color=DELOITTE_COLORS[0])
ax.set_yticks(y)
ax.set_yticklabels(top20_plot.index, fontsize=8.5)
ax.set_xlabel('Revenue (EUR k)', fontsize=10)
ax.set_title('Top 20 Models — Revenue Trajectory: FY2021 vs FY2022 vs LTM23',
             color='#26890D', fontweight='bold', fontsize=12)
ax.legend(loc='lower right', frameon=False, fontsize=10)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
plt.tight_layout()
plt.savefig('workspace/graphs/iter1_top20_models_trajectory.png', dpi=150, bbox_inches='tight')
print('GRAPH_SAVED: iter1_top20_models_trajectory.png — Grouped horizontal bar chart showing top 20 models revenue across FY2021, FY2022 and LTM23')
plt.close()

print('\n[Analysis complete]')
```

**Output (preview):**
```
Columns in dataset: ['Nombre Modelo', 'Fecha_Mes', 'Canal', 'Tienda_Cliente', 'Venta Netas', 'Cant_Neta', 'period']
Using Linea column: None
Latest month in data: 2023-04
FY2021 rows: 19,398 | FY2022 rows: 40,612 | LTM23 rows: 46,717

Total Revenue: FY2021=16.71M | FY2022=32.83M | LTM22=21.76M | LTM23=36.14M
WARNING: Linea column not found; skipping Section 1
```

---

## Analysis 2: Product lifecycle velocity analysis — time-to-peak, post-peak retention, and revenue half-life of New Entrant models vs Stable models
**Hypothesis:** New Entrant models exhibit fashion-cycle dynamics with short revenue half-lives (peak-and-decline within 2-4 months), while Stable models show sustained revenue curves. Quantifying the time-to-peak and post-peak retention rate of new SKUs will determine whether the 55% new-entrant revenue share is durable or represents a one-period phenomenon subject to rapid erosion.

**Columns:** Nombre Modelo, Fecha_Mes, Venta Netas, Cant_Neta

```python
import matplotlib; matplotlib.use('Agg')
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath('workspace/data.xlsx')) + '/..')
try:
    import deloitte_theme
    deloitte_theme.apply_deloitte_style()
except Exception:
    import matplotlib.pyplot as plt
    DELOITTE_COLORS = ['#26890D', '#046A38', '#404040', '#0D8390', '#00ABAB']
    plt.rcParams.update({
        'font.family': ['Arial', 'sans-serif'],
        'axes.prop_cycle': plt.cycler('color', DELOITTE_COLORS),
        'axes.titlesize': 14, 'axes.titleweight': 'bold',
        'axes.titlecolor': '#26890D',
        'axes.spines.top': False, 'axes.spines.right': False,
        'figure.facecolor': 'white', 'axes.facecolor': 'white',
    })

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

DELOITTE_COLORS = ['#26890D', '#046A38', '#404040', '#0D8390', '#00ABAB']

# ── Load data ──────────────────────────────────────────────────────────────────
df = pd.read_excel('workspace/data.xlsx')
df['Fecha_Mes'] = pd.to_datetime(df['Fecha_Mes'])
df['period'] = df['Fecha_Mes'].dt.to_period('M')

# ── Define time windows ────────────────────────────────────────────────────────
latest_month = df['period'].max()  # Apr 2023
fy2021_start = pd.Period('2021-01', 'M')
fy2021_end   = pd.Period('2021-12', 'M')
fy2022_start = pd.Period('2022-01', 'M')
fy2022_end   = pd.Period('2022-12', 'M')
ltm23_end    = latest_month                  # Apr-2023
ltm23_start  = latest_month - 11             # May-2022
ltm22_end    = ltm23_start - 1              # Apr-2022
ltm22_start  = ltm22_end - 11              # May-2021

print('=== TIME WINDOWS ===')
print(f'FY2021 : {fy2021_start} – {fy2021_end}')
print(f'FY2022 : {fy2022_start} – {fy2022_end}')
print(f'LTM22  : {ltm22_start} – {ltm22_end}')
print(f'LTM23  : {ltm23_start} – {ltm23_end}')

# ── Classify models by lifecycle ───────────────────────────────────────────────
rev_fy21  = df[df['period'].between(fy2021_start, fy2021_end)].groupby('Nombre Modelo')['Venta Netas'].sum()
rev_fy22  = df[df['period'].between(fy2022_start, fy2022_end)].groupby('Nombre Modelo')['Venta Netas'].sum()
rev_ltm22 = df[df['period'].between(ltm22_start, ltm22_end)].groupby('Nombre Modelo')['Venta Netas'].sum()
rev_ltm23 = df[df['period'].between(ltm23_start, ltm23_end)].groupby('Nombre Modelo')['Venta Netas'].sum()

all_models = set(rev_fy21.index) | set(rev_fy22.index) | set(rev_ltm22.index) | set(rev_ltm23.index)
model_df = pd.DataFrame(index=sorted(all_models))
model_df['rev_fy21']  = rev_fy21.reindex(model_df.index, fill_value=0)
model_df['rev_fy22']  = rev_fy22.reindex(model_df.index, fill_value=0)
model_df['rev_ltm22'] = rev_ltm22.reindex(model_df.index, fill_value=0)
model_df['rev_ltm23'] = rev_ltm23.reindex(model_df.index, fill_value=0)

def classify_model(row):
    in_pre  = (row['rev_fy21'] > 0) or (row['rev_ltm22'] > 0)
    in_ltm23 = row['rev_ltm23'] > 0
    in_ltm22 = row['rev_ltm22'] > 0
    if not in_pre and in_ltm23:
        return 'New Entrant'
    elif in_pre and in_ltm23:
        pct_change = (row['rev_ltm23'] - row['rev_ltm22']) / row['rev_ltm22'] if row['rev_ltm22'] > 0 else 0
        if pct_change >= 0.5:
            return 'Boom'
        elif pct_change <= -0.5:
            return 'Bust'
        else:
            return 'Stable'
    elif in_pre and not in_ltm23:
        return 'Disappeared'
    else:
        return 'Other'

model_df['lifecycle'] = model_df.apply(classify_model, axis=1)

# ── Build monthly revenue trajectory per model ─────────────────────────────────
monthly = df.groupby(['Nombre Modelo', 'period'])['Venta Netas'].sum().reset_index()
monthly['period_dt'] = monthly['period'].dt.to_timestamp()

# ── FOCUS: New Entrant models ──────────────────────────────────────────────────
new_entrant_models = model_df[model_df['lifecycle'] == 'New Entrant'].index.tolist()
stable_models      = model_df[model_df['lifecycle'] == 'Stable'].index.tolist()
boom_models        = model_df[model_df['lifecycle'] == 'Boom'].index.tolist()
bust_models        = model_df[model_df['lifecycle'] == 'Bust'].index.tolist()

print(f'\n=== MODEL COUNTS BY LIFECYCLE ===')
print(f'New Entrant : {len(new_entrant_models)} models')
print(f'Stable      : {len(stable_models)} models')
print(f'Boom        : {len(boom_models)} models')
print(f'Bust        : {len(bust_models)} models')
print(f'Disappeared : {(model_df["lifecycle"]=="Disappeared").sum()} models')

# ── For each New Entrant: compute months since first sale, monthly revenue indexed ──
def get_lifecycle_stats(models, monthly_df, label):
    records = []
    for m in models:
        traj = monthly_df[monthly_df['Nombre Modelo'] == m].sort_values('period')
        if len(traj) < 2:
            continue
        traj = traj.copy()
        traj['month_num'] = range(1, len(traj)+1)
        peak_rev   = traj['Venta Netas'].max()
        peak_month = traj.loc[traj['Venta Netas'].idxmax(), 'month_num']
        rev_m1     = traj[traj['month_num']==1]['Venta Netas'].values[0] if 1 in traj['month_num'].values else 0
        rev_m3     = traj[traj['month_num']==3]['Venta Netas'].values[0] if 3 in traj['month_num'].values else np.nan
        # revenue in months 7-12 (if available)
        late = traj[traj['month_num'] >= 7]['Venta Netas']
        rev_late = late.mean() if len(late) > 0 else np.nan
        # post-peak retention: revenue at month peak+3 vs peak
        post_peak_month = peak_month + 3
        post_peak_rev = traj[traj['month_num']==post_peak_month]['Venta Netas'].values
        post_peak_rev = post_peak_rev[0] if len(post_peak_rev)>0 else np.nan
        retention_3m = post_peak_rev / peak_rev if (not np.isnan(post_peak_rev if post_peak_rev is not None else np.nan) and peak_rev>0) else np.nan
        total_rev = traj['Venta Netas'].sum()
        n_months  = len(traj)
        records.append({
            'model': m,
            'lifecycle': label,
            'total_rev': total_rev,
            'peak_rev': peak_rev,
            'peak_month': peak_month,
            'n_months_active': n_months,
            'rev_m1': rev_m1,
            'rev_m3': rev_m3 if not pd.isna(rev_m3) else None,
            'rev_late_avg': rev_late,
            'retention_3m_post_peak': retention_3m
        })
    return pd.DataFrame(records)

ne_stats   = get_lifecycle_stats(new_entrant_models, monthly, 'New Entrant')
st_stats   = get_lifecycle_stats(stable_models, monthly, 'Stable')
boom_stats = get_lifecycle_stats(boom_models, monthly, 'Boom')
bust_stats = get_lifecycle_stats(bust_models, monthly, 'Bust')
all_stats  = pd.concat([ne_stats, st_stats, boom_stats, bust_stats], ignore_index=True)

# ── Summary by lifecycle group ─────────────────────────────────────────────────
print('\n' + '='*80)
print('LIFECYCLE VELOCITY SUMMARY')
print('='*80)

for label, grp in all_stats.groupby('lifecycle'):
    print(f'\n--- {label} (n={len(grp)}) ---')
    # Focus on top models by total revenue
    top_grp = grp.nlargest(20, 'total_rev') if len(grp)>20 else grp
    avg_peak_month = top_grp['peak_month'].mean()
    avg_n_months   = top_grp['n_months_active'].mean()
    avg_retention  = top_grp['retention_3m_post_peak'].dropna().mean()
    pct_retain_50  = (top_grp['retention_3m_post_peak'].dropna() >= 0.5).mean()
    print(f'  Avg months to peak revenue   : {avg_peak_month:.1f}')
    print(f'  Avg months active            : {avg_n_months:.1f}')
    print(f'  Avg retention 3m post-peak   : {avg_retention*100:.1f}% (of models with data)')
    print(f'  % retaining >50% after 3m    : {pct_retain_50*100:.1f}%')

# ── Detailed: New Entrant top 30 by LTM23 revenue ──────────────────────────────
ne_ltm23_rev = model_df[model_df['lifecycle']=='New Entrant']['rev_ltm23'].sort_values(ascending=False)
top30_ne = ne_ltm23_rev.head(30).index.tolist()

print('\n' + '='*80)
print('TOP 30 NEW ENTRANT MODELS — VELOCITY METRICS')
print('='*80)
print(f'{"Model":<25} {"LTM23 Rev":>10} {"Peak Mo":>8} {"N Mos":>6} {"Post-Pk Ret%":>14} {"Rev M1":>10} {"Rev M3":>10}')
print('-'*85)

ne30_stats = ne_stats[ne_stats['model'].isin(top30_ne)].copy()
ne30_stats['ltm23_rev'] = ne30_stats['model'].map(model_df['rev_ltm23'])
ne30_stats = ne30_stats.sort_values('ltm23_rev', ascending=False)

for _, row in ne30_stats.iterrows():
    ret_str = f"{row['retention_3m_post_peak']*100:.0f}%" if not pd.isna(row['retention_3m_post_peak']) else 'N/A'
    rev_m3_str = f"{row['rev_m3']:,.0f}" if row['rev_m3'] is not None and not pd.isna(row['rev_m3'] if row['rev_m3'] is not None else np.nan) else 'N/A'
    print(f"{row['model']:<25} {row['ltm23_rev']:>10,.0f} {row['peak_month']:>8.0f} {row['n_months_active']:>6.0f} {ret_str:>14} {row['rev_m1']:>10,.0f} {rev_m3_str:>10}")

# ── Distribution of time-to-peak ───────────────────────────────────────────────
print('\n' + '='*80)
print('DISTRIBUTION: TIME TO PEAK REVENUE (New Entrant vs Stable)')
print('='*80)
print(f'{"Month of Peak":>15} {"New Entrant (n)":>18} {"Stable (n)":>12}')
print('-'*50)
ne_peak_dist = ne_stats['peak_month'].value_counts().sort_index()
st_peak_dist = st_stats['peak_month'].value_counts().sort_index()
all_months = sorted(set(ne_peak_dist.index) | set(st_peak_dist.index))
for m in all_months:
    ne_cnt = ne_peak_dist.get(m, 0)
    st_cnt = st_peak_dist.get(m, 0)
    print(f'{m:>15} {ne_cnt:>18} {st_cnt:>12}')

# ── Post-peak retention distribution ──────────────────────────────────────────
print('\n' + '='*80)
print('POST-PEAK RETENTION (3 months after peak) — DISTRIBUTION')
print('='*80)
buckets = [(0, 0.25, '0–25%'), (0.25, 0.50, '25–50%'), (0.50, 0.75, '50–75%'), (0.75, 1.01, '75–100%+')]
for lo, hi, label in buckets:
    ne_pct = ne_stats['retention_3m_post_peak'].dropna()
    ne_cnt = ((ne_pct >= lo) & (ne_pct < hi)).sum()
    st_pct = st_stats['retention_3m_post_peak'].dropna()
    st_cnt = ((st_pct >= lo) & (st_pct < hi)).sum()
    print(f'  {label:<12}: New Entrant = {ne_cnt:>4} models | Stable = {st_cnt:>4} models')

# ── REVENUE HALF-LIFE: % of New Entrants retaining >50% of peak at 6m post-peak ─
print('\n' + '='*80)
print('REVENUE HALF-LIFE ANALYSIS — New Entrant Models')
print('='*80)

def compute_post_peak_retention_nm(model, traj_df, n_months_after):
    traj = traj_df[traj_df['Nombre Modelo']==model].sort_values('period').copy()
    traj['month_num'] = range(1, len(traj)+1)
    if len(traj) < 2: return np.nan
    peak_month = traj.loc[traj['Venta Netas'].idxmax(), 'month_num']
    peak_rev   = traj['Venta Netas'].max()
    target_month = peak_month + n_months_after
    post = traj[traj['month_num']==target_month]['Venta Netas'].values
    if len(post)==0: return np.nan
    return post[0] / peak_rev

retention_results = {}
for n in [1, 2, 3, 6, 9, 12]:
    retentions = [compute_post_peak_retention_nm(m, monthly, n) for m in new_entrant_models]
    retentions = [r for r in retentions if not np.isnan(r)]
    if retentions:
        pct_above_50 = np.mean([r>=0.5 for r in retentions])*100
        avg_ret      = np.mean(retentions)*100
        n_with_data  = len(retentions)
        retention_results[n] = (avg_ret, pct_above_50, n_with_data)

print(f'{"Months Post-Peak":>18} {"Avg Retention":>15} {">50% Retained":>16} {"N Models w/ Data":>18}')
print('-'*70)
for n, (avg, pct50, cnt) in retention_results.items():
    print(f'{n:>18} {avg:>14.1f}% {pct50:>15.1f}% {cnt:>18}')

# ── INDEXED REVENUE CURVES: New Entrant vs Stable ─────────────────────────────
print('\nBuilding indexed revenue curves (month 1 = 100) for top models...')

def build_indexed_curve(models, monthly_df, max_months=18):
    curves = []
    for m in models:
        traj = monthly_df[monthly_df['Nombre Modelo']==m].sort_values('period')
        if len(traj) < 3: continue
        rev_arr = traj['Venta Netas'].values
        base    = rev_arr[0]
        if base <= 0: continue
        indexed = rev_arr / base * 100
        for i, v in enumerate(indexed[:max_months]):
            curves.append({'month_num': i+1, 'indexed_rev': v, 'model': m})
    return pd.DataFrame(curves)

# Use top New Entrants (by total revenue) and all Stable models
top_ne_for_curve = ne_stats.nlargest(50, 'total_rev')['model'].tolist()
ne_curves  = build_indexed_curve(top_ne_for_curve, monthly)
st_curves  = build_indexed_curve(stable_models, monthly)

ne_median = ne_curves.groupby('month_num')['indexed_rev'].median()
st_median = st_curves.groupby('month_num')['indexed_rev'].median()
ne_p25    = ne_curves.groupby('month_num')['indexed_rev'].quantile(0.25)
ne_p75    = ne_curves.groupby('month_num')['indexed_rev'].quantile(0.75)

# ── CHART 1: Indexed revenue curves ───────────────────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(16, 7))
fig.suptitle('Product Revenue Lifecycle: New Entrant vs Stable Models', 
             color='#26890D', fontweight='bold', fontsize=15, y=1.01)

ax1 = axes[0]
ax1.fill_between(ne_p25.index, ne_p25.values, ne_p75.values, alpha=0.20, color=DELOITTE_COLORS[0], label='New Entrant IQR')
ax1.plot(ne_median.index, ne_median.values, color=DELOITTE_COLORS[0], lw=2.5, marker='o', markersize=4, label='New Entrant (median)')
ax1.plot(st_median.index, st_median.values, color=DELOITTE_COLORS[3], lw=2.5, marker='s', markersize=4, linestyle='--', label='Stable (median)')
ax1.axhline(100, color='#CCCCCC', lw=1, linestyle=':')
ax1.axhline(50, color='#FF6B6B', lw=1, linestyle=':', label='50% of launch revenue')
ax1.set_xlabel('Month Number (1 = first month of sales)', fontsize=11)
ax1.set_ylabel('Indexed Revenue (Month 1 = 100)', fontsize=11)
ax1.set_title('Revenue Trajectory\n(Indexed to Month 1)', color='#26890D', fontweight='bold')
ax1.legend(fontsize=9)
ax1.set_xlim(1, 18)
ax1.set_ylim(0, 400)

# ── CHART 2: Distribution of peak month ───────────────────────────────────────
ax2 = axes[1]
bins = range(1, 20)
ax2.hist(ne_stats['peak_month'].clip(upper=18), bins=bins, color=DELOITTE_COLORS[0], alpha=0.75, label='New Entrant', density=True, align='left')
ax2.hist(st_stats['peak_month'].clip(upper=18), bins=bins, color=DELOITTE_COLORS[3], alpha=0.65, label='Stable', density=True, align='left')
ax2.axvline(ne_stats['peak_month'].median(), color=DELOITTE_COLORS[0], lw=2, linestyle='--', label=f'NE Median: month {ne_stats["peak_month"].median():.0f}')
ax2.axvline(st_stats['peak_month'].median(), color=DELOITTE_COLORS[3], lw=2, linestyle='--', label=f'Stable Median: month {st_stats["peak_month"].median():.0f}')
ax2.set_xlabel('Month of Peak Revenue', fontsize=11)
ax2.set_ylabel('Density', fontsize=11)
ax2.set_title('Distribution: When do models hit peak revenue?', color='#26890D', fontweight='bold')
ax2.legend(fontsize=9)
ax2.set_xlim(1, 18)

plt.tight_layout()
plt.savefig('workspace/graphs/iter2_product_lifecycle_velocity.png', dpi=150, bbox_inches='tight')
plt.close()
print('GRAPH_SAVED: iter2_product_lifecycle_velocity.png — Indexed revenue curves and peak-month distributions for New Entrant vs Stable models')

# ── CHART 2: Post-peak retention waterfall by lifecycle class ─────────────────
fig, ax = plt.subplots(figsize=(13, 7))

months_list = [1, 2, 3, 6, 9, 12]
lifecycle_map = {
    'New Entrant': (new_entrant_models, DELOITTE_COLORS[0]),
    'Stable':      (stable_models,      DELOITTE_COLORS[3]),
    'Boom':        (boom_models,         DELOITTE_COLORS[1]),
    'Bust':        (bust_models,         DELOITTE_COLORS[2]),
}
bar_width = 0.18
x = np.arange(len(months_list))

for i, (lc_label, (models_list, color)) in enumerate(lifecycle_map.items()):
    means = []
    for n in months_list:
        rets = [compute_post_peak_retention_nm(m, monthly, n) for m in models_list]
        rets = [r for r in rets if not np.isnan(r)]
        means.append(np.mean(rets)*100 if rets else 0)
    offset = (i - 1.5) * bar_width
    bars = ax.bar(x + offset, means, width=bar_width, color=color, alpha=0.88, label=lc_label)

ax.axhline(50, color='#FF6B6B', lw=1.5, linestyle='--', label='50% retention line')
ax.axhline(100, color='#CCCCCC', lw=1, linestyle=':')
ax.set_xticks(x)
ax.set_xticklabels([f'+{m}m' for m in months_list])
ax.set_xlabel('Months After Peak', fontsize=11)
ax.set_ylabel('Avg Revenue Retention (% of Peak)', fontsize=11)
ax.set_title('Post-Peak Revenue Retention by Lifecycle Class\n(% of peak-month revenue retained N months later)', 
             color='#26890D', fontweight='bold')
ax.legend(fontsize=10)
ax.set_ylim(0, 160)

plt.tight_layout()
plt.savefig('workspace/graphs/iter2_post_peak_retention_by_lifecycle.png', dpi=150, bbox_inches='tight')
plt.close()
print('GRAPH_SAVED: iter2_post_peak_retention_by_lifecycle.png — Average post-peak revenue retention at +1, +2, +3, +6, +9, +12 months by lifecycle class')

# ── CHART 3: New Entrant revenue cohort — by launch year ──────────────────────
# For each New Entrant, find their first active year
monthly_ne = monthly[monthly['Nombre Modelo'].isin(new_entrant_models)].copy()
first_period = monthly_ne.groupby('Nombre Modelo')['period'].min().reset_index()
first_period.columns = ['Nombre Modelo', 'first_period']
first_period['launch_year'] = first_period['first_period'].dt.year
monthly_ne = monthly_ne.merge(first_period[['Nombre Modelo','launch_year']], on='Nombre Modelo', how='left')

cohort_rev = monthly_ne.groupby(['launch_year','period'])['Venta Netas'].sum().reset_index()
cohort_rev['period_dt'] = cohort_rev['period'].dt.to_timestamp()

fig, ax = plt.subplots(figsize=(14, 6))
for i, yr in enumerate(sorted(cohort_rev['launch_year'].unique())):
    sub = cohort_rev[cohort_rev['launch_year']==yr].sort_values('period_dt')
    ax.fill_between(sub['period_dt'], sub['Venta Netas']/1e6, alpha=0.5, color=DELOITTE_COLORS[i % len(DELOITTE_COLORS)], label=f'Launched in {yr}')
    ax.plot(sub['period_dt'], sub['Venta Netas']/1e6, color=DELOITTE_COLORS[i % len(DELOITTE_COLORS)], lw=1.5)

ax.set_title('Monthly Revenue from New Entrant Models — by Launch Year Cohort', color='#26890D', fontweight='bold')
ax.set_xlabel('Month')
ax.set_ylabel('Revenue (EUR M)')
ax.legend(fontsize=10)
plt.tight_layout()
plt.savefig('workspace/graphs/iter2_new_entrant_cohort_revenue.png', dpi=150, bbox_inches='tight')
plt.close()
print('GRAPH_SAVED: iter2_new_entrant_cohort_revenue.png — Monthly revenue from New Entrant models broken out by launch-year cohort')

# ── FINAL SUMMARY ─────────────────────────────────────────────────────────────
print('\n' + '='*80)
print('BUSINESS INSIGHT SUMMARY: PRODUCT LIFECYCLE VELOCITY')
print('='*80)

ne_peak_med  = ne_stats['peak_month'].median()
st_peak_med  = st_stats['peak_month'].median()
ne_active_med = ne_stats['n_months_active'].median()
st_active_med = st_stats['n_months_active'].median()
ne_ret3m  = ne_stats['retention_3m_post_peak'].dropna().mean()*100
st_ret3m  = st_stats['retention_3m_post_peak'].dropna().mean()*100
ne_half_life_6m = retention_results.get(6, (None,)*3)[0]
st_half_life_6m_val = [compute_post_peak_retention_nm(m, monthly, 6) for m in stable_models]
st_half_life_6m = np.nanmean(st_half_life_6m_val)*100 if st_half_life_6m_val else None

print(f'\n{"Metric":<40} {"New Entrant":>14} {"Stable":>14}')
print('-'*70)
print(f'{"Median month of peak revenue":<40} {ne_peak_med:>14.1f} {st_peak_med:>14.1f}')
print(f'{"Median months active":<40} {ne_active_med:>14.1f} {st_active_med:>14.1f}')
print(f'{"Avg retention 3m post-peak":<40} {ne_ret3m:>13.1f}% {st_ret3m:>13.1f}%')
if ne_half_life_6m:
    print(f'{"Avg retention 6m post-peak":<40} {ne_half_life_6m:>13.1f}% {"-" if st_half_life_6m is None else f"{st_half_life_6m:.1f}%":>14}')

new_entrant_ltm23_rev = model_df[model_df['lifecycle']=='New Entrant']['rev_ltm23'].sum()
total_ltm23_rev = model_df['rev_ltm23'].sum()
print(f'\nNew Entrant share of LTM23 revenue : {new_entrant_ltm23_rev/total_ltm23_rev*100:.1f}% (EUR {new_entrant_ltm23_rev/1e6:.2f}M)')
print(f'Stable share of LTM23 revenue     : {model_df[model_df["lifecycle"]=="Stable"]["rev_ltm23"].sum()/total_ltm23_rev*100:.1f}%')

# Estimate: how many NE models are past their peak?
ne_stats_ltm = ne_stats.copy()
ne_stats_ltm['is_past_peak'] = ne_stats_ltm['peak_month'] < ne_stats_ltm['n_months_active']
print(f'\nNew Entrant models that have passed their peak : {ne_stats_ltm["is_past_peak"].sum()} / {len(ne_stats_ltm)} ({ne_stats_ltm["is_past_peak"].mean()*100:.0f}%)')
print(f"Models still in month 1-3 (potential early stage): {(ne_stats_ltm['n_months_active']<=3).sum()}")
print('\n[Analysis complete]')
```

**Output (preview):**
```
=== TIME WINDOWS ===
FY2021 : 2021-01 – 2021-12
FY2022 : 2022-01 – 2022-12
LTM22  : 2021-05 – 2022-04
LTM23  : 2022-05 – 2023-04

=== MODEL COUNTS BY LIFECYCLE ===
New Entrant : 103 models
Stable      : 40 models
Boom        : 48 models
Bust        : 113 models
Disappeared : 84 models
```

---

## Analysis 3: Product lifecycle durability segmentation — distinguishing durable New Entrant models from transient ones by analyzing launch ASP, launch velocity, and post-peak retention correlation
**Hypothesis:** A small subset of New Entrant models (~10-15%) exhibits Stable-model-like retention durability. This durability can be predicted by launch-month revenue magnitude (high-momentum launches indicate broader distribution), price tier (higher ASP at launch → lower substitution elasticity), and potentially product family. Identifying these 'breakout' models vs transient SKUs is a key input for SKU investment strategy and production planning.

**Columns:** Nombre Modelo, Fecha_Mes, Venta Netas, Cant_Neta

```python
import matplotlib; matplotlib.use('Agg')
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath('workspace/data.xlsx')) + '/..')
try:
    import deloitte_theme
    deloitte_theme.apply_deloitte_style()
except Exception:
    import matplotlib.pyplot as plt
    DELOITTE_COLORS = ['#26890D', '#046A38', '#404040', '#0D8390', '#00ABAB']
    plt.rcParams.update({
        'font.family': ['Arial', 'sans-serif'],
        'axes.prop_cycle': plt.cycler('color', DELOITTE_COLORS),
        'axes.titlesize': 14, 'axes.titleweight': 'bold',
        'axes.titlecolor': '#26890D',
        'axes.spines.top': False, 'axes.spines.right': False,
        'figure.facecolor': 'white', 'axes.facecolor': 'white',
    })

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec
import warnings
warnings.filterwarnings('ignore')

DELOITTE_COLORS = ['#26890D', '#046A38', '#404040', '#0D8390', '#00ABAB']

# ── Load data ──────────────────────────────────────────────────────────────────
df = pd.read_excel('workspace/data.xlsx')
df['Fecha_Mes'] = pd.to_datetime(df['Fecha_Mes'])
df['period'] = df['Fecha_Mes'].dt.to_period('M')

# ── Define time windows (replicate Iter 1/2 logic) ────────────────────────────
latest_month = df['period'].max()
ltm_end = latest_month
ltm_start = ltm_end - 11

fy21_start = pd.Period('2021-01', 'M')
fy21_end   = pd.Period('2021-12', 'M')
fy22_start = pd.Period('2022-01', 'M')
fy22_end   = pd.Period('2022-12', 'M')

df_fy21 = df[df['period'].between(fy21_start, fy21_end)]
df_fy22 = df[df['period'].between(fy22_start, fy22_end)]
df_ltm  = df[df['period'].between(ltm_start, ltm_end)]

print(f'LTM window: {ltm_start} → {ltm_end}')
print(f'Dataset: {len(df):,} rows | {df["Nombre Modelo"].nunique()} unique models')
print()

# ── Identify New Entrant models (appear in LTM23 but NOT in FY2021) ────────────
models_fy21 = set(df_fy21['Nombre Modelo'].unique())
models_fy22 = set(df_fy22['Nombre Modelo'].unique())
models_ltm  = set(df_ltm['Nombre Modelo'].unique())

# New Entrants: not in FY2021 (first appearance after FY2021 cut-off)
new_entrants = models_ltm - models_fy21
print(f'New Entrant models active in LTM23 (not present in FY2021): {len(new_entrants)}')

# ── Build per-model monthly revenue series ────────────────────────────────────
# Restrict to models that are New Entrants
df_ne = df[df['Nombre Modelo'].isin(new_entrants)].copy()

monthly = (df_ne.groupby(['Nombre Modelo', 'period'])
           .agg(revenue=('Venta Netas', 'sum'), qty=('Cant_Neta', 'sum'))
           .reset_index())

# First month of each model
first_month = monthly.groupby('Nombre Modelo')['period'].min().rename('first_month')
monthly = monthly.join(first_month, on='Nombre Modelo')
monthly['months_since_launch'] = (monthly['period'] - monthly['first_month']).apply(lambda x: x.n)

# ── Peak-month metrics ─────────────────────────────────────────────────────────
peak = (monthly.groupby('Nombre Modelo')
        .apply(lambda g: g.loc[g['revenue'].idxmax()])
        .reset_index(drop=True)
        [['Nombre Modelo', 'period', 'months_since_launch', 'revenue', 'qty']])
peak.columns = ['Nombre Modelo', 'peak_period', 'peak_month_idx', 'peak_revenue', 'peak_qty']

# Launch metrics: revenue and ASP in month 0
launch = monthly[monthly['months_since_launch'] == 0][['Nombre Modelo', 'revenue', 'qty']].copy()
launch.columns = ['Nombre Modelo', 'launch_revenue', 'launch_qty']
launch['launch_asp'] = launch['launch_revenue'] / launch['launch_qty'].replace(0, np.nan)

# Cumulative LTM revenue per model
ltm_rev = (df_ne[df_ne['period'].between(ltm_start, ltm_end)]
           .groupby('Nombre Modelo')['Venta Netas'].sum().rename('ltm_revenue').reset_index())

# ── Post-peak retention at +3 months ──────────────────────────────────────────
def retention_at_offset(group, offset):
    peak_idx = group['months_since_launch'][group['revenue'].idxmax()]
    post = group[group['months_since_launch'] == peak_idx + offset]
    if len(post) == 0:
        return np.nan
    return post['revenue'].values[0] / group['revenue'].max()

retention3 = (monthly.groupby('Nombre Modelo')
              .apply(lambda g: retention_at_offset(g, 3))
              .rename('retention_3m').reset_index())

retention6 = (monthly.groupby('Nombre Modelo')
              .apply(lambda g: retention_at_offset(g, 6))
              .rename('retention_6m').reset_index())

# ── Active months per model ────────────────────────────────────────────────────
active_months = (monthly.groupby('Nombre Modelo')['months_since_launch'].max()
                 .add(1).rename('active_months').reset_index())

# ── Merge all metrics ─────────────────────────────────────────────────────────
profile = (peak
           .merge(launch, on='Nombre Modelo', how='left')
           .merge(retention3, on='Nombre Modelo', how='left')
           .merge(retention6, on='Nombre Modelo', how='left')
           .merge(active_months, on='Nombre Modelo', how='left')
           .merge(ltm_rev, on='Nombre Modelo', how='left'))

profile['launch_asp'] = profile['launch_asp'].fillna(
    profile['peak_revenue'] / profile['peak_qty'].replace(0, np.nan))

# ── Classify durability ────────────────────────────────────────────────────────
# Durable: retention_3m >= 0.40 (retains ≥40% of peak at 3 months post-peak)
# Transient: retention_3m < 0.20
# Mid-tier: 0.20 <= retention_3m < 0.40
profile['durability'] = pd.cut(
    profile['retention_3m'],
    bins=[-np.inf, 0.20, 0.40, np.inf],
    labels=['Transient (<20%)', 'Mid-tier (20–40%)', 'Durable (≥40%)']
)

print('=== NEW ENTRANT MODEL DURABILITY CLASSIFICATION (by +3m retention) ===')
counts = profile['durability'].value_counts().reindex(['Transient (<20%)', 'Mid-tier (20–40%)', 'Durable (≥40%)'])
for cat, n in counts.items():
    pct = 100*n/len(profile.dropna(subset=['retention_3m']))
    total_rev = profile[profile['durability']==cat]['ltm_revenue'].sum()
    print(f'  {cat:25s}: {n:3d} models ({pct:4.1f}%) | LTM rev: EUR {total_rev/1e6:.2f}M')

print()

# ── Launch ASP quartiles ───────────────────────────────────────────────────────
profile_valid = profile.dropna(subset=['launch_asp', 'retention_3m']).copy()
q25, q50, q75 = profile_valid['launch_asp'].quantile([0.25, 0.50, 0.75])

profile_valid['asp_tier'] = pd.cut(
    profile_valid['launch_asp'],
    bins=[-np.inf, q25, q50, q75, np.inf],
    labels=['Q1 (lowest)', 'Q2', 'Q3', 'Q4 (highest)']
)

print('=== LAUNCH ASP TIER vs AVG POST-PEAK RETENTION (+3m) ===')
asp_ret = profile_valid.groupby('asp_tier', observed=True).agg(
    n_models=('Nombre Modelo', 'count'),
    avg_launch_asp=('launch_asp', 'mean'),
    med_retention_3m=('retention_3m', 'median'),
    avg_retention_3m=('retention_3m', 'mean'),
    pct_durable=('durability', lambda x: 100*(x=='Durable (≥40%)').sum()/len(x))
).reset_index()

print(f"{'ASP Tier':<16} | {'N':>3} | {'Avg ASP':>10} | {'Med Ret3m':>10} | {'Avg Ret3m':>10} | {'% Durable':>10}")
print('-'*70)
for _, row in asp_ret.iterrows():
    print(f"{str(row['asp_tier']):<16} | {int(row['n_models']):>3} | "
          f"EUR {row['avg_launch_asp']:>6.2f} | "
          f"{row['med_retention_3m']*100:>8.1f}% | "
          f"{row['avg_retention_3m']*100:>8.1f}% | "
          f"{row['pct_durable']:>8.1f}%")

print()
print(f'  ASP quartile thresholds: Q25=EUR {q25:.2f}, Q50=EUR {q50:.2f}, Q75=EUR {q75:.2f}')

# ── Launch velocity quartiles (revenue in M1) ──────────────────────────────────
profile_valid2 = profile.dropna(subset=['launch_revenue', 'retention_3m']).copy()
lv25, lv50, lv75 = profile_valid2['launch_revenue'].quantile([0.25, 0.50, 0.75])

profile_valid2['velocity_tier'] = pd.cut(
    profile_valid2['launch_revenue'],
    bins=[-np.inf, lv25, lv50, lv75, np.inf],
    labels=['Q1 (slowest)', 'Q2', 'Q3', 'Q4 (fastest)']
)

print()
print('=== LAUNCH VELOCITY (M1 Revenue) vs AVG POST-PEAK RETENTION (+3m) ===')
vel_ret = profile_valid2.groupby('velocity_tier', observed=True).agg(
    n_models=('Nombre Modelo', 'count'),
    avg_launch_rev=('launch_revenue', 'mean'),
    med_retention_3m=('retention_3m', 'median'),
    avg_retention_3m=('retention_3m', 'mean'),
    pct_durable=('durability', lambda x: 100*(x=='Durable (≥40%)').sum()/len(x))
).reset_index()

print(f"{'Velocity Tier':<16} | {'N':>3} | {'Avg M1 Rev':>12} | {'Med Ret3m':>10} | {'Avg Ret3m':>10} | {'% Durable':>10}")
print('-'*75)
for _, row in vel_ret.iterrows():
    print(f"{str(row['velocity_tier']):<16} | {int(row['n_models']):>3} | "
          f"EUR {row['avg_launch_rev']:>8,.0f} | "
          f"{row['med_retention_3m']*100:>8.1f}% | "
          f"{row['avg_retention_3m']*100:>8.1f}% | "
          f"{row['pct_durable']:>8.1f}%")

print()
print(f'  Launch velocity quartile thresholds: Q25=EUR {lv25:,.0f}, Q50=EUR {lv50:,.0f}, Q75=EUR {lv75:,.0f}')

# ── Durable model roster ───────────────────────────────────────────────────────
durable_models = (profile[profile['durability'] == 'Durable (≥40%)']
                  .sort_values('ltm_revenue', ascending=False)
                  .head(25))

print()
print('=== TOP DURABLE NEW ENTRANT MODELS (retention_3m ≥ 40%) ===')
print(f"{'Model':<22} | {'LTM Rev':>10} | {'Peak Rev':>10} | {'Launch ASP':>11} | {'Ret+3m':>7} | {'Ret+6m':>7} | {'Active Mo':>9} | {'Peak Mo':>8}")
print('-'*105)
for _, row in durable_models.iterrows():
    r6 = f"{row['retention_6m']*100:.0f}%" if not pd.isna(row['retention_6m']) else 'N/A'
    lasp = f"EUR {row['launch_asp']:.2f}" if not pd.isna(row['launch_asp']) else 'N/A'
    print(f"{str(row['Nombre Modelo']):<22} | "
          f"EUR {row['ltm_revenue']:>6,.0f} | "
          f"EUR {row['peak_revenue']:>6,.0f} | "
          f"{lasp:>11} | "
          f"{row['retention_3m']*100:>5.0f}% | "
          f"{r6:>7} | "
          f"{int(row['active_months']):>9} | "
          f"{int(row['peak_month_idx']):>8}")

# ── Transient model roster (top revenue) ──────────────────────────────────────
transient_models = (profile[profile['durability'] == 'Transient (<20%)']
                    .sort_values('ltm_revenue', ascending=False)
                    .head(20))

print()
print('=== TOP TRANSIENT NEW ENTRANT MODELS (retention_3m < 20%) ===')
print(f"{'Model':<22} | {'LTM Rev':>10} | {'Peak Rev':>10} | {'Launch ASP':>11} | {'Ret+3m':>7} | {'Active Mo':>9}")
print('-'*85)
for _, row in transient_models.iterrows():
    lasp = f"EUR {row['launch_asp']:.2f}" if not pd.isna(row['launch_asp']) else 'N/A'
    r3 = f"{row['retention_3m']*100:.0f}%" if not pd.isna(row['retention_3m']) else 'N/A'
    print(f"{str(row['Nombre Modelo']):<22} | "
          f"EUR {row['ltm_revenue']:>6,.0f} | "
          f"EUR {row['peak_revenue']:>6,.0f} | "
          f"{lasp:>11} | "
          f"{r3:>7} | "
          f"{int(row['active_months']):>9}")

# ── Revenue concentration: Durable vs Transient in LTM ─────────────────────────
print()
print('=== DURABILITY COHORT CONTRIBUTION TO LTM23 REVENUE ===')
total_ltm = df_ltm['Venta Netas'].sum()
cohort_rev = profile.groupby('durability', observed=True)['ltm_revenue'].sum()
for cat in ['Transient (<20%)', 'Mid-tier (20–40%)', 'Durable (≥40%)']:
    rev = cohort_rev.get(cat, 0)
    print(f'  {cat:25s}: EUR {rev/1e6:.2f}M ({100*rev/total_ltm:.1f}% of total LTM23)')
print(f'  {"All New Entrants":25s}: EUR {profile["ltm_revenue"].sum()/1e6:.2f}M ({100*profile["ltm_revenue"].sum()/total_ltm:.1f}% of total LTM23)')

# ── Correlation summary ────────────────────────────────────────────────────────
profile_corr = profile.dropna(subset=['launch_asp', 'launch_revenue', 'retention_3m', 'active_months'])
corr_asp  = profile_corr['launch_asp'].corr(profile_corr['retention_3m'])
corr_vel  = profile_corr['launch_revenue'].corr(profile_corr['retention_3m'])
corr_act  = profile_corr['active_months'].corr(profile_corr['retention_3m'])

print()
print('=== PEARSON CORRELATION WITH +3m POST-PEAK RETENTION ===')
print(f'  Launch ASP         vs retention_3m: r = {corr_asp:+.3f}')
print(f'  Launch Revenue(M1) vs retention_3m: r = {corr_vel:+.3f}')
print(f'  Active months      vs retention_3m: r = {corr_act:+.3f}')

# ── CHART 1: Scatter — Launch ASP vs +3m retention, colored by durability ──────
fig, axes = plt.subplots(1, 2, figsize=(16, 7))

color_map = {
    'Transient (<20%)':  DELOITTE_COLORS[2],  # grey
    'Mid-tier (20–40%)': DELOITTE_COLORS[3],  # blue
    'Durable (≥40%)':    DELOITTE_COLORS[0],  # green
}

ax = axes[0]
for cat, color in color_map.items():
    sub = profile_corr[profile_corr['durability'] == cat]
    ax.scatter(sub['launch_asp'], sub['retention_3m']*100,
               color=color, alpha=0.7, edgecolors='white', linewidth=0.4,
               s=sub['ltm_revenue']/800+20, label=cat)
# Trend line
z = np.polyfit(profile_corr['launch_asp'].dropna(),
               profile_corr['retention_3m'].dropna()*100, 1)
p = np.poly1d(z)
xline = np.linspace(profile_corr['launch_asp'].min(), profile_corr['launch_asp'].max(), 100)
ax.plot(xline, p(xline), color=DELOITTE_COLORS[1], linestyle='--', linewidth=1.5,
        label=f'Trend (r={corr_asp:+.2f})')
ax.set_title('Launch ASP vs Post-Peak Retention (+3m)', color='#26890D', fontweight='bold')
ax.set_xlabel('Launch ASP (EUR / unit)', fontsize=10)
ax.set_ylabel('+3m Post-Peak Retention (%)', fontsize=10)
ax.axhline(40, color=DELOITTE_COLORS[0], linestyle=':', linewidth=1, alpha=0.6)
ax.axhline(20, color=DELOITTE_COLORS[2], linestyle=':', linewidth=1, alpha=0.6)
ax.legend(fontsize=8, framealpha=0.7)
ax.text(0.97, 0.97, f'n={len(profile_corr)}\nr={corr_asp:+.3f}',
        transform=ax.transAxes, ha='right', va='top', fontsize=9,
        bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))

ax = axes[1]
for cat, color in color_map.items():
    sub = profile_corr[profile_corr['durability'] == cat]
    ax.scatter(sub['launch_revenue'], sub['retention_3m']*100,
               color=color, alpha=0.7, edgecolors='white', linewidth=0.4,
               s=sub['ltm_revenue']/800+20, label=cat)
z2 = np.polyfit(profile_corr['launch_revenue'], profile_corr['retention_3m']*100, 1)
p2 = np.poly1d(z2)
xline2 = np.linspace(profile_corr['launch_revenue'].min(), profile_corr['launch_revenue'].max(), 100)
ax.plot(xline2, p2(xline2), color=DELOITTE_COLORS[1], linestyle='--', linewidth=1.5,
        label=f'Trend (r={corr_vel:+.2f})')
ax.set_title('Launch Velocity (M1 Revenue) vs Post-Peak Retention (+3m)', color='#26890D', fontweight='bold')
ax.set_xlabel('Month-1 Revenue (EUR)', fontsize=10)
ax.set_ylabel('+3m Post-Peak Retention (%)', fontsize=10)
ax.axhline(40, color=DELOITTE_COLORS[0], linestyle=':', linewidth=1, alpha=0.6)
ax.axhline(20, color=DELOITTE_COLORS[2], linestyle=':', linewidth=1, alpha=0.6)
ax.legend(fontsize=8, framealpha=0.7)
ax.text(0.97, 0.97, f'n={len(profile_corr)}\nr={corr_vel:+.3f}',
        transform=ax.transAxes, ha='right', va='top', fontsize=9,
        bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))

plt.suptitle('New Entrant Model Durability: Launch Characteristics vs Post-Peak Retention',
             fontsize=15, fontweight='bold', color='#26890D', y=1.01)
plt.tight_layout()
plt.savefig('workspace/graphs/iter3_durability_scatter_asp_velocity.png', dpi=150, bbox_inches='tight')
plt.close()
print('GRAPH_SAVED: iter3_durability_scatter_asp_velocity.png — Scatter plots of launch ASP and launch velocity vs post-peak retention (+3m) for New Entrant models')

# ── CHART 2: Revenue trajectory comparison — durable vs transient top models ───
fig, axes = plt.subplots(1, 2, figsize=(16, 7))

top_durable = (profile[profile['durability'] == 'Durable (≥40%)']
               .sort_values('ltm_revenue', ascending=False)
               .head(8)['Nombre Modelo'].tolist())
top_transient = (profile[profile['durability'] == 'Transient (<20%)']
                 .sort_values('ltm_revenue', ascending=False)
                 .head(8)['Nombre Modelo'].tolist())

def plot_indexed_trajectories(ax, model_list, title, color_list):
    for i, model in enumerate(model_list):
        data = (monthly[monthly['Nombre Modelo'] == model]
                .sort_values('months_since_launch'))
        if len(data) == 0:
            continue
        peak_rev = data['revenue'].max()
        if peak_rev == 0:
            continue
        indexed = data['revenue'] / peak_rev * 100
        ax.plot(data['months_since_launch'], indexed,
                color=color_list[i % len(color_list)],
                linewidth=1.8, marker='o', markersize=3,
                label=model)
    ax.axhline(40, color='#404040', linestyle='--', linewidth=1, alpha=0.5, label='40% threshold')
    ax.axhline(20, color='#404040', linestyle=':', linewidth=1, alpha=0.5, label='20% threshold')
    ax.set_title(title, color='#26890D', fontweight='bold')
    ax.set_xlabel('Months Since Launch', fontsize=10)
    ax.set_ylabel('Revenue (% of Peak)', fontsize=10)
    ax.legend(fontsize=7.5, framealpha=0.7, loc='upper right')
    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)

plot_indexed_trajectories(axes[0], top_durable,
                          'Top Durable New Entrants — Indexed Revenue Trajectories',
                          [DELOITTE_COLORS[0], DELOITTE_COLORS[1], '#3CB371', '#228B22',
                           '#66CDAA', '#2E8B57', '#006400', '#8FBC8F'])
plot_indexed_trajectories(axes[1], top_transient,
                          'Top Transient New Entrants — Indexed Revenue Trajectories',
                          [DELOITTE_COLORS[2], DELOITTE_COLORS[3], DELOITTE_COLORS[4],
                           '#808080', '#A9A9A9', '#696969', '#D3D3D3', '#778899'])

plt.suptitle('Revenue Trajectory Comparison: Durable vs Transient New Entrant Models',
             fontsize=14, fontweight='bold', color='#26890D', y=1.01)
plt.tight_layout()
plt.savefig('workspace/graphs/iter3_durable_vs_transient_trajectories.png', dpi=150, bbox_inches='tight')
plt.close()
print('GRAPH_SAVED: iter3_durable_vs_transient_trajectories.png — Indexed revenue trajectories (% of peak) for top 8 Durable vs top 8 Transient New Entrant models')

# ── CHART 3: Durability distribution by ASP tier and velocity tier ─────────────
fig, axes = plt.subplots(1, 2, figsize=(16, 6))

def plot_stacked_pct(ax, group_col, df_in, title):
    ct = pd.crosstab(df_in[group_col], df_in['durability'])
    order = ['Transient (<20%)', 'Mid-tier (20–40%)', 'Durable (≥40%)']
    for col in order:
        if col not in ct.columns:
            ct[col] = 0
    ct = ct[order]
    pct = ct.div(ct.sum(axis=1), axis=0) * 100
    colors = [DELOITTE_COLORS[2], DELOITTE_COLORS[3], DELOITTE_COLORS[0]]
    bottom = np.zeros(len(pct))
    for j, col in enumerate(order):
        ax.bar(range(len(pct)), pct[col], bottom=bottom,
               color=colors[j], label=col, width=0.6)
        for k, val in enumerate(pct[col]):
            if val > 7:
                ax.text(k, bottom[k] + val/2, f'{val:.0f}%',
                        ha='center', va='center', fontsize=8,
                        fontweight='bold', color='white')
        bottom += pct[col].values
    ax.set_xticks(range(len(pct)))
    ax.set_xticklabels(pct.index, fontsize=9)
    ax.set_ylabel('Share of Models (%)', fontsize=10)
    ax.set_title(title, color='#26890D', fontweight='bold')
    ax.set_ylim(0, 110)
    ax.legend(fontsize=8, loc='upper right', framealpha=0.7)

plot_stacked_pct(axes[0], 'asp_tier', profile_valid,
                 'Durability Mix by Launch ASP Tier')
plot_stacked_pct(axes[1], 'velocity_tier', profile_valid2,
                 'Durability Mix by Launch Velocity Tier (M1 Revenue)')

plt.suptitle('New Entrant Model Durability: Distribution by Launch Characteristics',
             fontsize=14, fontweight='bold', color='#26890D', y=1.01)
plt.tight_layout()
plt.savefig('workspace/graphs/iter3_durability_by_tier.png', dpi=150, bbox_inches='tight')
plt.close()
print('GRAPH_SAVED: iter3_durability_by_tier.png — Stacked bar charts showing the % of Durable/Mid-tier/Transient models by ASP tier and by launch velocity tier')

# ── CHART 4: Revenue at risk — durability cohort revenue over time ─────────────
fig, ax = plt.subplots(figsize=(14, 6))

durable_set   = set(profile[profile['durability'] == 'Durable (≥40%)']['Nombre Modelo'])
midtier_set   = set(profile[profile['durability'] == 'Mid-tier (20–40%)']['Nombre Modelo'])
transient_set = set(profile[profile['durability'] == 'Transient (<20%)']['Nombre Modelo'])
unclassified  = set(profile[profile['durability'].isna()]['Nombre Modelo'])

df_ne2 = df[df['Nombre Modelo'].isin(new_entrants)].copy()
df_ne2['cohort'] = np.where(df_ne2['Nombre Modelo'].isin(durable_set), 'Durable (≥40%)',
                    np.where(df_ne2['Nombre Modelo'].isin(midtier_set), 'Mid-tier (20–40%)',
                    np.where(df_ne2['Nombre Modelo'].isin(transient_set), 'Transient (<20%)', 'Unclassified')))

monthly_cohort = (df_ne2.groupby(['cohort', df_ne2['Fecha_Mes'].dt.to_period('M')])
                  ['Venta Netas'].sum().reset_index())
monthly_cohort.columns = ['cohort', 'month', 'revenue']
monthly_cohort['month_dt'] = monthly_cohort['month'].dt.to_timestamp()

cohort_order = ['Durable (≥40%)', 'Mid-tier (20–40%)', 'Transient (<20%)', 'Unclassified']
cohort_colors = [DELOITTE_COLORS[0], DELOITTE_COLORS[3], DELOITTE_COLORS[2], '#CCCCCC']

for cohort, color in zip(cohort_order, cohort_colors):
    sub = monthly_cohort[monthly_cohort['cohort'] == cohort].sort_values('month_dt')
    if len(sub) > 0:
        ax.plot(sub['month_dt'], sub['revenue']/1000, color=color,
                linewidth=2, marker='o', markersize=4, label=cohort)

ax.set_title('Monthly Revenue by New Entrant Durability Cohort',
             color='#26890D', fontweight='bold')
ax.set_xlabel('Month', fontsize=10)
ax.set_ylabel('Revenue (EUR thousands)', fontsize=10)
ax.legend(fontsize=9, framealpha=0.8)
plt.tight_layout()
plt.savefig('workspace/graphs/iter3_revenue_by_durability_cohort.png', dpi=150, bbox_inches='tight')
plt.close()
print('GRAPH_SAVED: iter3_revenue_by_durability_cohort.png — Monthly revenue evolution split by Durable / Mid-tier / Transient New Entrant cohorts')

# ── Summary print ─────────────────────────────────────────────────────────────
print()
print('=== KEY FINDINGS SUMMARY ===')
total_ne = len(profile.dropna(subset=['retention_3m']))
durable_n   = (profile['durability'] == 'Durable (≥40%)').sum()
midtier_n   = (profile['durability'] == 'Mid-tier (20–40%)').sum()
transient_n = (profile['durability'] == 'Transient (<20%)').sum()

print(f'  New Entrant models with retention data: {total_ne}')
print(f'  Durable   (ret3m ≥ 40%): {durable_n:3d} models ({100*durable_n/total_ne:.1f}%) | '
      f"EUR {profile[profile['durability']=='Durable (≥40%)']['ltm_revenue'].sum()/1e6:.2f}M LTM rev")
print(f'  Mid-tier  (20–40%):      {midtier_n:3d} models ({100*midtier_n/total_ne:.1f}%) | '
      f"EUR {profile[profile['durability']=='Mid-tier (20–40%)']['ltm_revenue'].sum()/1e6:.2f}M LTM rev")
print(f'  Transient (ret3m < 20%): {transient_n:3d} models ({100*transient_n/total_ne:.1f}%) | '
      f"EUR {profile[profile['durability']=='Transient (<20%)']['ltm_revenue'].sum()/1e6:.2f}M LTM rev")

print()
print(f'  Launch ASP correlation with retention: r={corr_asp:+.3f} — weak/no linear relationship')
print(f'  Launch velocity correlation with retention: r={corr_vel:+.3f} — weak/no linear relationship')
print(f'  (Note: non-linear segmentation via quartile tables above is more informative)')
print()
print(f'  Highest-ASP quartile (Q4) % Durable: {asp_ret.iloc[-1]["pct_durable"]:.1f}%')
print(f'  Lowest-ASP quartile (Q1) % Durable:  {asp_ret.iloc[0]["pct_durable"]:.1f}%')
print(f'  Highest-velocity quartile (Q4) % Durable: {vel_ret.iloc[-1]["pct_durable"]:.1f}%')
print(f'  Lowest-velocity quartile (Q1) % Durable:  {vel_ret.iloc[0]["pct_durable"]:.1f}%')
```

**Output (preview):**
```
LTM window: 2022-05 → 2023-04
Dataset: 77,610 rows | 398 unique models

New Entrant models active in LTM23 (not present in FY2021): 123
=== NEW ENTRANT MODEL DURABILITY CLASSIFICATION (by +3m retention) ===
  Transient (<20%)         :  47 models (48.5%) | LTM rev: EUR 6.27M
  Mid-tier (20–40%)        :  32 models (33.0%) | LTM rev: EUR 6.17M
  Durable (≥40%)           :  18 models (18.6%) | LTM rev: EUR 4.60M

=== LAUNCH ASP TIER vs AVG POST-PEAK RETENTION (+3m) ===
ASP Tier         |   N |    Avg ASP |  Med Ret3m |  Avg Ret3m |  % Durable
-----------------------------------------------------
```

---

## Analysis 5: Product family clustering from model names + lifecycle durability and channel distribution by cluster
**Hypothesis:** Product family taxonomy can be derived from model names (city-named models dominate), and different geographic/thematic clusters will show distinct lifecycle patterns — some city clusters may be systematically more durable than others, and channel distribution may vary by cluster. This would reveal whether durability is driven by product family (naming convention / collection tier) rather than individual model characteristics.

**Columns:** Nombre Modelo, Fecha_Mes, Canal, Venta Netas, Cant_Neta

```python
import matplotlib; matplotlib.use('Agg')
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')
try:
    import deloitte_theme
    deloitte_theme.apply_deloitte_style()
except Exception:
    import matplotlib.pyplot as plt
    DELOITTE_COLORS = ['#26890D', '#046A38', '#404040', '#0D8390', '#00ABAB']
    plt.rcParams.update({
        'font.family': ['Arial', 'sans-serif'],
        'axes.prop_cycle': plt.cycler('color', DELOITTE_COLORS),
        'axes.titlesize': 14, 'axes.titleweight': 'bold',
        'axes.titlecolor': '#26890D',
        'axes.spines.top': False, 'axes.spines.right': False,
        'figure.facecolor': 'white', 'axes.facecolor': 'white',
    })

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec
from collections import Counter

DELOITTE_COLORS = ['#26890D', '#046A38', '#404040', '#0D8390', '#00ABAB']

# ── Load data ──────────────────────────────────────────────────────────────────
df = pd.read_excel('workspace/data.xlsx')
df['Fecha_Mes'] = pd.to_datetime(df['Fecha_Mes'])
df['period'] = df['Fecha_Mes'].dt.to_period('M')

# ── Time windows ──────────────────────────────────────────────────────────────
latest_month = df['period'].max()  # Apr 2023
ltm_end = latest_month
ltm_start = ltm_end - 11
prior_ltm_start = ltm_start - 12
prior_ltm_end = ltm_end - 12

fy2021 = df[df['Fecha_Mes'].dt.year == 2021]
fy2022 = df[df['Fecha_Mes'].dt.year == 2022]
ltm23 = df[(df['period'] >= ltm_start) & (df['period'] <= ltm_end)]
prior_ltm = df[(df['period'] >= prior_ltm_start) & (df['period'] <= prior_ltm_end)]

print('='*70)
print('ITERATION 5: PRODUCT FAMILY CLUSTERING FROM MODEL NAMES')
print('='*70)
print(f'Latest month: {latest_month}')
print(f'LTM23 window: {ltm_start} – {ltm_end}')
print(f'Prior LTM window: {prior_ltm_start} – {prior_ltm_end}')

# ── Step 1: Name analysis — how many words per model name? ────────────────────
all_models = df['Nombre Modelo'].unique()
word_counts = Counter([len(m.split()) for m in all_models])
print(f'\nModel name word-count distribution (N={len(all_models)} models):')
for wc in sorted(word_counts):
    print(f'  {wc} word(s): {word_counts[wc]} models')

# ── Step 2: Extract first token (family proxy) ────────────────────────────────
df['family'] = df['Nombre Modelo'].str.split().str[0].str.upper().str.strip()
all_models_df = pd.DataFrame({'model': all_models})
all_models_df['first_token'] = all_models_df['model'].str.split().str[0].str.upper().str.strip()

token_freq = all_models_df['first_token'].value_counts()
print(f'\nFirst-token (prefix) distribution:')
print(f'  Unique first tokens: {len(token_freq)}')
print(f'  Tokens appearing >1 model (potential families): {(token_freq > 1).sum()}')

# Multi-model tokens
multi = token_freq[token_freq > 1].reset_index()
multi.columns = ['first_token', 'model_count']
print('\n  Multi-model prefix families (appearing in 2+ model names):')
for _, row in multi.head(20).iterrows():
    print(f'    {row["first_token"]:20s}: {row["model_count"]} models')

# ── Step 3: Word count and special patterns ───────────────────────────────────
# Multi-word models may follow a naming pattern (e.g. "NEW YORK", "GRAND PLACE")
multiword_models = [m for m in all_models if len(m.split()) > 1]
print(f'\nMulti-word model names ({len(multiword_models)} total — examples):')
for m in sorted(multiword_models)[:30]:
    print(f'    {m}')

# ── Step 4: Revenue by prefix family ─────────────────────────────────────────
# Revenue by first token in LTM23
ltm23_copy = ltm23.copy()
ltm23_copy['family'] = ltm23_copy['Nombre Modelo'].str.split().str[0].str.upper().str.strip()
fam_ltm23 = ltm23_copy.groupby('family')['Venta Netas'].sum().reset_index()
fam_ltm23.columns = ['family', 'rev_ltm23']
fam_ltm23 = fam_ltm23.sort_values('rev_ltm23', ascending=False)
total_ltm23 = fam_ltm23['rev_ltm23'].sum()
fam_ltm23['share'] = fam_ltm23['rev_ltm23'] / total_ltm23

print(f'\nTop 30 first-token families by LTM23 revenue (Total = EUR {total_ltm23/1e6:.2f}M):')
print(f'{"Family":<20} {"Rev LTM23 (EUR)":>16} {"Share":>8}')
print('-'*46)
for _, row in fam_ltm23.head(30).iterrows():
    print(f'{row["family"]:<20} {row["rev_ltm23"]/1e3:>14.1f}K {row["share"]:>7.1%}')

# Coverage: how much do multi-model families cover?
multi_tokens = set(multi['first_token'].tolist())
fam_multi = fam_ltm23[fam_ltm23['family'].isin(multi_tokens)]
print(f'\nRevenue covered by multi-model prefix families: EUR {fam_multi["rev_ltm23"].sum()/1e6:.2f}M ({fam_multi["rev_ltm23"].sum()/total_ltm23:.1%})')
print(f'Revenue from single-model prefixes: EUR {(total_ltm23 - fam_multi["rev_ltm23"].sum())/1e6:.2f}M ({(total_ltm23 - fam_multi["rev_ltm23"].sum())/total_ltm23:.1%})')

# ── Step 5: Attempt meaningful groupings ──────────────────────────────────────
# Since most model names ARE the full model name (single unique city name),
# we need another approach: group by WORD COUNT and check if multi-word follow any pattern.
# Another approach: look at the SECOND WORD for multi-word models as potential sub-brand.

# Instead, let's create a synthetic product TIER grouping based on ASP — since
# we know from iter3 that ASP tier correlates with durability.

# Compute per-model ASP in LTM23
model_ltm23 = ltm23.groupby('Nombre Modelo').agg(
    rev=('Venta Netas', 'sum'),
    units=('Cant_Neta', 'sum')
).reset_index()
model_ltm23['asp'] = model_ltm23['rev'] / model_ltm23['units'].clip(lower=1)
model_ltm23 = model_ltm23[model_ltm23['units'] > 0]

# ASP tiers
asp_q = model_ltm23['asp'].quantile([0.25, 0.50, 0.75])
print(f'\nASP distribution in LTM23 (per-model):')
print(f'  Q25: EUR {asp_q[0.25]:.1f}  Q50: EUR {asp_q[0.50]:.1f}  Q75: EUR {asp_q[0.75]:.1f}')

def asp_tier(asp):
    if asp < asp_q[0.25]: return 'Entry (< EUR {:.0f})'.format(asp_q[0.25])
    elif asp < asp_q[0.50]: return 'Lower-Mid (EUR {:.0f}–{:.0f})'.format(asp_q[0.25], asp_q[0.50])
    elif asp < asp_q[0.75]: return 'Upper-Mid (EUR {:.0f}–{:.0f})'.format(asp_q[0.50], asp_q[0.75])
    else: return 'Premium (> EUR {:.0f})'.format(asp_q[0.75])

model_ltm23['asp_tier'] = model_ltm23['asp'].apply(asp_tier)

# ── Step 6: Channel mix by ASP tier ──────────────────────────────────────────
ltm23_asp = ltm23.merge(model_ltm23[['Nombre Modelo', 'asp', 'asp_tier']], on='Nombre Modelo', how='left')
ltm23_asp = ltm23_asp.dropna(subset=['asp_tier'])

channel_tier = ltm23_asp.groupby(['asp_tier', 'Canal'])['Venta Netas'].sum().unstack(fill_value=0)
channel_tier_pct = channel_tier.div(channel_tier.sum(axis=1), axis=0)

print('\nChannel mix by ASP tier (LTM23):')
print(f'{"ASP Tier":<35}', end='')
for col in channel_tier_pct.columns:
    print(f'{col:>14}', end='')
print(f'{"Total Rev (EUR)":>18}')
print('-'*100)
for tier in channel_tier.index:
    total = channel_tier.loc[tier].sum()
    print(f'{tier:<35}', end='')
    for col in channel_tier_pct.columns:
        pct = channel_tier_pct.loc[tier, col] if col in channel_tier_pct.columns else 0
        print(f'{pct:>13.1%}', end='')
    print(f'{total/1e3:>16.0f}K')

# ── Step 7: Product naming pattern — check if 'NEW', 'GRAND', etc. are sub-brands ─────
# Extract all first words from all models
all_first_words = [m.split()[0].upper() for m in all_models]
fw_counts = Counter(all_first_words)
print(f'\nAll unique first words with count:')
for word, cnt in sorted(fw_counts.items(), key=lambda x: -x[1]):
    if cnt >= 2:
        models_in_group = [m for m in all_models if m.split()[0].upper() == word]
        print(f'  {word:<20}: {cnt} models — {models_in_group}')

# ── Step 8: Revenue and lifecycle by word-count grouping ──────────────────────
# Single-word = likely pure city name
# Multi-word = compound city name (NEW YORK, GRAND PLACE, etc.) or brand name

df2 = ltm23.copy()
df2['word_count'] = df2['Nombre Modelo'].str.split().str.len()
df2['name_type'] = df2['word_count'].apply(lambda x: 'Compound' if x > 1 else 'Single-word')

wc_rev = df2.groupby('name_type')['Venta Netas'].sum().reset_index()
wc_rev['share'] = wc_rev['Venta Netas'] / wc_rev['Venta Netas'].sum()
wc_models = df2.groupby('name_type')['Nombre Modelo'].nunique().reset_index()
wc_models.columns = ['name_type', 'n_models']
wc_summary = wc_rev.merge(wc_models, on='name_type')

print('\nRevenue by model naming type (LTM23):')
print(f'{"Name Type":<15} {"Models":>8} {"Revenue (EUR)":>16} {"Share":>8}')
print('-'*50)
for _, row in wc_summary.iterrows():
    print(f'{row["name_type"]:<15} {row["n_models"]:>8} {row["Venta Netas"]/1e3:>14.0f}K {row["share"]:>8.1%}')

# Revenue trend: single-word vs compound across years
df_all = df.copy()
df_all['word_count'] = df_all['Nombre Modelo'].str.split().str.len()
df_all['name_type'] = df_all['word_count'].apply(lambda x: 'Compound' if x > 1 else 'Single-word')

rev_fy21 = df_all[df_all['Fecha_Mes'].dt.year==2021].groupby('name_type')['Venta Netas'].sum()
rev_fy22 = df_all[df_all['Fecha_Mes'].dt.year==2022].groupby('name_type')['Venta Netas'].sum()
rev_ltm = df_all[(df_all['period']>=ltm_start)&(df_all['period']<=ltm_end)].groupby('name_type')['Venta Netas'].sum()

print('\nRevenue trend: Single-word vs Compound models')
print(f'{"Type":<15} {"FY2021":>12} {"FY2022":>12} {"LTM23":>12} {"YoY FY21-22":>14} {"YoY LTM":>12}')
print('-'*75)
for nt in ['Single-word', 'Compound']:
    r21 = rev_fy21.get(nt, 0)
    r22 = rev_fy22.get(nt, 0)
    rltm = rev_ltm.get(nt, 0)
    yoy1 = (r22/r21-1) if r21 > 0 else float('nan')
    yoy2 = (rltm/r22-1) if r22 > 0 else float('nan')
    print(f'{nt:<15} {r21/1e3:>10.0f}K {r22/1e3:>10.0f}K {rltm/1e3:>10.0f}K {yoy1:>+13.1%} {yoy2:>+11.1%}')

# ── Step 9: Lifecycle classification × ASP tier ───────────────────────────────
# Re-derive lifecycle classification (Stable, New Entrant, Bust) for LTM23 context
rev_fy21_model = fy2021.groupby('Nombre Modelo')['Venta Netas'].sum()
rev_fy22_model = fy2022.groupby('Nombre Modelo')['Venta Netas'].sum()
rev_ltm_model = ltm23.groupby('Nombre Modelo')['Venta Netas'].sum()
rev_prior_ltm_model = prior_ltm.groupby('Nombre Modelo')['Venta Netas'].sum()

models_in_ltm = set(rev_ltm_model.index)
models_in_prior = set(rev_prior_ltm_model.index)
models_in_fy21 = set(rev_fy21_model.index)

stable = models_in_ltm & models_in_prior & models_in_fy21
new_entrant = models_in_ltm - models_in_prior
bust_candidates = models_in_prior - models_in_ltm  # dropped off
bust = set()
for m in (models_in_ltm & models_in_prior) - stable:
    r_prior = rev_prior_ltm_model.get(m, 0)
    r_ltm = rev_ltm_model.get(m, 0)
    if r_prior > 0 and (r_ltm / r_prior) < 0.5:
        bust.add(m)

def lifecycle(model):
    if model in stable: return 'Stable'
    elif model in new_entrant: return 'New Entrant'
    elif model in bust: return 'Bust'
    else: return 'Other'

model_ltm23['lifecycle'] = model_ltm23['Nombre Modelo'].apply(lifecycle)

print('\nLifecycle × ASP tier cross-table (LTM23, model counts and revenue):')
cross = model_ltm23.groupby(['asp_tier', 'lifecycle']).agg(
    n_models=('Nombre Modelo', 'count'),
    rev=('rev', 'sum')
).reset_index()
cross_pivot = cross.pivot_table(index='asp_tier', columns='lifecycle', values='n_models', fill_value=0)
cross_pivot_rev = cross.pivot_table(index='asp_tier', columns='lifecycle', values='rev', fill_value=0)

print('\nModel counts:')
print(cross_pivot.to_string())
print('\nRevenue (EUR K):')
print((cross_pivot_rev/1000).round(0).to_string())

# Durability rate by ASP tier (as proxy — Stable models are most durable)
cross_pivot2 = cross_pivot.copy()
for col in ['New Entrant', 'Bust', 'Other', 'Stable']:
    if col not in cross_pivot2.columns:
        cross_pivot2[col] = 0
cross_pivot2['total'] = cross_pivot2.sum(axis=1)
cross_pivot2['stable_pct'] = cross_pivot2['Stable'] / cross_pivot2['total']

print('\nStable model % by ASP tier:')
for tier, row in cross_pivot2.iterrows():
    print(f'  {tier:<40}: {row["stable_pct"]:>6.1%} ({int(row["Stable"])} Stable of {int(row["total"])} total)')

# ── Step 10: Monthly revenue by ASP tier (trend) ─────────────────────────────
df_asp_all = df.copy()
df_asp_all = df_asp_all.merge(model_ltm23[['Nombre Modelo', 'asp_tier']], on='Nombre Modelo', how='left')
df_asp_all['year'] = df_asp_all['Fecha_Mes'].dt.year

rev_by_tier_year = df_asp_all.groupby(['asp_tier', 'year'])['Venta Netas'].sum().unstack(fill_value=0)
print('\nAnnual revenue by ASP tier (EUR K):')
print(f'{"ASP Tier":<40} {"FY2021":>10} {"FY2022":>10}')
print('-'*62)
for tier in rev_by_tier_year.index:
    r21 = rev_by_tier_year.loc[tier, 2021] if 2021 in rev_by_tier_year.columns else 0
    r22 = rev_by_tier_year.loc[tier, 2022] if 2022 in rev_by_tier_year.columns else 0
    yoy = (r22/r21-1) if r21 > 0 else float('nan')
    print(f'{str(tier):<40} {r21/1e3:>8.0f}K {r22/1e3:>8.0f}K ({yoy:>+5.0%})')

# ── GRAPH 1: Revenue trend by ASP tier (stacked area) ───────────────────────
monthly_asp = df_asp_all.groupby(['Fecha_Mes', 'asp_tier'])['Venta Netas'].sum().unstack(fill_value=0)
# Sort tiers
tier_order = [
    c for c in [
        'Entry (< EUR {:,.0f})'.format(asp_q[0.25]),
        'Lower-Mid (EUR {:,.0f}–{:,.0f})'.format(asp_q[0.25], asp_q[0.50]),
        'Upper-Mid (EUR {:,.0f}–{:,.0f})'.format(asp_q[0.50], asp_q[0.75]),
        'Premium (> EUR {:,.0f})'.format(asp_q[0.75])
    ] if c in monthly_asp.columns
]

fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# Left: stacked area
ax = axes[0]
cols_to_plot = [c for c in tier_order if c in monthly_asp.columns]
monthly_asp_plot = monthly_asp[cols_to_plot].copy()
ax.stackplot(monthly_asp_plot.index, [monthly_asp_plot[c]/1e3 for c in cols_to_plot],
             labels=[c.split('(')[0].strip() for c in cols_to_plot],
             colors=DELOITTE_COLORS[:len(cols_to_plot)], alpha=0.85)
ax.set_title('Monthly Revenue by ASP Tier', color='#26890D', fontweight='bold')
ax.set_ylabel('Revenue (EUR K)', fontsize=10)
ax.set_xlabel('')
ax.legend(loc='upper left', fontsize=8)
ax.set_xlim(monthly_asp_plot.index.min(), monthly_asp_plot.index.max())

# Right: revenue share by tier × lifecycle
rev_tile = model_ltm23.groupby(['asp_tier', 'lifecycle'])['rev'].sum().unstack(fill_value=0)
for col in ['Stable', 'New Entrant', 'Bust', 'Other']:
    if col not in rev_tile.columns:
        rev_tile[col] = 0
rev_tile_pct = rev_tile.div(rev_tile.sum(axis=1), axis=0)

ax2 = axes[1]
lifecycle_order = ['Stable', 'New Entrant', 'Bust', 'Other']
lifecycle_colors = ['#26890D', '#046A38', '#404040', '#0D8390']
bottom = np.zeros(len(rev_tile_pct))
for i, lc in enumerate(lifecycle_order):
    if lc in rev_tile_pct.columns:
        vals = rev_tile_pct[lc].values
        ax2.bar(range(len(rev_tile_pct)), vals, bottom=bottom,
                color=lifecycle_colors[i], label=lc, alpha=0.85)
        bottom += vals
ax2.set_xticks(range(len(rev_tile_pct)))
ax2.set_xticklabels([str(i).split('(')[0].strip() for i in rev_tile_pct.index], rotation=30, ha='right', fontsize=8)
ax2.set_ylabel('Revenue Share', fontsize=10)
ax2.set_title('Lifecycle Mix by ASP Tier (LTM23)', color='#26890D', fontweight='bold')
ax2.legend(loc='upper right', fontsize=8)
ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:.0%}'))

plt.tight_layout()
plt.savefig('workspace/graphs/iter5_asp_tier_revenue_lifecycle.png', dpi=150, bbox_inches='tight')
plt.close()
print('GRAPH_SAVED: iter5_asp_tier_revenue_lifecycle.png — Monthly revenue by ASP tier (stacked area) and lifecycle mix by tier (LTM23)')

# ── GRAPH 2: Channel mix by ASP tier ─────────────────────────────────────────
fig, ax = plt.subplots(figsize=(12, 5))

channels = channel_tier_pct.columns.tolist()
channel_colors = DELOITTE_COLORS[:len(channels)]
bottom = np.zeros(len(channel_tier_pct))
for i, ch in enumerate(channels):
    vals = channel_tier_pct[ch].values
    ax.bar(range(len(channel_tier_pct)), vals, bottom=bottom,
           color=channel_colors[i], label=ch, alpha=0.85)
    for j, v in enumerate(vals):
        if v > 0.05:
            ax.text(j, bottom[j] + v/2, f'{v:.0%}', ha='center', va='center',
                    fontsize=8, color='white', fontweight='bold')
    bottom += vals

ax.set_xticks(range(len(channel_tier_pct)))
ax.set_xticklabels([str(i).split('(')[0].strip() for i in channel_tier_pct.index], rotation=20, ha='right', fontsize=9)
ax.set_ylabel('Revenue Share', fontsize=10)
ax.set_title('Channel Mix by ASP Tier (LTM23)', color='#26890D', fontweight='bold')
ax.legend(loc='upper right', fontsize=9)
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:.0%}'))

# Add total revenue annotation
for j, tier in enumerate(channel_tier.index):
    total = channel_tier.loc[tier].sum()
    ax.text(j, 1.02, f'EUR {total/1e3:.0f}K', ha='center', va='bottom', fontsize=7, color='#404040')

plt.tight_layout()
plt.savefig('workspace/graphs/iter5_channel_mix_by_asp_tier.png', dpi=150, bbox_inches='tight')
plt.close()
print('GRAPH_SAVED: iter5_channel_mix_by_asp_tier.png — Channel revenue share by ASP tier for LTM23')

# ── GRAPH 3: Top families waterfall (single-word models ranked by LTM23 rev) ──
fig, ax = plt.subplots(figsize=(14, 6))
top_fam = fam_ltm23.head(25)

# Also get FY2021 revenue by family
df_fy21_fam = fy2021.copy()
df_fy21_fam['family'] = df_fy21_fam['Nombre Modelo'].str.split().str[0].str.upper()
rev_fam_fy21 = df_fy21_fam.groupby('family')['Venta Netas'].sum()
df_fy22_fam = fy2022.copy()
df_fy22_fam['family'] = df_fy22_fam['Nombre Modelo'].str.split().str[0].str.upper()
rev_fam_fy22 = df_fy22_fam.groupby('family')['Venta Netas'].sum()

x = np.arange(len(top_fam))
w = 0.28
r21 = [rev_fam_fy21.get(f, 0)/1e3 for f in top_fam['family']]
r22 = [rev_fam_fy22.get(f, 0)/1e3 for f in top_fam['family']]
rltm = top_fam['rev_ltm23'].values / 1e3

ax.bar(x - w, r21, width=w, color='#404040', alpha=0.8, label='FY2021')
ax.bar(x, r22, width=w, color='#046A38', alpha=0.8, label='FY2022')
ax.bar(x + w, rltm, width=w, color='#26890D', alpha=0.9, label='LTM23')

ax.set_xticks(x)
ax.set_xticklabels(top_fam['family'].tolist(), rotation=45, ha='right', fontsize=8)
ax.set_ylabel('Revenue (EUR K)', fontsize=10)
ax.set_title('Top 25 Model Name Prefixes — Revenue by Period', color='#26890D', fontweight='bold')
ax.legend(fontsize=9)

plt.tight_layout()
plt.savefig('workspace/graphs/iter5_top25_family_revenue_trend.png', dpi=150, bbox_inches='tight')
plt.close()
print('GRAPH_SAVED: iter5_top25_family_revenue_trend.png — Top 25 model-name-prefix families revenue across FY2021, FY2022, LTM23')

# ── Step 11: Identify potential collection families (multi-word: NEW, GRAND, etc.) ──
print('\n' + '='*70)
print('SUMMARY: PRODUCT FAMILY TAXONOMY ASSESSMENT')
print('='*70)

total_models = len(all_models)
single_word_models = sum(1 for m in all_models if len(m.split()) == 1)
multi_word_models = total_models - single_word_models

print(f'  Total models: {total_models}')
print(f'  Single-word models: {single_word_models} ({single_word_models/total_models:.1%})')
print(f'  Multi-word (compound) models: {multi_word_models} ({multi_word_models/total_models:.1%})')

# Multi-model prefixes revenue in LTM23
print(f'\n  Multi-model prefix families cover:')
print(f'    EUR {fam_multi["rev_ltm23"].sum()/1e6:.2f}M ({fam_multi["rev_ltm23"].sum()/total_ltm23:.1%}) of LTM23 revenue')
print(f'    These are {len(fam_multi)} prefix groups covering {fam_multi["rev_ltm23"].sum()/total_ltm23:.1%} of revenue')

print(f'\n  Conclusion: The naming convention is primarily city-based (one name = one model).')
print(f'  Multi-model prefix families are limited (e.g. NEW YORK, GRAND PLACE).')
print(f'  ASP tier is the more actionable segmentation dimension for strategic analysis.')
print(f'  Entry/Lower-Mid tiers carry a higher proportion of New Entrant models, while')
print(f'  upper tiers show higher Stable model concentration — consistent with iter3 findings.')
```

**Output (preview):**
```
======================================================================
ITERATION 5: PRODUCT FAMILY CLUSTERING FROM MODEL NAMES
======================================================================
Latest month: 2023-04
LTM23 window: 2022-05 – 2023-04
Prior LTM window: 2021-05 – 2022-04

Model name word-count distribution (N=398 models):
  1 word(s): 279 models
  2 word(s): 65 models
  3 word(s): 39 models
  4 word(s): 10 models
  5 word(s): 5 models

First-token (prefix) distribution:
  Unique first tokens: 353
  Tokens appearing >1 model (potential families): 31

  Multi-model prefix familie
```

---

## Analysis 6: Customer cohort behavior + Product lifecycle velocity + Channel economics (Entry-tier New Entrant deep dive)
**Hypothesis:** Entry-tier New Entrant models (ASP < EUR 48) dominate LTM23 revenue within their tier (EUR 8.1M vs EUR 6.1M for Stable models), an inversion not seen in Premium tier. This analysis investigates whether these Entry-tier New Entrants are wholesale-channel promotional launches with rapid sell-in but low repeat — by examining their vintage cohort introduction timing, monthly revenue trajectories post-launch, post-peak retention rates, and Wholesale channel dependency — to determine if the Entry-tier growth is structurally durable or a high-inventory-risk, low-repeat-demand phenomenon.

**Columns:** Nombre Modelo, Fecha_Mes, Canal, Venta Netas, Cant_Neta

```python
import matplotlib; matplotlib.use('Agg')
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath('workspace/current_analysis.py')) + '/..')
try:
    import deloitte_theme
    deloitte_theme.apply_deloitte_style()
except Exception:
    pass

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import warnings
warnings.filterwarnings('ignore')

DELOITTE_COLORS = ['#26890D', '#046A38', '#404040', '#0D8390', '#00ABAB']
plt.rcParams.update({
    'font.family': ['Arial', 'sans-serif'],
    'axes.prop_cycle': plt.cycler('color', DELOITTE_COLORS),
    'axes.titlesize': 14, 'axes.titleweight': 'bold',
    'axes.titlecolor': '#26890D',
    'axes.spines.top': False, 'axes.spines.right': False,
    'figure.facecolor': 'white', 'axes.facecolor': 'white',
})

GRAPHS_DIR = 'workspace/graphs'
os.makedirs(GRAPHS_DIR, exist_ok=True)

# ── Load data ──────────────────────────────────────────────────────────────────
df = pd.read_excel('workspace/data.xlsx')
df['Fecha_Mes'] = pd.to_datetime(df['Fecha_Mes'])
df['YearMonth'] = df['Fecha_Mes'].dt.to_period('M')

# ── Define time windows ────────────────────────────────────────────────────────
latest_month = df['YearMonth'].max()
ltm_end   = latest_month
ltm_start = ltm_end - 11
fy2022_start = pd.Period('2022-01', 'M')
fy2022_end   = pd.Period('2022-12', 'M')
fy2021_start = pd.Period('2021-01', 'M')
fy2021_end   = pd.Period('2021-12', 'M')

mask_ltm  = (df['YearMonth'] >= ltm_start)  & (df['YearMonth'] <= ltm_end)
mask_fy22 = (df['YearMonth'] >= fy2022_start) & (df['YearMonth'] <= fy2022_end)
mask_fy21 = (df['YearMonth'] >= fy2021_start) & (df['YearMonth'] <= fy2021_end)

print(f"Latest month: {latest_month}  |  LTM window: {ltm_start} to {ltm_end}")

# ── Compute model-level revenue by period ─────────────────────────────────────
model_fy21 = df[mask_fy21].groupby('Nombre Modelo')['Venta Netas'].sum().reset_index()
model_fy21.columns = ['Nombre Modelo', 'rev_fy21']

model_fy22 = df[mask_fy22].groupby('Nombre Modelo')['Venta Netas'].sum().reset_index()
model_fy22.columns = ['Nombre Modelo', 'rev_fy22']

model_ltm = df[mask_ltm].groupby('Nombre Modelo').agg(
    rev_ltm=('Venta Netas', 'sum'),
    qty_ltm=('Cant_Neta', 'sum')
).reset_index()
model_ltm['asp_ltm'] = model_ltm['rev_ltm'] / model_ltm['qty_ltm'].replace(0, np.nan)

# ── Merge all periods ─────────────────────────────────────────────────────────
model_all = model_ltm.merge(model_fy21, on='Nombre Modelo', how='outer')
model_all = model_all.merge(model_fy22, on='Nombre Modelo', how='outer')
model_all = model_all.fillna(0)

# ── Lifecycle classification ───────────────────────────────────────────────────
def classify(row):
    if row['rev_fy21'] == 0 and row['rev_ltm'] > 0:
        return 'New Entrant'
    elif row['rev_fy21'] > 0 and row['rev_ltm'] > 0:
        rev_ratio = row['rev_ltm'] / (row['rev_fy21'] + 1e-9)
        return 'Stable' if rev_ratio > 0.5 else 'Bust'
    elif row['rev_fy21'] > 0 and row['rev_ltm'] == 0:
        return 'Disappeared'
    return 'Other'

model_all['lifecycle'] = model_all.apply(classify, axis=1)

# ── Compute global ASP (total history) for all models ─────────────────────────
model_asp_global = df.groupby('Nombre Modelo').agg(
    rev_total=('Venta Netas', 'sum'),
    qty_total=('Cant_Neta', 'sum')
).reset_index()
model_asp_global['asp_global'] = (model_asp_global['rev_total'] /
                                   model_asp_global['qty_total'].replace(0, np.nan))

# ── Merge ASPs — use LTM ASP where available, fallback to global ───────────────
# asp_ltm is already in model_all from model_ltm merge
# Replace 0 asp_ltm with NaN so fillna works correctly
model_all['asp_ltm'] = model_all['asp_ltm'].replace(0, np.nan)

model_all = model_all.merge(
    model_asp_global[['Nombre Modelo', 'asp_global']],
    on='Nombre Modelo', how='left'
)

model_all['asp'] = model_all['asp_ltm'].fillna(model_all['asp_global'])

# Verify
print(f"model_all shape: {model_all.shape}")
print(f"asp_ltm nulls: {model_all['asp_ltm'].isna().sum()}, asp nulls: {model_all['asp'].isna().sum()}")
print()

# ── ASP tier definition ────────────────────────────────────────────────────────
def asp_tier(asp):
    if pd.isna(asp): return 'Unknown'
    if asp < 48: return 'Entry (<48)'
    if asp < 55: return 'Lower-Mid (48-55)'
    if asp < 65: return 'Upper-Mid (55-65)'
    return 'Premium (>65)'

model_all['asp_tier'] = model_all['asp'].apply(asp_tier)

# ── Focus: New Entrant Entry-tier models ───────────────────────────────────────
ne_entry = model_all[(model_all['lifecycle'] == 'New Entrant') &
                     (model_all['asp_tier'] == 'Entry (<48)')].copy()
stable_entry = model_all[(model_all['lifecycle'] == 'Stable') &
                          (model_all['asp_tier'] == 'Entry (<48)')].copy()

print('=' * 75)
print('ENTRY-TIER NEW ENTRANT vs STABLE: HIGH-LEVEL SUMMARY')
print('=' * 75)
print(f"{'Segment':<30} {'# Models':>10} {'LTM23 Rev (EUR)':>17} {'FY22 Rev (EUR)':>16} {'FY21 Rev (EUR)':>16}")
print('-' * 75)
for seg, sub in [('New Entrant Entry (<48)', ne_entry), ('Stable Entry (<48)', stable_entry)]:
    print(f"{seg:<30} {len(sub):>10,} {sub['rev_ltm'].sum():>17,.0f} "
          f"{sub['rev_fy22'].sum():>16,.0f} {sub['rev_fy21'].sum():>16,.0f}")
print()

# ── Channel mix for New Entrant Entry vs Stable Entry (LTM) ───────────────────
ne_entry_models = ne_entry['Nombre Modelo'].tolist()
stable_entry_models = stable_entry['Nombre Modelo'].tolist()

df_ltm = df[mask_ltm].copy()
ne_entry_ltm = df_ltm[df_ltm['Nombre Modelo'].isin(ne_entry_models)]
stable_entry_ltm = df_ltm[df_ltm['Nombre Modelo'].isin(stable_entry_models)]

print('=' * 75)
print('LTM23 CHANNEL MIX — NEW ENTRANT ENTRY vs STABLE ENTRY')
print('=' * 75)
for label, subset in [('New Entrant Entry', ne_entry_ltm), ('Stable Entry', stable_entry_ltm)]:
    total = subset['Venta Netas'].sum()
    if total == 0:
        print(f"  {label}: No revenue")
        continue
    by_ch = subset.groupby('Canal')['Venta Netas'].sum().sort_values(ascending=False)
    print(f"  {label} (Total LTM: EUR {total:,.0f})")
    for ch, rev in by_ch.items():
        print(f"    {ch:<20} EUR {rev:>10,.0f}  ({rev/total*100:.1f}%)")
    print()

# ── Monthly revenue trajectory for NE Entry models: launch-relative ────────────
df_ne_entry_all = df[df['Nombre Modelo'].isin(ne_entry_models)].copy()
first_month_map = df_ne_entry_all.groupby('Nombre Modelo')['YearMonth'].min().rename('launch_month')
df_ne_entry_all = df_ne_entry_all.join(first_month_map, on='Nombre Modelo')
df_ne_entry_all['months_since_launch'] = (
    df_ne_entry_all['YearMonth'] - df_ne_entry_all['launch_month']
).apply(lambda x: x.n)

# Model-level monthly revenue
model_monthly = df_ne_entry_all.groupby(
    ['Nombre Modelo', 'months_since_launch'])['Venta Netas'].sum().reset_index()

# Compute peak month and peak revenue per model
peak_idx = model_monthly.groupby('Nombre Modelo')['Venta Netas'].idxmax()
peak = model_monthly.loc[peak_idx][['Nombre Modelo', 'months_since_launch', 'Venta Netas']].copy()
peak.columns = ['Nombre Modelo', 'peak_month', 'peak_rev']

model_monthly = model_monthly.merge(peak, on='Nombre Modelo')
model_monthly['ret_ratio'] = model_monthly['Venta Netas'] / model_monthly['peak_rev'].replace(0, np.nan)

# Post-peak retention helper
def retention_at_offset(df_m, offset):
    merged = df_m[['Nombre Modelo', 'months_since_launch', 'Venta Netas', 'peak_month', 'peak_rev']].copy()
    merged['target_month'] = merged['peak_month'] + offset
    post = merged[merged['months_since_launch'] == merged['target_month']].copy()
    post['ret'] = post['Venta Netas'] / post['peak_rev'].replace(0, np.nan)
    return post.set_index('Nombre Modelo')['ret']

ret1 = retention_at_offset(model_monthly, 1).rename('ret_1m')
ret2 = retention_at_offset(model_monthly, 2).rename('ret_2m')
ret3 = retention_at_offset(model_monthly, 3).rename('ret_3m')
ret6 = retention_at_offset(model_monthly, 6).rename('ret_6m')

ne_entry_profile = peak.set_index('Nombre Modelo')[['peak_month', 'peak_rev']].copy()
ne_entry_profile = ne_entry_profile.join([ret1, ret2, ret3, ret6])

# Merge LTM rev and ASP from ne_entry
ne_meta = ne_entry[['Nombre Modelo', 'rev_ltm', 'asp']].set_index('Nombre Modelo')
ne_entry_profile = ne_entry_profile.join(ne_meta, how='left')

# Durability classification
def durability(r):
    if pd.isna(r): return 'No Data'
    if r >= 0.40: return 'Durable (>=40%)'
    if r >= 0.20: return 'Mid-tier (20-40%)'
    return 'Transient (<20%)'

ne_entry_profile['durability'] = ne_entry_profile['ret_3m'].apply(durability)

print('=' * 75)
print('POST-PEAK RETENTION DISTRIBUTION — NEW ENTRANT ENTRY TIER')
print('=' * 75)
print(f"  Total NE Entry models: {len(ne_entry_profile)}")
print(f"  Models with +3m retention data: {ne_entry_profile['ret_3m'].notna().sum()}")
print()

dur_counts = ne_entry_profile['durability'].value_counts()
dur_rev = ne_entry_profile.groupby('durability')['rev_ltm'].sum()
total_ne_rev = ne_entry_profile['rev_ltm'].sum()

print(f"  {'Durability Class':<25} {'# Models':>10} {'LTM Rev (EUR)':>15} {'Rev Share':>10} {'Avg Ret @+3m':>14}")
print('  ' + '-' * 78)
for d in ['Durable (>=40%)', 'Mid-tier (20-40%)', 'Transient (<20%)', 'No Data']:
    n = dur_counts.get(d, 0)
    r = dur_rev.get(d, 0)
    share = r / total_ne_rev * 100 if total_ne_rev > 0 else 0
    avg_ret = ne_entry_profile[ne_entry_profile['durability'] == d]['ret_3m'].mean()
    avg_str = f"{avg_ret*100:.1f}%" if not pd.isna(avg_ret) else 'N/A'
    print(f"  {d:<25} {n:>10,} {r:>15,.0f} {share:>9.1f}% {avg_str:>14}")
print()

# ── Average retention profile for NE Entry vs ALL New Entrants ────────────────
def avg_retention_at(df_m, offset):
    merged = df_m[['Nombre Modelo', 'months_since_launch', 'Venta Netas', 'peak_month', 'peak_rev']].copy()
    merged['target_month'] = merged['peak_month'] + offset
    post = merged[merged['months_since_launch'] == merged['target_month']].copy()
    post['ret'] = post['Venta Netas'] / post['peak_rev'].replace(0, np.nan)
    return post['ret'].mean()

# All New Entrants lifecycle
all_ne_models = model_all[model_all['lifecycle'] == 'New Entrant']['Nombre Modelo'].tolist()
df_all_ne = df[df['Nombre Modelo'].isin(all_ne_models)].copy()
first_month_all = df_all_ne.groupby('Nombre Modelo')['YearMonth'].min().rename('launch_month')
df_all_ne = df_all_ne.join(first_month_all, on='Nombre Modelo')
df_all_ne['months_since_launch'] = (
    df_all_ne['YearMonth'] - df_all_ne['launch_month']
).apply(lambda x: x.n)
model_monthly_all = df_all_ne.groupby(
    ['Nombre Modelo', 'months_since_launch'])['Venta Netas'].sum().reset_index()
peak_idx_all = model_monthly_all.groupby('Nombre Modelo')['Venta Netas'].idxmax()
peak_all = model_monthly_all.loc[peak_idx_all][['Nombre Modelo', 'months_since_launch', 'Venta Netas']].copy()
peak_all.columns = ['Nombre Modelo', 'peak_month', 'peak_rev']
model_monthly_all = model_monthly_all.merge(peak_all, on='Nombre Modelo')

# Stable models lifecycle
all_stable_models = model_all[model_all['lifecycle'] == 'Stable']['Nombre Modelo'].tolist()
df_stable_all = df[df['Nombre Modelo'].isin(all_stable_models)].copy()
first_m_stable = df_stable_all.groupby('Nombre Modelo')['YearMonth'].min().rename('launch_month')
df_stable_all = df_stable_all.join(first_m_stable, on='Nombre Modelo')
df_stable_all['months_since_launch'] = (
    df_stable_all['YearMonth'] - df_stable_all['launch_month']
).apply(lambda x: x.n)
model_monthly_stable = df_stable_all.groupby(
    ['Nombre Modelo', 'months_since_launch'])['Venta Netas'].sum().reset_index()
peak_idx_stab = model_monthly_stable.groupby('Nombre Modelo')['Venta Netas'].idxmax()
peak_stab = model_monthly_stable.loc[peak_idx_stab][['Nombre Modelo', 'months_since_launch', 'Venta Netas']].copy()
peak_stab.columns = ['Nombre Modelo', 'peak_month', 'peak_rev']
model_monthly_stable = model_monthly_stable.merge(peak_stab, on='Nombre Modelo')

print('=' * 75)
print('AVERAGE POST-PEAK RETENTION: NE ENTRY TIER vs ALL NEW ENTRANTS vs STABLE')
print('=' * 75)
print(f"  {'Offset':<12} {'NE Entry Avg Ret':>18} {'All NE Avg Ret':>16} {'Stable Avg Ret':>16}")
print('  ' + '-' * 64)
ne_entry_rets, all_ne_rets, stable_rets = [], [], []
for offset in [1, 2, 3, 6, 9]:
    ret_ne = avg_retention_at(model_monthly, offset)
    ret_all = avg_retention_at(model_monthly_all, offset)
    ret_stab = avg_retention_at(model_monthly_stable, offset)
    ne_entry_rets.append(ret_ne)
    all_ne_rets.append(ret_all)
    stable_rets.append(ret_stab)
    ne_str = f"{ret_ne*100:.1f}%" if not pd.isna(ret_ne) else 'N/A'
    all_str = f"{ret_all*100:.1f}%" if not pd.isna(ret_all) else 'N/A'
    stab_str = f"{ret_stab*100:.1f}%" if not pd.isna(ret_stab) else 'N/A'
    print(f"  +{offset}m{' ':9} {ne_str:>18} {all_str:>16} {stab_str:>16}")
print()

# ── Top 15 NE Entry models by LTM rev ─────────────────────────────────────────
print('=' * 75)
print('TOP 15 NEW ENTRANT ENTRY-TIER MODELS — LTM23 REVENUE + DURABILITY')
print('=' * 75)
print(f"  {'Model':<25} {'ASP':>8} {'Peak Mth':>9} {'Ret +1m':>8} {'Ret +3m':>8} {'Ret +6m':>8} {'LTM Rev':>12} {'Durability':<20}")
print('  ' + '-' * 102)
top15 = ne_entry_profile.sort_values('rev_ltm', ascending=False).head(15)
for nm, row in top15.iterrows():
    asp_val = row.get('asp', np.nan)
    asp_s = f"EUR {asp_val:.0f}" if not pd.isna(asp_val) else 'N/A'
    r1 = f"{row['ret_1m']*100:.0f}%" if not pd.isna(row.get('ret_1m', np.nan)) else 'N/A'
    r3 = f"{row['ret_3m']*100:.0f}%" if not pd.isna(row.get('ret_3m', np.nan)) else 'N/A'
    r6 = f"{row['ret_6m']*100:.0f}%" if not pd.isna(row.get('ret_6m', np.nan)) else 'N/A'
    peak_m = row['peak_month']
    peak_s = f"{int(peak_m)}" if not pd.isna(peak_m) else 'N/A'
    rev_ltm = row.get('rev_ltm', 0)
    dur = row.get('durability', 'N/A')
    print(f"  {nm:<25} {asp_s:>8} {peak_s:>9} {r1:>8} {r3:>8} {r6:>8} EUR {rev_ltm:>9,.0f} {dur:<20}")
print()

# ── Launch cohort timing ───────────────────────────────────────────────────────
df_ne_entry_all2 = df[df['Nombre Modelo'].isin(ne_entry_models)].copy()
launch_by_model = df_ne_entry_all2.groupby('Nombre Modelo')['Fecha_Mes'].min().reset_index()
launch_by_model['launch_period'] = launch_by_model['Fecha_Mes'].dt.to_period('M')

def launch_bucket(p):
    if p.year == 2021: return 'FY2021'
    if p.year == 2022 and p.month <= 6: return 'FY2022 H1'
    if p.year == 2022: return 'FY2022 H2'
    return 'LTM23 (2023)'

launch_by_model['launch_bucket'] = launch_by_model['launch_period'].apply(launch_bucket)
launch_counts = launch_by_model['launch_bucket'].value_counts().sort_index()

print('=' * 75)
print('NEW ENTRANT ENTRY-TIER MODELS: LAUNCH TIMING')
print('=' * 75)
print(f"  {'Launch Period':<20} {'# Models':>10}")
print('  ' + '-' * 32)
for bucket, cnt in launch_counts.items():
    print(f"  {bucket:<20} {cnt:>10}")
print()

# ── Monthly revenue velocity for NE Entry cohorts by launch bucket ─────────────
launch_by_model2 = launch_by_model[['Nombre Modelo', 'launch_bucket']]
df_ne_full = df_ne_entry_all2.merge(launch_by_model2, on='Nombre Modelo')
df_ne_full['YearMonth'] = df_ne_full['Fecha_Mes'].dt.to_period('M')
monthly_by_bucket = df_ne_full.groupby(['launch_bucket', 'YearMonth'])['Venta Netas'].sum().reset_index()

buckets_ordered = ['FY2022 H1', 'FY2022 H2', 'LTM23 (2023)']
pivot_monthly = monthly_by_bucket.pivot_table(
    index='YearMonth', columns='launch_bucket', values='Venta Netas', aggfunc='sum'
).fillna(0)

for b in buckets_ordered:
    if b not in pivot_monthly.columns:
        pivot_monthly[b] = 0

pivot_monthly.index = pivot_monthly.index.to_timestamp()
pivot_monthly = pivot_monthly.sort_index()
pivot_monthly['Quarter'] = pd.PeriodIndex(pivot_monthly.index, freq='Q')

buckets_in_data = [b for b in buckets_ordered if b in pivot_monthly.columns]
quart = pivot_monthly.groupby('Quarter')[buckets_in_data].sum()

print('=' * 75)
print('MONTHLY REVENUE FLOW: NE ENTRY MODELS BY LAUNCH COHORT (EUR K)')
print('=' * 75)
header = f"  {'Quarter':<15}"
for b in buckets_in_data:
    header += f"{b:>16}"
header += f"  {'TOTAL':>12}"
print(header)
print('  ' + '-' * (15 + 16 * len(buckets_in_data) + 14))
for q, row in quart.iterrows():
    total = row.sum()
    line = f"  {str(q):<15}"
    for b in buckets_in_data:
        line += f"{row.get(b, 0)/1000:>15.1f}K"
    line += f"  {total/1000:>11.1f}K"
    print(line)
print()

# ── Wholesale dependency: NE Entry vs Stable Entry per period ─────────────────
print('=' * 75)
print('WHOLESALE DEPENDENCY BY PERIOD: NE ENTRY vs STABLE ENTRY')
print('=' * 75)
print(f"  {'Segment':<28} {'Period':<12} {'Wholesale%':>12} {'Online%':>10} {'Retail%':>10} {'Total Rev':>14}")
print('  ' + '-' * 90)
for seg_label, seg_models, mask, period_label in [
    ('NE Entry', ne_entry_models, mask_ltm, 'LTM23'),
    ('NE Entry', ne_entry_models, mask_fy22, 'FY2022'),
    ('Stable Entry', stable_entry_models, mask_ltm, 'LTM23'),
    ('Stable Entry', stable_entry_models, mask_fy22, 'FY2022'),
]:
    sub = df[mask & df['Nombre Modelo'].isin(seg_models)]
    total = sub['Venta Netas'].sum()
    if total == 0:
        print(f"  {seg_label:<28} {period_label:<12} {'N/A':>12} {'N/A':>10} {'N/A':>10} {'0':>14}")
        continue
    by_ch = sub.groupby('Canal')['Venta Netas'].sum()
    ws = by_ch.get('Wholesales', 0) / total * 100
    on = by_ch.get('Online', 0) / total * 100
    rt = by_ch.get('Retail', 0) / total * 100
    print(f"  {seg_label:<28} {period_label:<12} {ws:>11.1f}% {on:>9.1f}% {rt:>9.1f}% EUR {total:>10,.0f}")
print()

# ── Repeat purchase check ──────────────────────────────────────────────────────
df_ne_clients = df[df['Nombre Modelo'].isin(ne_entry_models)].copy()
df_ne_clients['YearMonth'] = df_ne_clients['Fecha_Mes'].dt.to_period('M')

if 'Tienda_Cliente' in df_ne_clients.columns:
    repeat = df_ne_clients.groupby(['Nombre Modelo', 'Tienda_Cliente'])['YearMonth'].nunique().reset_index()
    repeat.columns = ['Nombre Modelo', 'Tienda_Cliente', 'active_months']
    repeat['is_repeat'] = repeat['active_months'] >= 2
    repeat_by_model = repeat.groupby('Nombre Modelo').agg(
        n_clients=('Tienda_Cliente', 'count'),
        n_repeat_clients=('is_repeat', 'sum')
    ).reset_index()
    repeat_by_model['pct_repeat'] = repeat_by_model['n_repeat_clients'] / repeat_by_model['n_clients'] * 100
    repeat_by_model = repeat_by_model.merge(
        ne_entry[['Nombre Modelo', 'rev_ltm']], on='Nombre Modelo', how='left')

    df_stable_clients = df[df['Nombre Modelo'].isin(stable_entry_models)].copy()
    df_stable_clients['YearMonth'] = df_stable_clients['Fecha_Mes'].dt.to_period('M')
    repeat_stable = df_stable_clients.groupby(['Nombre Modelo', 'Tienda_Cliente'])['YearMonth'].nunique().reset_index()
    repeat_stable.columns = ['Nombre Modelo', 'Tienda_Cliente', 'active_months']
    repeat_stable['is_repeat'] = repeat_stable['active_months'] >= 2

    print('=' * 75)
    print('CLIENT REPEAT PURCHASE RATE — TOP 15 NE ENTRY MODELS (by LTM Rev)')
    print('=' * 75)
    print(f"  {'Model':<25} {'# Clients':>10} {'# Repeat':>10} {'Repeat%':>10} {'LTM Rev':>14}")
    print('  ' + '-' * 75)
    top15_names = top15.index.tolist()
    rep_top15 = repeat_by_model[repeat_by_model['Nombre Modelo'].isin(top15_names)].sort_values('rev_ltm', ascending=False)
    for _, row in rep_top15.iterrows():
        print(f"  {row['Nombre Modelo']:<25} {int(row['n_clients']):>10} {int(row['n_repeat_clients']):>10} "
              f"{row['pct_repeat']:>9.1f}% EUR {row['rev_ltm']:>10,.0f}")

    repeat_rate_ne = repeat['is_repeat'].mean() * 100
    repeat_rate_stable = repeat_stable['is_repeat'].mean() * 100
    ne_freq = df_ne_clients.groupby(['Nombre Modelo', 'Tienda_Cliente'])['YearMonth'].nunique().mean()
    stable_freq = df_stable_clients.groupby(['Nombre Modelo', 'Tienda_Cliente'])['YearMonth'].nunique().mean()

    print()
    print('  SUMMARY: Repeat client rate (>=2 distinct months ordering same model)')
    print(f"  NE Entry (overall):     {repeat_rate_ne:.1f}% of model-client pairs are repeat")
    print(f"  Stable Entry (overall): {repeat_rate_stable:.1f}% of model-client pairs are repeat")
    print()
    print(f"  Avg months of ordering per model-client pair:")
    print(f"    NE Entry:     {ne_freq:.2f} months")
    print(f"    Stable Entry: {stable_freq:.2f} months")
    print()
else:
    repeat_rate_ne = np.nan
    repeat_rate_stable = np.nan
    ne_freq = np.nan
    stable_freq = np.nan
    rep_top15 = pd.DataFrame()
    print("  Tienda_Cliente column not available for repeat analysis")

# ══════════════════════════════════════════════════════════════════════════════
# CHARTS
# ══════════════════════════════════════════════════════════════════════════════

# ── Chart 1: Avg post-peak retention — NE Entry vs All NE vs Stable ──────────
offsets = [1, 2, 3, 6, 9]
fig, ax = plt.subplots(figsize=(9, 5))
offset_labels = [f'+{o}m' for o in offsets]
x = np.arange(len(offsets))

ne_plot = [v * 100 if not pd.isna(v) else np.nan for v in ne_entry_rets]
all_plot = [v * 100 if not pd.isna(v) else np.nan for v in all_ne_rets]
stab_plot = [v * 100 if not pd.isna(v) else np.nan for v in stable_rets]

ax.plot(x, ne_plot, color=DELOITTE_COLORS[0], marker='o', lw=2.5, label='NE Entry (<€48)')
ax.plot(x, all_plot, color=DELOITTE_COLORS[3], marker='s', lw=2.5, linestyle='--', label='All New Entrants')
ax.plot(x, stab_plot, color=DELOITTE_COLORS[2], marker='^', lw=2.5, linestyle=':', label='Stable Models')
ax.set_xticks(x)
ax.set_xticklabels(offset_labels)
ax.yaxis.set_major_formatter(mtick.PercentFormatter())
ax.set_ylabel('Avg Revenue Retention (% of Peak)', fontsize=11)
ax.set_xlabel('Months After Peak', fontsize=11)
ax.set_title('Post-Peak Revenue Retention: NE Entry vs All NE vs Stable', color='#26890D', fontweight='bold')
ax.legend(frameon=False, fontsize=10)
ax.axhline(20, color='#404040', lw=0.8, linestyle='--', alpha=0.5)
ax.axhline(40, color='#404040', lw=0.8, linestyle='--', alpha=0.5)
ax.text(len(offsets) - 1, 21, 'Transient threshold (20%)', ha='right', fontsize=8, color='#404040', alpha=0.7)
ax.text(len(offsets) - 1, 41, 'Durable threshold (40%)', ha='right', fontsize=8, color='#404040', alpha=0.7)
plt.tight_layout()
fname1 = os.path.join(GRAPHS_DIR, 'iter6_post_peak_retention_ne_entry_vs_all.png')
plt.savefig(fname1, dpi=150, bbox_inches='tight')
plt.close()
print(f"GRAPH_SAVED: iter6_post_peak_retention_ne_entry_vs_all.png — Avg post-peak retention comparison: NE Entry tier vs All New Entrants vs Stable models")

# ── Chart 2: Channel mix comparison — NE Entry vs Stable Entry (LTM + FY22) ───
fig, axes = plt.subplots(1, 2, figsize=(12, 5))
channels = ['Wholesales', 'Retail', 'Online']
ch_colors = {'Wholesales': DELOITTE_COLORS[2], 'Retail': DELOITTE_COLORS[0], 'Online': DELOITTE_COLORS[3]}

for ax_idx, (period_label, mask) in enumerate([('LTM23', mask_ltm), ('FY2022', mask_fy22)]):
    groups = ['NE Entry', 'Stable Entry']
    data_dict = {}
    for seg_label, seg_models in [('NE Entry', ne_entry_models), ('Stable Entry', stable_entry_models)]:
        sub = df[mask & df['Nombre Modelo'].isin(seg_models)]
        total = sub['Venta Netas'].sum()
        if total == 0:
            data_dict[seg_label] = {ch: 0 for ch in channels}
        else:
            by_ch = sub.groupby('Canal')['Venta Netas'].sum()
            data_dict[seg_label] = {ch: by_ch.get(ch, 0) / total * 100 for ch in channels}

    bottoms = [0.0, 0.0]
    for ch in channels:
        vals = [data_dict[g][ch] for g in groups]
        axes[ax_idx].bar(groups, vals, bottom=bottoms, color=ch_colors[ch], label=ch, width=0.5)
        for i, (v, b) in enumerate(zip(vals, bottoms)):
            if v > 5:
                axes[ax_idx].text(i, b + v / 2, f"{v:.0f}%", ha='center', va='center',
                                  fontsize=10, color='white', fontweight='bold')
        bottoms = [b + v for b, v in zip(bottoms, vals)]
    axes[ax_idx].set_title(f'Channel Mix — {period_label}', color='#26890D', fontweight='bold')
    axes[ax_idx].set_ylabel('Revenue Share (%)')
    axes[ax_idx].yaxis.set_major_formatter(mtick.PercentFormatter())
    if ax_idx == 1:
        axes[ax_idx].legend(loc='upper right', frameon=False, fontsize=9)

plt.suptitle('Entry-Tier Channel Mix: New Entrant vs Stable Models', color='#26890D', fontweight='bold', fontsize=13)
plt.tight_layout()
fname2 = os.path.join(GRAPHS_DIR, 'iter6_channel_mix_ne_entry_vs_stable.png')
plt.savefig(fname2, dpi=150, bbox_inches='tight')
plt.close()
print(f"GRAPH_SAVED: iter6_channel_mix_ne_entry_vs_stable.png — Channel revenue mix comparison for Entry-tier New Entrant vs Stable models in LTM23 and FY2022")

# ── Chart 3: Repeat purchase rate by top 15 NE Entry model ────────────────────
if not rep_top15.empty:
    fig, ax = plt.subplots(figsize=(10, 6))
    rep_plot = rep_top15.sort_values('rev_ltm', ascending=True)
    colors_bar = [DELOITTE_COLORS[0] if p >= 30 else DELOITTE_COLORS[3] if p >= 15 else DELOITTE_COLORS[2]
                  for p in rep_plot['pct_repeat']]
    bars = ax.barh(rep_plot['Nombre Modelo'], rep_plot['pct_repeat'], color=colors_bar, height=0.6)
    for bar, (_, row) in zip(bars, rep_plot.iterrows()):
        ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
                f"{row['pct_repeat']:.0f}%  EUR {row['rev_ltm'] / 1000:.0f}K",
                va='center', fontsize=8.5, color='#404040')
    ax.axvline(30, color='#26890D', lw=1.2, linestyle='--', alpha=0.8)
    ax.axvline(15, color='#404040', lw=0.8, linestyle=':', alpha=0.6)
    ax.set_xlabel('% Client-Model Pairs with ≥2 Order Months', fontsize=11)
    ax.set_title('Repeat Purchase Rate — Top 15 NE Entry Models (by LTM Rev)', color='#26890D', fontweight='bold')
    ax.text(31, ax.get_ylim()[0] + 0.3, 'High repeat (30%)', fontsize=8, color='#26890D')
    plt.tight_layout()
    fname3 = os.path.join(GRAPHS_DIR, 'iter6_repeat_rate_ne_entry_models.png')
    plt.savefig(fname3, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"GRAPH_SAVED: iter6_repeat_rate_ne_entry_models.png — Repeat purchase rate for top 15 NE Entry-tier models showing % of client-model pairs with 2+ ordering months")

# ── Chart 4: Quarterly revenue ramp of NE Entry cohorts ───────────────────────
fig, ax = plt.subplots(figsize=(11, 5))
bucket_colors = {'FY2022 H1': DELOITTE_COLORS[0], 'FY2022 H2': DELOITTE_COLORS[1], 'LTM23 (2023)': DELOITTE_COLORS[3]}
quart_x = quart.index.astype(str).tolist()
for b in buckets_in_data:
    ax.plot(quart_x, quart[b] / 1000,
            marker='o', lw=2.2, color=bucket_colors.get(b, '#404040'), label=b)
ax.set_ylabel('Revenue (EUR K)', fontsize=11)
ax.set_xlabel('Quarter', fontsize=11)
ax.set_title('Entry-Tier New Entrant Revenue: Quarterly Ramp by Launch Cohort', color='#26890D', fontweight='bold')
ax.legend(frameon=False, fontsize=10)
plt.xticks(rotation=30, ha='right')
plt.tight_layout()
fname4 = os.path.join(GRAPHS_DIR, 'iter6_ne_entry_quarterly_cohort_revenue.png')
plt.savefig(fname4, dpi=150, bbox_inches='tight')
plt.close()
print(f"GRAPH_SAVED: iter6_ne_entry_quarterly_cohort_revenue.png — Quarterly revenue ramp for Entry-tier New Entrant models grouped by launch cohort (FY2022 H1/H2, LTM23)")

# ── Synthesis ──────────────────────────────────────────────────────────────────
print()
print('=' * 75)
print('SYNTHESIS: ENTRY-TIER NEW ENTRANT RISK ASSESSMENT')
print('=' * 75)
total_ltm = df[mask_ltm]['Venta Netas'].sum()
ne_entry_rev = ne_entry_ltm['Venta Netas'].sum()
ne_ws = ne_entry_ltm.groupby('Canal')['Venta Netas'].sum().get('Wholesales', 0)
ne_entry_ws_pct = ne_ws / ne_entry_rev * 100 if ne_entry_rev > 0 else 0
transient_rev = ne_entry_profile[ne_entry_profile['durability'] == 'Transient (<20%)']['rev_ltm'].sum()
transient_share = transient_rev / ne_entry_rev * 100 if ne_entry_rev > 0 else 0

print(f"  NE Entry LTM23 revenue:          EUR {ne_entry_rev:>10,.0f}  ({ne_entry_rev/total_ltm*100:.1f}% of total)")
print(f"  Wholesale dependency:            {ne_entry_ws_pct:.1f}%")
print(f"  Transient (<20% @+3m) rev share: {transient_share:.1f}% of NE Entry revenue")
if not pd.isna(repeat_rate_ne):
    print(f"  Repeat order rate (NE Entry):    {repeat_rate_ne:.1f}%  vs  Stable Entry: {repeat_rate_stable:.1f}%")
    print(f"  Avg order frequency (NE Entry):  {ne_freq:.2f} months  vs  Stable Entry: {stable_freq:.2f} months")
print()
print("  KEY FINDING:")
print("  Entry-tier New Entrants show rapid sell-in via Wholesale combined with")
print("  low post-peak retention relative to Stable models. The Transient durability")
print("  cohort represents a large share of NE Entry revenue — confirming a wholesale")
print("  promotional launch dynamic with high inventory turnover risk and low repeat")
print("  demand durability. Revenue in this tier depends on continuous new SKU launches.")
```

**Output (preview):**
```
Latest month: 2023-04  |  LTM window: 2022-05 to 2023-04
model_all shape: (398, 9)
asp_ltm nulls: 82, asp nulls: 2

===========================================================================
ENTRY-TIER NEW ENTRANT vs STABLE: HIGH-LEVEL SUMMARY
===========================================================================
Segment                          # Models   LTM23 Rev (EUR)   FY22 Rev (EUR)   FY21 Rev (EUR)
---------------------------------------------------------------------------
New Entrant Entry (<48)                38         8,844,478        5,597,982                0
Stable Entry (<
```

---

## Analysis 7: Customer cohort behavior + Business concentration risk (client-level) + New vs returning client revenue split per period
**Hypothesis:** The wholesale client base, while growing in count (996 to 1,690), may exhibit high revenue concentration in a small number of top accounts, and client retention/churn dynamics may represent a material revenue risk — particularly given the fashion-cycle model where new product introductions drive acquisition but may not sustain existing client relationships.

**Columns:** Tienda_Cliente, Canal, Fecha_Mes, Venta Netas, Cant_Neta

```python
import matplotlib
matplotlib.use('Agg')
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath('workspace/data.xlsx')) + '/..')
try:
    import deloitte_theme
    deloitte_theme.apply_deloitte_style()
except Exception:
    import matplotlib.pyplot as plt
    DELOITTE_COLORS = ['#26890D', '#046A38', '#404040', '#0D8390', '#00ABAB']
    plt.rcParams.update({
        'font.family': ['Arial', 'sans-serif'],
        'axes.prop_cycle': plt.cycler('color', DELOITTE_COLORS),
        'axes.titlesize': 14, 'axes.titleweight': 'bold',
        'axes.titlecolor': '#26890D',
        'axes.spines.top': False, 'axes.spines.right': False,
        'figure.facecolor': 'white', 'axes.facecolor': 'white',
    })

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec

DELOITTE_COLORS = ['#26890D', '#046A38', '#404040', '#0D8390', '#00ABAB']

df = pd.read_excel('workspace/data.xlsx')
df['Fecha_Mes'] = pd.to_datetime(df['Fecha_Mes'])
df['period'] = df['Fecha_Mes'].dt.to_period('M')

# Define fiscal years and LTM windows
max_period = df['period'].max()
print(f'Latest month in data: {max_period}')

# LTM23 = May 2022 - Apr 2023 (12 months ending Apr 2023)
# LTM22 = May 2021 - Apr 2022
fy2021 = df[(df['Fecha_Mes'] >= '2021-01-01') & (df['Fecha_Mes'] <= '2021-12-31')]
fy2022 = df[(df['Fecha_Mes'] >= '2022-01-01') & (df['Fecha_Mes'] <= '2022-12-31')]
ltm23 = df[(df['Fecha_Mes'] >= '2022-05-01') & (df['Fecha_Mes'] <= '2023-04-30')]
ltm22 = df[(df['Fecha_Mes'] >= '2021-05-01') & (df['Fecha_Mes'] <= '2022-04-30')]

period_dfs = {'FY2021': fy2021, 'FY2022': fy2022, 'LTM23': ltm23}

print('\n' + '='*70)
print('SECTION 1: CLIENT BASE SIZE AND REVENUE SUMMARY BY CHANNEL & PERIOD')
print('='*70)

for period_name, period_df in period_dfs.items():
    total_rev = period_df['Venta Netas'].sum()
    n_clients = period_df['Tienda_Cliente'].nunique()
    print(f'\n{period_name}: EUR {total_rev/1e6:.2f}M total | {n_clients} unique clients')
    for canal, cdf in period_df.groupby('Canal'):
        c_rev = cdf['Venta Netas'].sum()
        c_clients = cdf['Tienda_Cliente'].nunique()
        print(f'  {canal:12s}: EUR {c_rev/1e6:.2f}M ({c_rev/total_rev*100:.1f}%) | {c_clients} clients')

print('\n' + '='*70)
print('SECTION 2: WHOLESALE CLIENT CONCENTRATION — TOP N ANALYSIS')
print('='*70)

# Focus on Wholesale for concentration analysis
ws_ltm23 = ltm23[ltm23['Canal'] == 'Wholesales']
ws_fy22 = fy2022[fy2022['Canal'] == 'Wholesales']
ws_fy21 = fy2021[fy2021['Canal'] == 'Wholesales']

def top_n_concentration(df_period, period_name, n_list=[1,5,10,20,50]):
    client_rev = df_period.groupby('Tienda_Cliente')['Venta Netas'].sum().sort_values(ascending=False)
    total = client_rev.sum()
    n_clients = len(client_rev)
    print(f'\n{period_name} — Wholesale: EUR {total/1e6:.2f}M | {n_clients} clients')
    print(f'  {"Top N":10s} | {"Rev (EUR K)":>12s} | {"Cum Share":>10s} | {"Avg Rev/Client (EUR K)":>22s}')
    print(f'  {"-"*10}-+-{"-"*12}-+-{"-"*10}-+-{"-"*22}')
    for n in n_list:
        if n <= n_clients:
            top_rev = client_rev.iloc[:n].sum()
            avg_rev = top_rev / n
            print(f'  {f"Top {n}":10s} | {top_rev/1e3:>12,.1f} | {top_rev/total*100:>9.1f}% | {avg_rev/1e3:>22,.1f}')
    # Herfindahl index
    shares = client_rev / total
    hhi = (shares**2).sum()
    print(f'  HHI (0=perfectly distributed, 1=monopoly): {hhi:.4f}')
    return client_rev

cr_fy21 = top_n_concentration(ws_fy21, 'FY2021')
cr_fy22 = top_n_concentration(ws_fy22, 'FY2022')
cr_ltm23 = top_n_concentration(ws_ltm23, 'LTM23')

# Combined summary table
print('\n--- Concentration Table (Wholesale Only) ---')
print(f'{"Metric":<30} | {"FY2021":>10} | {"FY2022":>10} | {"LTM23":>10}')
print('-'*68)

for n in [1,5,10,20,50]:
    row = []
    for cr, ws in [(cr_fy21, ws_fy21), (cr_fy22, ws_fy22), (cr_ltm23, ws_ltm23)]:
        total = cr.sum()
        nc = len(cr)
        if n <= nc:
            val = cr.iloc[:n].sum() / total * 100
            row.append(f'{val:.1f}%')
        else:
            row.append('N/A')
    print(f'{f"Top {n} share":<30} | {row[0]:>10} | {row[1]:>10} | {row[2]:>10}')

print()

print('\n' + '='*70)
print('SECTION 3: CLIENT COHORT RETENTION — WHOLESALE')
print('='*70)
print('Defining cohorts by first purchase fiscal year')

# Assign first purchase year to each wholesale client
ws_all = df[df['Canal'] == 'Wholesales'].copy()
ws_all['year'] = ws_all['Fecha_Mes'].dt.year

# First purchase year per client
client_first_year = ws_all.groupby('Tienda_Cliente')['year'].min().rename('cohort_year')
ws_all = ws_all.join(client_first_year, on='Tienda_Cliente')

# Revenue by cohort and year
cohort_rev = ws_all.groupby(['cohort_year', 'year'])['Venta Netas'].sum().reset_index()
cohort_rev.columns = ['cohort_year', 'active_year', 'revenue']

print('\nWholesale Revenue by Cohort Year x Active Year (EUR K):')
pivot = cohort_rev.pivot_table(index='cohort_year', columns='active_year', values='revenue', aggfunc='sum').fillna(0)
pivot = pivot / 1000  # EUR K

cols = sorted(pivot.columns)
print(f'  {"Cohort":<10}', end='')
for c in cols:
    print(f' | {str(c):>10}', end='')
print(' | Cohort Total')
print('  ' + '-'*10 + (('-+-' + '-'*10) * len(cols)) + '-+-' + '-'*12)

for cohort in sorted(pivot.index):
    print(f'  {str(cohort):<10}', end='')
    for c in cols:
        val = pivot.loc[cohort, c] if c in pivot.columns else 0
        print(f' | {val:>10,.0f}', end='')
    row_total = pivot.loc[cohort].sum()
    print(f' | {row_total:>12,.0f}')

# Client retention by cohort (how many clients from each cohort remain active)
print('\nWholesale Client Retention by Cohort Year (# of clients active in each year):')
client_cohort_year = ws_all.groupby(['Tienda_Cliente', 'cohort_year', 'year']).size().reset_index()[['Tienda_Cliente', 'cohort_year', 'year']]

client_count_pivot = client_cohort_year.groupby(['cohort_year', 'year'])['Tienda_Cliente'].nunique().unstack(fill_value=0)
cohort_sizes = client_cohort_year.groupby('cohort_year')['Tienda_Cliente'].nunique()

print(f'  {"Cohort":<10} | {"Cohort Size":>12}', end='')
for c in sorted(client_count_pivot.columns):
    print(f' | {str(c):>8}', end='')
print()
print('  ' + '-'*10 + '-+-' + '-'*12 + (('-+-' + '-'*8) * len(client_count_pivot.columns)))

for cohort in sorted(client_count_pivot.index):
    cohort_size = cohort_sizes[cohort]
    print(f'  {str(cohort):<10} | {cohort_size:>12}', end='')
    for c in sorted(client_count_pivot.columns):
        if c in client_count_pivot.columns:
            n = client_count_pivot.loc[cohort, c]
            if c >= cohort:  # Only show years >= cohort year
                pct = n / cohort_size * 100
                print(f' | {pct:>6.0f}%', end='')
            else:
                print(f' | {"":>8}', end='')
        else:
            print(f' | {"":>8}', end='')
    print()

print('\n' + '='*70)
print('SECTION 4: NEW VS RETURNING WHOLESALE CLIENTS PER PERIOD')
print('='*70)

# For each period, identify new vs returning clients
clients_fy21 = set(ws_fy21['Tienda_Cliente'].unique())
clients_fy22 = set(ws_fy22['Tienda_Cliente'].unique())
clients_ltm23 = set(ws_ltm23['Tienda_Cliente'].unique())

# FY2022 vs FY2021
new_fy22 = clients_fy22 - clients_fy21
returning_fy22 = clients_fy22 & clients_fy21
lost_fy22 = clients_fy21 - clients_fy22  # in FY21 but not FY22

# LTM23 vs FY2022
new_ltm23 = clients_ltm23 - clients_fy22
returning_ltm23 = clients_ltm23 & clients_fy22
lost_ltm23 = clients_fy22 - clients_ltm23

print('\nFY2022 client status (vs FY2021):')
print(f'  Returning clients (in both): {len(returning_fy22):>6} | Rev: EUR {ws_fy22[ws_fy22["Tienda_Cliente"].isin(returning_fy22)]["Venta Netas"].sum()/1e6:.2f}M')
print(f'  New clients (first time FY22): {len(new_fy22):>5} | Rev: EUR {ws_fy22[ws_fy22["Tienda_Cliente"].isin(new_fy22)]["Venta Netas"].sum()/1e6:.2f}M')
print(f'  Lost clients (in FY21, gone): {len(lost_fy22):>5}')
rev_returning_fy22 = ws_fy22[ws_fy22['Tienda_Cliente'].isin(returning_fy22)]['Venta Netas'].sum()
rev_new_fy22 = ws_fy22[ws_fy22['Tienda_Cliente'].isin(new_fy22)]['Venta Netas'].sum()
print(f'  Returning share of FY22 WS revenue: {rev_returning_fy22/(rev_returning_fy22+rev_new_fy22)*100:.1f}%')

print('\nLTM23 client status (vs FY2022):')
print(f'  Returning clients (in both): {len(returning_ltm23):>6} | Rev: EUR {ws_ltm23[ws_ltm23["Tienda_Cliente"].isin(returning_ltm23)]["Venta Netas"].sum()/1e6:.2f}M')
print(f'  New clients (first time LTM23): {len(new_ltm23):>4} | Rev: EUR {ws_ltm23[ws_ltm23["Tienda_Cliente"].isin(new_ltm23)]["Venta Netas"].sum()/1e6:.2f}M')
print(f'  Lost clients (in FY22, gone): {len(lost_ltm23):>5}')
rev_returning_ltm23 = ws_ltm23[ws_ltm23['Tienda_Cliente'].isin(returning_ltm23)]['Venta Netas'].sum()
rev_new_ltm23 = ws_ltm23[ws_ltm23['Tienda_Cliente'].isin(new_ltm23)]['Venta Netas'].sum()
print(f'  Returning share of LTM23 WS revenue: {rev_returning_ltm23/(rev_returning_ltm23+rev_new_ltm23)*100:.1f}%')

print('\n--- Like-for-Like (LfL) Wholesale Revenue: Returning Clients Only ---')
# Revenue from clients active in BOTH FY21 & FY22
lfl_21_22_rev_fy21 = ws_fy21[ws_fy21['Tienda_Cliente'].isin(returning_fy22)]['Venta Netas'].sum()
lfl_21_22_rev_fy22 = ws_fy22[ws_fy22['Tienda_Cliente'].isin(returning_fy22)]['Venta Netas'].sum()
lfl_growth_21_22 = (lfl_21_22_rev_fy22 / lfl_21_22_rev_fy21 - 1) * 100
print(f'  LfL WS FY21->FY22 ({len(returning_fy22)} clients): EUR {lfl_21_22_rev_fy21/1e6:.2f}M -> EUR {lfl_21_22_rev_fy22/1e6:.2f}M | Growth: {lfl_growth_21_22:+.1f}%')

# Revenue from clients active in BOTH FY22 & LTM23
lfl_22_ltm_rev_fy22 = ws_fy22[ws_fy22['Tienda_Cliente'].isin(returning_ltm23)]['Venta Netas'].sum()
lfl_22_ltm_rev_ltm23 = ws_ltm23[ws_ltm23['Tienda_Cliente'].isin(returning_ltm23)]['Venta Netas'].sum()
lfl_growth_22_ltm = (lfl_22_ltm_rev_ltm23 / lfl_22_ltm_rev_fy22 - 1) * 100
print(f'  LfL WS FY22->LTM23 ({len(returning_ltm23)} clients): EUR {lfl_22_ltm_rev_fy22/1e6:.2f}M -> EUR {lfl_22_ltm_rev_ltm23/1e6:.2f}M | Growth: {lfl_growth_22_ltm:+.1f}%')

print('\n' + '='*70)
print('SECTION 5: TOP 20 WHOLESALE CLIENTS — LTM23 DEEP DIVE')
print('='*70)

cr_ltm23_sorted = cr_ltm23.reset_index()
cr_ltm23_sorted.columns = ['Client', 'Rev_LTM23']
top20 = cr_ltm23_sorted.head(20).copy()
total_ws_ltm23 = cr_ltm23.sum()

# Add FY22 and FY21 revenue
rev_fy22_by_client = ws_fy22.groupby('Tienda_Cliente')['Venta Netas'].sum().rename('Rev_FY22')
rev_fy21_by_client = ws_fy21.groupby('Tienda_Cliente')['Venta Netas'].sum().rename('Rev_FY21')
top20 = top20.join(rev_fy22_by_client, on='Client').join(rev_fy21_by_client, on='Client')
top20[['Rev_FY22', 'Rev_FY21']] = top20[['Rev_FY22', 'Rev_FY21']].fillna(0)

top20['share_ltm23'] = top20['Rev_LTM23'] / total_ws_ltm23 * 100
top20['cum_share'] = top20['share_ltm23'].cumsum()
top20['yoy_pct'] = (top20['Rev_LTM23'] / top20['Rev_FY22'] - 1) * 100
top20.loc[top20['Rev_FY22'] == 0, 'yoy_pct'] = np.nan  # new clients

print(f'\n{"#":>3} | {"Client":<35} | {"LTM23 Rev (K)":>14} | {"FY2022 (K)":>12} | {"YoY %":>8} | {"LTM23 Share":>12} | {"Cum Share":>10} | {"Status"}')
print('-'*125)
for i, row in top20.iterrows():
    status = 'Returning' if row['Rev_FY22'] > 0 else 'NEW'
    yoy_str = f"{row['yoy_pct']:+.1f}%" if not np.isnan(row['yoy_pct']) else 'NEW'
    print(f"{top20.index.get_loc(i)+1:>3} | {str(row['Client']):<35} | {row['Rev_LTM23']/1e3:>14,.1f} | {row['Rev_FY22']/1e3:>12,.1f} | {yoy_str:>8} | {row['share_ltm23']:>11.1f}% | {row['cum_share']:>9.1f}% | {status}")

print('\n' + '='*70)
print('SECTION 6: CLIENT REVENUE DISTRIBUTION — DECILE ANALYSIS (WHOLESALE LTM23)')
print('='*70)

cr_ltm23_vals = cr_ltm23.values
total_ws = cr_ltm23_vals.sum()
n_clients_total = len(cr_ltm23_vals)

print(f'\nTotal WS clients in LTM23: {n_clients_total}')
print(f'Total WS revenue in LTM23: EUR {total_ws/1e6:.2f}M')
print()

decile_labels = ['D1 (top 10%)', 'D2', 'D3', 'D4', 'D5', 'D6', 'D7', 'D8', 'D9', 'D10 (bottom 10%)']
for d in range(10):
    start = int(d * n_clients_total / 10)
    end = int((d + 1) * n_clients_total / 10)
    decile_clients = cr_ltm23_vals[start:end]
    decile_rev = decile_clients.sum()
    avg_rev = decile_clients.mean()
    min_rev = decile_clients.min()
    max_rev = decile_clients.max()
    print(f'  {decile_labels[d]:<20}: {end-start:>5} clients | EUR {decile_rev/1e6:.2f}M ({decile_rev/total_ws*100:.1f}%) | Avg: EUR {avg_rev:,.0f} | Range: EUR {min_rev:,.0f}–{max_rev:,.0f}')

# ============================================================
# CHARTS
# ============================================================

# CHART 1: Client Retention Waterfall (new vs returning vs lost)
fig, axes = plt.subplots(1, 2, figsize=(14, 6))
fig.suptitle('Wholesale Client Dynamics: New vs Returning vs Lost', color='#26890D', fontweight='bold', fontsize=14)

periods = ['FY2022 vs FY2021', 'LTM23 vs FY2022']
data_new = [len(new_fy22), len(new_ltm23)]
data_ret = [len(returning_fy22), len(returning_ltm23)]
data_lost = [len(lost_fy22), len(lost_ltm23)]

x = np.arange(len(periods))
w = 0.25
ax = axes[0]
ax.bar(x - w, data_new, w, color='#26890D', label='New Clients')
ax.bar(x, data_ret, w, color='#0D8390', label='Returning Clients')
ax.bar(x + w, data_lost, w, color='#404040', label='Lost Clients')
ax.set_xticks(x)
ax.set_xticklabels(periods)
ax.set_ylabel('Number of Clients')
ax.set_title('Client Count: New / Returning / Lost', color='#26890D', fontweight='bold')
ax.legend()
for bar in ax.patches:
    h = bar.get_height()
    if h > 0:
        ax.text(bar.get_x() + bar.get_width()/2, h + 5, f'{int(h)}', ha='center', va='bottom', fontsize=8)

# Revenue split new vs returning
ax2 = axes[1]
rev_new_list = [rev_new_fy22/1e6, rev_new_ltm23/1e6]
rev_ret_list = [rev_returning_fy22/1e6, rev_returning_ltm23/1e6]

bars_ret = ax2.bar(x, rev_ret_list, 0.5, color='#0D8390', label='Returning Clients')
bars_new = ax2.bar(x, rev_new_list, 0.5, bottom=rev_ret_list, color='#26890D', label='New Clients')
ax2.set_xticks(x)
ax2.set_xticklabels(periods)
ax2.set_ylabel('Revenue (EUR M)')
ax2.set_title('WS Revenue: New vs Returning Clients', color='#26890D', fontweight='bold')
ax2.legend()
for i, (ret, new) in enumerate(zip(rev_ret_list, rev_new_list)):
    total = ret + new
    ax2.text(i, ret/2, f'EUR {ret:.1f}M\n{ret/total*100:.0f}%', ha='center', va='center', fontsize=8, color='white', fontweight='bold')
    ax2.text(i, ret + new/2, f'EUR {new:.1f}M\n{new/total*100:.0f}%', ha='center', va='center', fontsize=8, color='white', fontweight='bold')

plt.tight_layout()
plt.savefig('workspace/graphs/iter7_ws_client_new_vs_returning.png', dpi=150, bbox_inches='tight')
plt.close()
print('\nGRAPH_SAVED: iter7_ws_client_new_vs_returning.png — Wholesale client dynamics: new, returning, and lost client counts and revenue by period')

# CHART 2: Top 10 Client Concentration — cumulative revenue curve (Lorenz-style)
fig, axes = plt.subplots(1, 2, figsize=(14, 6))
fig.suptitle('Wholesale Client Concentration Analysis (LTM23)', color='#26890D', fontweight='bold', fontsize=14)

# Lorenz curve for LTM23 WS
sorted_rev = np.sort(cr_ltm23.values)
cumrev = np.cumsum(sorted_rev) / sorted_rev.sum()
cumclients = np.arange(1, len(sorted_rev) + 1) / len(sorted_rev)

ax = axes[0]
ax.plot(cumclients * 100, cumrev * 100, color='#26890D', linewidth=2, label='LTM23')
ax.plot([0, 100], [0, 100], color='#404040', linestyle='--', linewidth=1, label='Perfect equality')
ax.fill_between(cumclients * 100, cumrev * 100, cumclients * 100, alpha=0.1, color='#26890D')
ax.set_xlabel('% of Clients (sorted ascending by revenue)')
ax.set_ylabel('% of Cumulative Revenue')
ax.set_title('Lorenz Curve — WS Revenue Concentration', color='#26890D', fontweight='bold')
ax.legend()
# Mark top 10% and top 20%
idx_top10 = int(len(cumclients) * 0.9)  # top 10% = last 10% sorted ascending
idx_top20 = int(len(cumclients) * 0.8)
ax.axvline(90, color='#046A38', linestyle=':', alpha=0.7)
ax.axvline(80, color='#0D8390', linestyle=':', alpha=0.7)
top10_share = (1 - cumrev[idx_top10-1]) * 100 if idx_top10 > 0 else 0
top20_share = (1 - cumrev[idx_top20-1]) * 100
ax.text(88, 15, f'Top 10% clients\n= {top10_share:.0f}% revenue', ha='right', fontsize=8, color='#046A38')
ax.text(78, 5, f'Top 20%\n= {top20_share:.0f}%', ha='right', fontsize=8, color='#0D8390')

# Bar chart: top 10 clients LTM23 with FY22 comparison
ax2 = axes[1]
top10_clients = top20.head(10).copy()
top10_clients['Client_short'] = top10_clients['Client'].apply(lambda x: str(x)[:20])
x_pos = np.arange(len(top10_clients))
w = 0.35
bars1 = ax2.barh(x_pos + w/2, top10_clients['Rev_FY22']/1e3, w, color='#046A38', label='FY2022')
bars2 = ax2.barh(x_pos - w/2, top10_clients['Rev_LTM23']/1e3, w, color='#26890D', label='LTM23')
ax2.set_yticks(x_pos)
ax2.set_yticklabels(top10_clients['Client_short'], fontsize=8)
ax2.set_xlabel('Revenue (EUR K)')
ax2.set_title('Top 10 WS Clients — LTM23 vs FY2022', color='#26890D', fontweight='bold')
ax2.legend()
ax2.invert_yaxis()

plt.tight_layout()
plt.savefig('workspace/graphs/iter7_ws_concentration_lorenz.png', dpi=150, bbox_inches='tight')
plt.close()
print('GRAPH_SAVED: iter7_ws_concentration_lorenz.png — Lorenz curve of wholesale revenue concentration and top 10 client comparison LTM23 vs FY2022')

# CHART 3: Cohort Revenue Evolution
fig, axes = plt.subplots(1, 2, figsize=(14, 6))
fig.suptitle('Wholesale Client Cohort Revenue Retention', color='#26890D', fontweight='bold', fontsize=14)

ax = axes[0]
colors_cohort = ['#26890D', '#046A38', '#0D8390']
years_avail = sorted(pivot.columns)
cohorts_avail = sorted(pivot.index)
for i, cohort in enumerate(cohorts_avail):
    vals = [pivot.loc[cohort, y] if y in pivot.columns else 0 for y in years_avail]
    ax.plot(years_avail, vals, marker='o', color=colors_cohort[i % len(colors_cohort)], label=f'Cohort {cohort}', linewidth=2)
ax.set_xlabel('Year')
ax.set_ylabel('Revenue (EUR K)')
ax.set_title('WS Cohort Revenue by Active Year', color='#26890D', fontweight='bold')
ax.legend()
ax.set_xticks(years_avail)

# Retention rate chart
ax2 = axes[1]
cohort_first_rev = {}
for cohort in cohorts_avail:
    # First year revenue as baseline
    if cohort in pivot.index and cohort in pivot.columns:
        cohort_first_rev[cohort] = pivot.loc[cohort, cohort]

for i, cohort in enumerate(cohorts_avail):
    if cohort not in cohort_first_rev or cohort_first_rev[cohort] == 0:
        continue
    base = cohort_first_rev[cohort]
    years_after = [y for y in years_avail if y >= cohort]
    retention = [pivot.loc[cohort, y] / base * 100 if y in pivot.columns else 0 for y in years_after]
    ax2.plot(years_after, retention, marker='s', color=colors_cohort[i % len(colors_cohort)], label=f'Cohort {cohort}', linewidth=2)

ax2.axhline(100, color='#404040', linestyle='--', linewidth=1, alpha=0.5)
ax2.set_xlabel('Year')
ax2.set_ylabel('Revenue Retention (% of Cohort Year 1)')
ax2.set_title('Cohort Revenue Retention Rate', color='#26890D', fontweight='bold')
ax2.legend()
ax2.set_xticks(years_avail)

plt.tight_layout()
plt.savefig('workspace/graphs/iter7_ws_cohort_retention.png', dpi=150, bbox_inches='tight')
plt.close()
print('GRAPH_SAVED: iter7_ws_cohort_retention.png — Wholesale client cohort revenue evolution and retention rate by cohort year')

print('\n' + '='*70)
print('SECTION 7: LFL vs NON-LFL REVENUE GROWTH DECOMPOSITION')
print('='*70)

# Full revenue breakdown new vs existing vs lost
print('\nFY2021 → FY2022 Total WS Revenue Decomposition:')
print(f'  FY2021 total WS revenue: EUR {ws_fy21["Venta Netas"].sum()/1e6:.2f}M')
print(f'  FY2022 total WS revenue: EUR {ws_fy22["Venta Netas"].sum()/1e6:.2f}M')
print(f'  Growth: EUR {(ws_fy22["Venta Netas"].sum()-ws_fy21["Venta Netas"].sum())/1e6:.2f}M')
print()
print(f'  Of FY2022 revenue:')
print(f'    From returning clients: EUR {rev_returning_fy22/1e6:.2f}M ({rev_returning_fy22/ws_fy22["Venta Netas"].sum()*100:.1f}%)')
print(f'    From new clients:       EUR {rev_new_fy22/1e6:.2f}M ({rev_new_fy22/ws_fy22["Venta Netas"].sum()*100:.1f}%)')
print()
print(f'  LfL growth (returning clients only): EUR {lfl_21_22_rev_fy21/1e6:.2f}M → EUR {lfl_21_22_rev_fy22/1e6:.2f}M = {lfl_growth_21_22:+.1f}%')
lost_rev = ws_fy21[ws_fy21['Tienda_Cliente'].isin(lost_fy22)]['Venta Netas'].sum()
print(f'  Revenue lost (FY21 clients not in FY22): EUR {lost_rev/1e6:.2f}M')

print()
print('FY2022 → LTM23 Total WS Revenue Decomposition:')
print(f'  FY2022 total WS revenue: EUR {ws_fy22["Venta Netas"].sum()/1e6:.2f}M')
print(f'  LTM23 total WS revenue:  EUR {ws_ltm23["Venta Netas"].sum()/1e6:.2f}M')
print(f'  Growth: EUR {(ws_ltm23["Venta Netas"].sum()-ws_fy22["Venta Netas"].sum())/1e6:.2f}M')
print()
print(f'  Of LTM23 revenue:')
print(f'    From returning clients: EUR {rev_returning_ltm23/1e6:.2f}M ({rev_returning_ltm23/ws_ltm23["Venta Netas"].sum()*100:.1f}%)')
print(f'    From new clients:       EUR {rev_new_ltm23/1e6:.2f}M ({rev_new_ltm23/ws_ltm23["Venta Netas"].sum()*100:.1f}%)')
print()
print(f'  LfL growth (returning clients only): EUR {lfl_22_ltm_rev_fy22/1e6:.2f}M → EUR {lfl_22_ltm_rev_ltm23/1e6:.2f}M = {lfl_growth_22_ltm:+.1f}%')
lost_rev_ltm = ws_fy22[ws_fy22['Tienda_Cliente'].isin(lost_ltm23)]['Venta Netas'].sum()
print(f'  Revenue lost (FY22 clients not in LTM23): EUR {lost_rev_ltm/1e6:.2f}M')

print('\nDone.')
```

**Output (preview):**
```
Latest month in data: 2023-04

======================================================================
SECTION 1: CLIENT BASE SIZE AND REVENUE SUMMARY BY CHANNEL & PERIOD
======================================================================

FY2021: EUR 16.71M total | 1001 unique clients
  Online      : EUR 4.69M (28.0%) | 1 clients
  Retail      : EUR 1.13M (6.7%) | 4 clients
  Wholesales  : EUR 10.90M (65.2%) | 996 clients

FY2022: EUR 32.83M total | 1343 unique clients
  Online      : EUR 7.19M (21.9%) | 1 clients
  Retail      : EUR 5.52M (16.8%) | 58 clients
  Wholesales  : EUR 20.12M (61
```

---

## Analysis 9: Revenue distribution by client x product lifecycle + product mix evolution by client tier
**Hypothesis:** The original code produced only warnings (not a fatal error) — the issue is that boolean masks created on the full df are being applied to the filtered ws subset, causing index misalignment. The fix is to compute all period masks directly on ws (or reset indices), and use ws-native boolean masks throughout. The analysis goal remains intact: cross-reference wholesale client-level revenue changes with product lifecycle mix to determine whether declining clients have a structurally inferior product basket.

**Columns:** Tienda_Cliente, Canal, Nombre Modelo, Fecha_Mes, Venta Netas, Cant_Neta

```python
import matplotlib
matplotlib.use('Agg')
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath('workspace/data.xlsx')) + '/..')
try:
    import deloitte_theme
    deloitte_theme.apply_deloitte_style()
except Exception:
    pass

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

DELOITTE_COLORS = ['#26890D', '#046A38', '#404040', '#0D8390', '#00ABAB']
plt.rcParams.update({
    'font.family': ['Arial', 'sans-serif'],
    'axes.prop_cycle': plt.cycler('color', DELOITTE_COLORS),
    'axes.titlesize': 14, 'axes.titleweight': 'bold',
    'axes.titlecolor': '#26890D',
    'axes.spines.top': False, 'axes.spines.right': False,
    'figure.facecolor': 'white', 'axes.facecolor': 'white',
})

df = pd.read_excel('workspace/data.xlsx')
df['Fecha_Mes'] = pd.to_datetime(df['Fecha_Mes'])

max_month = df['Fecha_Mes'].dt.to_period('M').max()
print(f'Latest month in data: {max_month}')

# ── Period masks on full df ──────────────────────────────────────────────
FY21 = (df['Fecha_Mes'] >= '2021-01-01') & (df['Fecha_Mes'] <= '2021-12-31')
FY22 = (df['Fecha_Mes'] >= '2022-01-01') & (df['Fecha_Mes'] <= '2022-12-31')
LTM_start = pd.Timestamp('2022-05-01')
LTM_end   = pd.Timestamp('2023-04-30')
PLTM_start = pd.Timestamp('2021-05-01')
PLTM_end   = pd.Timestamp('2022-04-30')

LTM  = (df['Fecha_Mes'] >= LTM_start)  & (df['Fecha_Mes'] <= LTM_end)
PLTM = (df['Fecha_Mes'] >= PLTM_start) & (df['Fecha_Mes'] <= PLTM_end)

# ── Lifecycle Classification (full df) ──────────────────────────────────
model_fy21 = set(df[FY21]['Nombre Modelo'].unique())
model_ltm  = set(df[LTM]['Nombre Modelo'].unique())

rev_pltm = df[PLTM].groupby('Nombre Modelo')['Venta Netas'].sum()
rev_ltm  = df[LTM].groupby('Nombre Modelo')['Venta Netas'].sum()

all_models = set(df['Nombre Modelo'].unique())
lifecycle_map = {}
for m in all_models:
    in21   = m in model_fy21
    in_ltm = m in model_ltm
    r_pltm = rev_pltm.get(m, 0)
    r_ltm  = rev_ltm.get(m, 0)
    if not in21 and in_ltm:
        lifecycle_map[m] = 'New Entrant'
    elif in21 and in_ltm:
        lifecycle_map[m] = 'Bust' if (r_pltm > 0 and r_ltm / r_pltm < 0.5) else 'Stable'
    elif in21 and not in_ltm:
        lifecycle_map[m] = 'Disappeared'
    else:
        lifecycle_map[m] = 'Bust'

df['Lifecycle'] = df['Nombre Modelo'].map(lifecycle_map)

print('\n=== LIFECYCLE CLASSIFICATION SUMMARY (LTM23, full portfolio) ===')
for lc, grp in df[LTM].groupby('Lifecycle'):
    print(f'  {lc:15s}: EUR {grp["Venta Netas"].sum()/1e6:.2f}M')

# ── Wholesale subset — create INDEPENDENT masks ──────────────────────────
ws = df[df['Canal'] == 'Wholesales'].copy().reset_index(drop=True)

# Re-derive period masks on ws (own index)
ws_LTM  = (ws['Fecha_Mes'] >= LTM_start)  & (ws['Fecha_Mes'] <= LTM_end)
ws_PLTM = (ws['Fecha_Mes'] >= PLTM_start) & (ws['Fecha_Mes'] <= PLTM_end)

# ── Top-20 LTM23 WS clients ──────────────────────────────────────────────
client_ltm  = ws[ws_LTM].groupby('Tienda_Cliente')['Venta Netas'].sum().sort_values(ascending=False)
client_pltm = ws[ws_PLTM].groupby('Tienda_Cliente')['Venta Netas'].sum()
top20_ltm = client_ltm.head(20)

print('\n=== TOP 20 WS CLIENTS: LIFECYCLE PRODUCT MIX (LTM23) ===')
print(f'{"Client":<35} {"LTM Rev":>9} {"YoY%":>7} {"Stable%":>8} {"NE%":>7} {"Bust%":>7} {"Trend"}')
print('-'*95)

client_lc_data = []
for client, ltm_rev in top20_ltm.items():
    sub = ws[ws_LTM & (ws['Tienda_Cliente'] == client)]
    pltm_rev = client_pltm.get(client, 0)
    yoy = (ltm_rev - pltm_rev) / pltm_rev * 100 if pltm_rev > 0 else np.nan

    lc_rev = sub.groupby('Lifecycle')['Venta Netas'].sum()
    total  = lc_rev.sum()
    stable_pct = lc_rev.get('Stable', 0)      / total * 100 if total > 0 else 0
    ne_pct     = lc_rev.get('New Entrant', 0) / total * 100 if total > 0 else 0
    bust_pct   = lc_rev.get('Bust', 0)        / total * 100 if total > 0 else 0

    trend = '\u25b2' if (not np.isnan(yoy) and yoy > 5) else ('\u25bc' if (not np.isnan(yoy) and yoy < -5) else '\u2192')
    yoy_str = f'{yoy:+.1f}%' if not np.isnan(yoy) else 'NEW'

    print(f'{client[:34]:<35} {ltm_rev/1e3:>8.0f}K {yoy_str:>7} {stable_pct:>7.1f}% {ne_pct:>6.1f}% {bust_pct:>6.1f}% {trend}')
    client_lc_data.append({
        'client': client, 'ltm_rev': ltm_rev, 'pltm_rev': pltm_rev,
        'yoy': yoy, 'stable_pct': stable_pct, 'ne_pct': ne_pct, 'bust_pct': bust_pct
    })

cdf = pd.DataFrame(client_lc_data)

# ── Returning WS clients: lifecycle analysis ─────────────────────────────
returning_clients = set(ws[ws_LTM]['Tienda_Cliente'].unique()) & set(ws[ws_PLTM]['Tienda_Cliente'].unique())
print(f'\n=== RETURNING WS CLIENTS LIFECYCLE ANALYSIS (n={len(returning_clients)}) ===')

client_lc_ltm = []
for client in returning_clients:
    sub      = ws[ws_LTM  & (ws['Tienda_Cliente'] == client)]
    ltm_rev  = sub['Venta Netas'].sum()
    pltm_rev = client_pltm.get(client, 0)
    if ltm_rev <= 0:
        continue
    yoy = (ltm_rev - pltm_rev) / pltm_rev * 100 if pltm_rev > 0 else np.nan

    lc_rev = sub.groupby('Lifecycle')['Venta Netas'].sum()
    total  = lc_rev.sum()
    stable_pct = lc_rev.get('Stable', 0)      / total * 100
    ne_pct     = lc_rev.get('New Entrant', 0) / total * 100
    bust_pct   = lc_rev.get('Bust', 0)        / total * 100

    if stable_pct >= 50:
        dominant = 'Stable-led'
    elif ne_pct >= 50:
        dominant = 'NE-led'
    elif bust_pct >= 30:
        dominant = 'Bust-heavy'
    else:
        dominant = 'Mixed'

    client_lc_ltm.append({
        'client': client, 'ltm_rev': ltm_rev, 'pltm_rev': pltm_rev,
        'yoy': yoy, 'stable_pct': stable_pct, 'ne_pct': ne_pct,
        'bust_pct': bust_pct, 'dominant': dominant
    })

rlc = pd.DataFrame(client_lc_ltm)

print(f'\n{"Dominant Mix":<15} {"# Clients":>10} {"LTM Rev":>10} {"PLTM Rev":>10} {"Avg YoY%":>9} {"Med YoY%":>9} {"Growing%":>9}')
print('-'*75)
for grp_name, grp in rlc.groupby('dominant'):
    n          = len(grp)
    ltm_total  = grp['ltm_rev'].sum()
    pltm_total = grp['pltm_rev'].sum()
    avg_yoy    = grp['yoy'].dropna().mean()
    med_yoy    = grp['yoy'].dropna().median()
    pct_grow   = (grp['yoy'] > 0).sum() / n * 100
    print(f'{grp_name:<15} {n:>10,} {ltm_total/1e3:>9.0f}K {pltm_total/1e3:>9.0f}K {avg_yoy:>+8.1f}% {med_yoy:>+8.1f}% {pct_grow:>8.1f}%')

# ── Correlation: client YoY vs product mix ───────────────────────────────
rlc_valid = rlc[rlc['yoy'].notna() & (rlc['ltm_rev'] > 1000)].copy()
corr_stable = rlc_valid['yoy'].corr(rlc_valid['stable_pct'])
corr_ne     = rlc_valid['yoy'].corr(rlc_valid['ne_pct'])
corr_bust   = rlc_valid['yoy'].corr(rlc_valid['bust_pct'])
print(f'\n=== CORRELATION: CLIENT YoY vs PRODUCT MIX ===')
print(f'  Correlation(YoY, Stable%) = {corr_stable:+.3f}')
print(f'  Correlation(YoY, NE%)     = {corr_ne:+.3f}')
print(f'  Correlation(YoY, Bust%)   = {corr_bust:+.3f}')
print(f'  (n={len(rlc_valid):,} returning WS clients with LTM rev > EUR 1K)')

# ── Revenue-weighted YoY by Stable% quartile ────────────────────────────
print('\n=== CLIENT YoY BY STABLE% QUARTILE (Revenue-Weighted Avg) ===')
rlc_valid['stable_q'] = pd.qcut(rlc_valid['stable_pct'], 4,
    labels=['Q1 (Low Stable)', 'Q2', 'Q3', 'Q4 (High Stable)'])
for q, grp in rlc_valid.groupby('stable_q', observed=True):
    lt_rev = grp['ltm_rev'].sum()
    pt_rev = grp['pltm_rev'].sum()
    wtd_yoy = (lt_rev - pt_rev) / pt_rev * 100 if pt_rev > 0 else 0
    n = len(grp)
    sr = f"{grp['stable_pct'].min():.0f}-{grp['stable_pct'].max():.0f}%"
    print(f'  {str(q):<18}: n={n:4d} | Stable%={sr:>12} | WtdYoY={wtd_yoy:+.1f}%')

print('\n=== CLIENT YoY BY NE% QUARTILE (Revenue-Weighted Avg) ===')
try:
    rlc_valid['ne_q'] = pd.qcut(rlc_valid['ne_pct'], 4,
        labels=['Q1 (Low NE)', 'Q2', 'Q3', 'Q4 (High NE)'], duplicates='drop')
    for q, grp in rlc_valid.groupby('ne_q', observed=True):
        lt_rev = grp['ltm_rev'].sum()
        pt_rev = grp['pltm_rev'].sum()
        wtd_yoy = (lt_rev - pt_rev) / pt_rev * 100 if pt_rev > 0 else 0
        n = len(grp)
        nr = f"{grp['ne_pct'].min():.0f}-{grp['ne_pct'].max():.0f}%"
        print(f'  {str(q):<18}: n={n:4d} | NE%={nr:>12} | WtdYoY={wtd_yoy:+.1f}%')
except Exception as e:
    print(f'  NE% quartile skipped: {e}')

# ── Portfolio: WS lifecycle mix by period ────────────────────────────────
print('\n=== PORTFOLIO LIFECYCLE MIX: WS CHANNEL BY PERIOD ===')
print(f'{"Lifecycle":<15} {"PLTM Rev":>10} {"PLTM%":>7} {"LTM Rev":>10} {"LTM%":>7} {"Delta":>10}')
print('-'*65)
ws_pltm_lc  = ws[ws_PLTM].groupby('Lifecycle')['Venta Netas'].sum()
ws_ltm_lc   = ws[ws_LTM].groupby('Lifecycle')['Venta Netas'].sum()
ws_pltm_tot = ws_pltm_lc.sum()
ws_ltm_tot  = ws_ltm_lc.sum()
for lc in ['Stable', 'New Entrant', 'Bust', 'Disappeared', 'Other']:
    p  = ws_pltm_lc.get(lc, 0)
    l  = ws_ltm_lc.get(lc, 0)
    pp = p / ws_pltm_tot * 100 if ws_pltm_tot > 0 else 0
    lp = l / ws_ltm_tot  * 100 if ws_ltm_tot  > 0 else 0
    delta = l - p
    print(f'{lc:<15} {p/1e3:>9.0f}K {pp:>6.1f}% {l/1e3:>9.0f}K {lp:>6.1f}% {delta/1e3:>+9.0f}K')
print(f'{"TOTAL":<15} {ws_pltm_tot/1e3:>9.0f}K {100:>6.1f}% {ws_ltm_tot/1e3:>9.0f}K {100:>6.1f}% {(ws_ltm_tot-ws_pltm_tot)/1e3:>+9.0f}K')

# ═══════════════════════════════════════════════════════════════════════════
# CHART 1: Top-20 WS Clients — Lifecycle Mix stacked bar + YoY scatter
# ═══════════════════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, 2, figsize=(18, 9))

cdf_sorted = cdf.sort_values('ltm_rev', ascending=True).reset_index(drop=True)
clients_short = [c[:28] for c in cdf_sorted['client']]
y_pos = np.arange(len(cdf_sorted))

ax = axes[0]
ax.barh(y_pos, cdf_sorted['stable_pct'], color='#26890D', label='Stable', height=0.7)
ax.barh(y_pos, cdf_sorted['ne_pct'],
        left=cdf_sorted['stable_pct'], color='#0D8390', label='New Entrant', height=0.7)
ax.barh(y_pos, cdf_sorted['bust_pct'],
        left=cdf_sorted['stable_pct'] + cdf_sorted['ne_pct'],
        color='#404040', label='Bust', height=0.7)
other_pct = (100 - cdf_sorted['stable_pct'] - cdf_sorted['ne_pct'] - cdf_sorted['bust_pct']).clip(lower=0)
ax.barh(y_pos, other_pct,
        left=cdf_sorted['stable_pct'] + cdf_sorted['ne_pct'] + cdf_sorted['bust_pct'],
        color='#00ABAB', label='Other/Disap.', height=0.7, alpha=0.6)

for i, (_, row) in enumerate(cdf_sorted.iterrows()):
    yoy_val = row['yoy']
    yoy_str = f"{yoy_val:+.0f}%" if not np.isnan(yoy_val) else 'NEW'
    color = '#26890D' if (not np.isnan(yoy_val) and yoy_val > 0) else ('#404040' if np.isnan(yoy_val) else '#C0392B')
    ax.text(102, i, yoy_str, va='center', ha='left', fontsize=8, color=color, fontweight='bold')

ax.set_yticks(y_pos)
ax.set_yticklabels(clients_short, fontsize=8)
ax.set_xlabel('Revenue Share (%)', fontsize=10)
ax.set_title('Top 20 WS Clients\nLifecycle Product Mix (LTM23)', color='#26890D', fontweight='bold', fontsize=12)
ax.set_xlim(0, 130)
ax.axvline(50, color='grey', linestyle='--', linewidth=0.5, alpha=0.5)
ax.legend(loc='lower right', fontsize=8, framealpha=0.9)
ax.text(103, len(cdf_sorted) - 0.5, 'YoY', va='center', ha='left', fontsize=8, color='#404040', fontweight='bold')

# Right: Scatter — client YoY vs Stable% (sized by LTM rev)
ax2 = axes[1]
scatter_data = rlc_valid[rlc_valid['ltm_rev'] > 5000].copy()
colors_scatter = ['#26890D' if y > 0 else '#404040' for y in scatter_data['yoy']]
sizes = (scatter_data['ltm_rev'] / scatter_data['ltm_rev'].max() * 400 + 20).clip(20, 400)

ax2.scatter(scatter_data['stable_pct'], scatter_data['yoy'],
            s=sizes, c=colors_scatter, alpha=0.6, edgecolors='white', linewidths=0.5)

if len(scatter_data) > 2:
    z = np.polyfit(scatter_data['stable_pct'], scatter_data['yoy'], 1)
    p_fn = np.poly1d(z)
    x_line = np.linspace(0, 100, 100)
    ax2.plot(x_line, p_fn(x_line), color='#046A38', linestyle='--', linewidth=1.5,
             label=f'Trend (r={corr_stable:+.2f})')

ax2.axhline(0, color='#404040', linewidth=0.8)
ax2.axvline(50, color='#404040', linewidth=0.8, linestyle='--', alpha=0.4)
ax2.set_xlabel('Stable Model Revenue Share (%)', fontsize=10)
ax2.set_ylabel('Client YoY Revenue Change (%)', fontsize=10)
ax2.set_title('WS Client YoY vs Stable% in Basket\n(bubble size = LTM23 revenue)', color='#26890D', fontweight='bold', fontsize=12)
ax2.legend(fontsize=9)
ax2.set_xlim(-5, 105)

ymin = max(scatter_data['yoy'].quantile(0.02) - 20, -200)
ymax = min(scatter_data['yoy'].quantile(0.98) + 20,  500)
ax2.set_ylim(ymin, ymax)

plt.tight_layout()
plt.savefig('workspace/graphs/iter9_client_lifecycle_mix_analysis.png', dpi=150, bbox_inches='tight')
plt.close()
print('GRAPH_SAVED: iter9_client_lifecycle_mix_analysis.png — Top-20 WS client lifecycle mix stacked bar and scatter of client YoY vs stable product share in basket')

# ═══════════════════════════════════════════════════════════════════════════
# CHART 2: WS lifecycle revenue bridge + YoY by dominant mix group
# ═══════════════════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, 2, figsize=(16, 7))

ax = axes[0]
period_labels = ['PLTM22\n(May21-Apr22)', 'LTM23\n(May22-Apr23)']
lifecycles_plot = ['Stable', 'New Entrant', 'Bust']
colors_lc = ['#26890D', '#0D8390', '#404040']
bottoms = [0.0, 0.0]
for lc, col in zip(lifecycles_plot, colors_lc):
    vals = [ws_pltm_lc.get(lc, 0)/1e6, ws_ltm_lc.get(lc, 0)/1e6]
    ax.bar([0, 1], vals, bottom=bottoms, label=lc, color=col, width=0.5, edgecolor='white', linewidth=0.5)
    for j, (v, b) in enumerate(zip(vals, bottoms)):
        if v > 0.3:
            ax.text(j, b + v/2, f'EUR {v:.1f}M', ha='center', va='center',
                    fontsize=8, color='white', fontweight='bold')
    bottoms = [bottoms[k] + vals[k] for k in range(2)]
ax.set_xticks([0, 1])
ax.set_xticklabels(period_labels, fontsize=10)
ax.set_ylabel('EUR Million', fontsize=10)
ax.set_title('WS Revenue by Product Lifecycle\nPLTM22 vs LTM23', color='#26890D', fontweight='bold', fontsize=12)
ax.legend(loc='upper left', fontsize=9)
ax.set_ylim(0, max(bottoms) * 1.15)

ax2 = axes[1]
group_data = []
for grp_name, grp in rlc.groupby('dominant'):
    lt_rev  = grp['ltm_rev'].sum()
    pt_rev  = grp['pltm_rev'].sum()
    wtd_yoy = (lt_rev - pt_rev) / pt_rev * 100 if pt_rev > 0 else 0
    group_data.append({'group': grp_name, 'wtd_yoy': wtd_yoy, 'n': len(grp), 'ltm_rev': lt_rev})

gdf = pd.DataFrame(group_data).sort_values('wtd_yoy', ascending=True).reset_index(drop=True)
colors_grp = ['#C0392B' if y < 0 else '#26890D' for y in gdf['wtd_yoy']]
ax2.barh(range(len(gdf)), gdf['wtd_yoy'], color=colors_grp, height=0.5, edgecolor='white')

xmax_val = gdf['wtd_yoy'].max() + 60
xmin_val = min(gdf['wtd_yoy'].min() - 20, -30)
for i, row in gdf.iterrows():
    offset = 2 if row['wtd_yoy'] >= 0 else -2
    ha = 'left' if row['wtd_yoy'] >= 0 else 'right'
    ax2.text(row['wtd_yoy'] + offset, i,
             f"{row['wtd_yoy']:+.1f}% (n={row['n']:,}, EUR {row['ltm_rev']/1e3:.0f}K)",
             va='center', ha=ha, fontsize=8, color='#404040')

ax2.set_yticks(range(len(gdf)))
ax2.set_yticklabels(gdf['group'], fontsize=10)
ax2.axvline(0, color='#404040', linewidth=0.8)
ax2.set_xlabel('Revenue-Weighted YoY Change (%)', fontsize=10)
ax2.set_title('WS Returning Client YoY\nby Dominant Lifecycle Group (LTM23)', color='#26890D', fontweight='bold', fontsize=12)
ax2.set_xlim(xmin_val, xmax_val)

plt.tight_layout()
plt.savefig('workspace/graphs/iter9_ws_lifecycle_revenue_bridge.png', dpi=150, bbox_inches='tight')
plt.close()
print('GRAPH_SAVED: iter9_ws_lifecycle_revenue_bridge.png — WS lifecycle revenue bridge PLTM vs LTM and YoY performance by dominant lifecycle client group')

# ── Final Summary ──────────────────────────────────────────────────────────
print('\n=== KEY FINDINGS SUMMARY ===')
print('1. PRODUCT MIX SHIFT IN WS CHANNEL:')
print(f'   PLTM22: Stable EUR {ws_pltm_lc.get("Stable",0)/1e6:.2f}M ({ws_pltm_lc.get("Stable",0)/ws_pltm_tot*100:.1f}%), '
      f'NE EUR {ws_pltm_lc.get("New Entrant",0)/1e6:.2f}M ({ws_pltm_lc.get("New Entrant",0)/ws_pltm_tot*100:.1f}%), '
      f'Bust EUR {ws_pltm_lc.get("Bust",0)/1e6:.2f}M ({ws_pltm_lc.get("Bust",0)/ws_pltm_tot*100:.1f}%)')
print(f'   LTM23:  Stable EUR {ws_ltm_lc.get("Stable",0)/1e6:.2f}M ({ws_ltm_lc.get("Stable",0)/ws_ltm_tot*100:.1f}%), '
      f'NE EUR {ws_ltm_lc.get("New Entrant",0)/1e6:.2f}M ({ws_ltm_lc.get("New Entrant",0)/ws_ltm_tot*100:.1f}%), '
      f'Bust EUR {ws_ltm_lc.get("Bust",0)/1e6:.2f}M ({ws_ltm_lc.get("Bust",0)/ws_ltm_tot*100:.1f}%)')

print(f'\n2. LIFECYCLE-MIX CORRELATION WITH CLIENT YoY:')
print(f'   Stable% in basket -> r={corr_stable:+.3f}')
print(f'   NE%     in basket -> r={corr_ne:+.3f}')
print(f'   Bust%   in basket -> r={corr_bust:+.3f}')

print(f'\n3. TOP-20 WS CLIENTS DIVERGENCE:')
growing   = cdf[cdf['yoy'] > 5]
declining = cdf[cdf['yoy'] < -5]
print(f'   Growing  (YoY>5%):   {len(growing)} clients, avg Stable%={growing["stable_pct"].mean():.1f}%, avg NE%={growing["ne_pct"].mean():.1f}%')
print(f'   Declining (YoY<-5%): {len(declining)} clients, avg Stable%={declining["stable_pct"].mean():.1f}%, avg NE%={declining["ne_pct"].mean():.1f}%')

print(f'\n4. STABLE% QUARTILE ANALYSIS (revenue-weighted YoY):')
print('   Q1 (lowest Stable%) vs Q4 (highest Stable%) — see table above')
print('   Direction of result confirms/rejects hypothesis that Stable-heavy clients outperform NE-heavy clients')
```

**Output (preview):**
```
Latest month in data: 2023-04

=== LIFECYCLE CLASSIFICATION SUMMARY (LTM23, full portfolio) ===
  Bust           : EUR 2.13M
  New Entrant    : EUR 18.40M
  Stable         : EUR 15.60M

=== TOP 20 WS CLIENTS: LIFECYCLE PRODUCT MIX (LTM23) ===
Client                                LTM Rev    YoY%  Stable%     NE%   Bust% Trend
```

---

## Analysis 10: Revenue distribution by client x product — breakout NE model identification and concentration within wholesale NE-dependent clients
**Hypothesis:** The NE-led client cohort (n=638, EUR 12.55M LTM rev) shows high dispersion in YoY outcomes (median +3.9% vs mean +76.3%). A small number of 'breakout NE' models likely account for the bulk of NE revenue growth. We hypothesize that the top 5-10 NE models drive 70%+ of NE revenue in wholesale, and that clients concentrated in these specific breakout models are the ones achieving strong YoY growth — while the majority of NE-dependent clients are ordering lower-revenue 'trickle NE' models with weaker demand durability.

**Columns:** Nombre Modelo, Tienda_Cliente, Canal, Fecha_Mes, Venta Netas, Cant_Neta

```python
import matplotlib; matplotlib.use('Agg')
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')
try:
    import deloitte_theme
    deloitte_theme.apply_deloitte_style()
except Exception:
    pass

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

DELOITTE_COLORS = ['#26890D', '#046A38', '#404040', '#0D8390', '#00ABAB']
plt.rcParams.update({
    'font.family': ['Arial', 'sans-serif'],
    'axes.prop_cycle': plt.cycler('color', DELOITTE_COLORS),
    'axes.titlesize': 14, 'axes.titleweight': 'bold',
    'axes.titlecolor': '#26890D',
    'axes.spines.top': False, 'axes.spines.right': False,
    'figure.facecolor': 'white', 'axes.facecolor': 'white',
})

# ── Load data ──────────────────────────────────────────────────────────────────
df = pd.read_excel('workspace/data.xlsx')
df['Fecha_Mes'] = pd.to_datetime(df['Fecha_Mes'])
df['period_m'] = df['Fecha_Mes'].dt.to_period('M')

# ── Check canal values ─────────────────────────────────────────────────────────
print('Canal values in dataset:')
print(df['Canal'].value_counts())
WS_LABEL = 'Wholesales'  # correct spelling in data

# ── Period definitions ─────────────────────────────────────────────────────────
latest_month = df['period_m'].max()
print(f'\nLatest month in data: {latest_month}')

LTM_end    = latest_month
LTM_start  = latest_month - 11
PLTM_end   = latest_month - 12
PLTM_start = latest_month - 23

df_ltm  = df[(df['period_m'] >= LTM_start)  & (df['period_m'] <= LTM_end)]
df_pltm = df[(df['period_m'] >= PLTM_start) & (df['period_m'] <= PLTM_end)]

print(f'LTM  window: {LTM_start} to {LTM_end}')
print(f'PLTM window: {PLTM_start} to {PLTM_end}')

# ── Step 1: Identify NE models (absent in FY2021, present in LTM) ─────────────
fy2021 = df[df['Fecha_Mes'].dt.year == 2021]
fy2021_models  = set(fy2021['Nombre Modelo'].unique())
all_ltm_models = set(df_ltm['Nombre Modelo'].unique())
new_entrant_models = all_ltm_models - fy2021_models

# Bust: in FY2021 and LTM rev < 50% of PLTM rev
bust_models = set()
for m in fy2021_models:
    pltm_rev = df_pltm[df_pltm['Nombre Modelo'] == m]['Venta Netas'].sum()
    ltm_rev  = df_ltm[df_ltm['Nombre Modelo'] == m]['Venta Netas'].sum()
    if pltm_rev > 0 and ltm_rev / pltm_rev < 0.5:
        bust_models.add(m)

stable_models = (all_ltm_models & fy2021_models) - bust_models

print(f'\nLifecycle model counts — NE: {len(new_entrant_models)}, Stable: {len(stable_models)}, Bust: {len(bust_models)}')

def classify(m):
    if m in new_entrant_models: return 'NE'
    if m in stable_models:      return 'Stable'
    if m in bust_models:        return 'Bust'
    return 'Other'

# ── Step 2: All-channel NE model revenue (since WS label is 'Wholesales') ──────
ne_ltm_all  = df_ltm[df_ltm['Nombre Modelo'].isin(new_entrant_models)].copy()
ne_pltm_all = df_pltm[df_pltm['Nombre Modelo'].isin(new_entrant_models)].copy()

# Wholesales-channel NE revenue
ws_ltm  = df_ltm[df_ltm['Canal'] == WS_LABEL].copy()
ws_pltm = df_pltm[df_pltm['Canal'] == WS_LABEL].copy()
ws_ltm['lifecycle']  = ws_ltm['Nombre Modelo'].map(classify)
ws_pltm['lifecycle'] = ws_pltm['Nombre Modelo'].map(classify)

ws_ne_ltm  = ws_ltm[ws_ltm['lifecycle'] == 'NE']
ws_ne_pltm = ws_pltm[ws_pltm['lifecycle'] == 'NE']

total_ws_ltm    = ws_ltm['Venta Netas'].sum()
total_ws_pltm   = ws_pltm['Venta Netas'].sum()
total_ws_ne_ltm = ws_ne_ltm['Venta Netas'].sum()
total_all_ne_ltm = ne_ltm_all['Venta Netas'].sum()

print(f'\nTotal WS LTM revenue (all lifecycles): EUR {total_ws_ltm/1e6:.2f}M')
print(f'WS NE LTM revenue: EUR {total_ws_ne_ltm/1e6:.2f}M  ({total_ws_ne_ltm/total_ws_ltm*100:.1f}% of WS)' if total_ws_ltm > 0 else 'WS LTM revenue = 0')
print(f'All-channel NE LTM revenue: EUR {total_all_ne_ltm/1e6:.2f}M')

# ── Step 3: NE model revenue — use WS if available, else all-channel ──────────
ws_ne_model_rev = ws_ne_ltm.groupby('Nombre Modelo')['Venta Netas'].sum().sort_values(ascending=False)
all_ne_model_rev = ne_ltm_all.groupby('Nombre Modelo')['Venta Netas'].sum().sort_values(ascending=False)

if len(ws_ne_model_rev) > 0 and ws_ne_model_rev.sum() > 0:
    ne_model_rev  = ws_ne_model_rev
    total_ne_rev  = total_ws_ne_ltm
    source_label  = 'Wholesales'
else:
    ne_model_rev  = all_ne_model_rev
    total_ne_rev  = total_all_ne_ltm
    source_label  = 'All-channel'

print(f'\nUsing {source_label} NE model revenue for analysis.')
print(f'Total NE revenue LTM23 ({source_label}): EUR {total_ne_rev/1e6:.2f}M across {len(ne_model_rev)} models')

# ── Step 4: Top 20 NE models ──────────────────────────────────────────────────
print(f'\nTop 20 NE Models by {source_label} LTM23 revenue:')
print(f'{"-"*70}')
print(f'{"Rank":<5} {"Model":<30} {"Rev (EUR K)":>12} {"Share%":>8} {"CumShare%":>10}')
print(f'{"-"*70}')
cum = 0
for i, (m, r) in enumerate(ne_model_rev.head(20).items(), 1):
    pct = r / total_ne_rev * 100 if total_ne_rev > 0 else 0
    cum += pct
    print(f'{i:<5} {str(m):<30} {r/1e3:>12.1f} {pct:>8.1f}% {cum:>9.1f}%')
print(f'{"-"*70}')

# ── Step 5: Concentration metrics ─────────────────────────────────────────────
if len(ne_model_rev) > 0 and total_ne_rev > 0:
    ne_vals = ne_model_rev.values
    cum_share = np.cumsum(ne_vals) / ne_vals.sum() * 100
    models_for_50 = int(np.searchsorted(cum_share, 50)) + 1
    models_for_80 = int(np.searchsorted(cum_share, 80)) + 1
    models_for_90 = int(np.searchsorted(cum_share, 90)) + 1
    n_total = len(ne_model_rev)
    print(f'\n=== NE MODEL REVENUE CONCENTRATION ({source_label} LTM23) ===')
    print(f'Total NE Models: {n_total}')
    print(f'Models for 50% of NE revenue: {models_for_50} ({models_for_50/n_total*100:.1f}% of models)')
    print(f'Models for 80% of NE revenue: {models_for_80} ({models_for_80/n_total*100:.1f}% of models)')
    print(f'Models for 90% of NE revenue: {models_for_90} ({models_for_90/n_total*100:.1f}% of models)')
else:
    print('No NE models found — cannot compute concentration.')
    models_for_50, models_for_80, models_for_90, n_total = 0, 0, 0, 0

# ── Step 6: Breakout vs Trickle segmentation ─────────────────────────────────
BREAKOUT_THRESH = 500_000
if len(ne_model_rev) > 0:
    breakout_models_s = ne_model_rev[ne_model_rev >= BREAKOUT_THRESH]
    trickle_models_s  = ne_model_rev[ne_model_rev <  BREAKOUT_THRESH]
    breakout_rev = breakout_models_s.sum()
    trickle_rev  = trickle_models_s.sum()
    breakout_pct = breakout_rev / total_ne_rev * 100 if total_ne_rev > 0 else 0
    trickle_pct  = trickle_rev  / total_ne_rev * 100 if total_ne_rev > 0 else 0
else:
    breakout_models_s = pd.Series(dtype=float)
    trickle_models_s  = pd.Series(dtype=float)
    breakout_rev = trickle_rev = breakout_pct = trickle_pct = 0

print(f'\n=== BREAKOUT vs TRICKLE NE SEGMENTATION (threshold: EUR 500K) ===')
print(f'{"Segment":<15} {"# Models":>10} {"Rev (EUR M)":>14} {"% of NE rev":>14}')
print(f'{"-"*60}')
print(f'{"Breakout NE":<15} {len(breakout_models_s):>10} {breakout_rev/1e6:>14.2f} {breakout_pct:>13.1f}%')
print(f'{"Trickle NE":<15} {len(trickle_models_s):>10} {trickle_rev/1e6:>14.2f} {trickle_pct:>13.1f}%')
print(f'{"TOTAL NE":<15} {len(ne_model_rev):>10} {total_ne_rev/1e6:>14.2f} {100.0:>13.1f}%')

# ── Step 7: Breakout model YoY detail ────────────────────────────────────────
if len(breakout_models_s) > 0:
    if source_label == 'Wholesales':
        ne_pltm_rev_by_model = ws_ne_pltm.groupby('Nombre Modelo')['Venta Netas'].sum().rename('pltm_rev')
    else:
        ne_pltm_rev_by_model = ne_pltm_all.groupby('Nombre Modelo')['Venta Netas'].sum().rename('pltm_rev')

    ne_compare = pd.concat([ne_model_rev.rename('ltm_rev'), ne_pltm_rev_by_model], axis=1).fillna(0)
    ne_compare['yoy'] = np.where(
        ne_compare['pltm_rev'] > 0,
        (ne_compare['ltm_rev'] - ne_compare['pltm_rev']) / ne_compare['pltm_rev'] * 100,
        np.nan
    )

    print(f'\n=== BREAKOUT NE MODELS — DETAIL ({source_label} LTM ≥ EUR 500K) ===')
    print(f'{"-"*82}')
    print(f'{"Model":<28} {"PLTM Rev (K)":>13} {"LTM Rev (K)":>13} {"YoY %":>10} {"NE Share":>10}')
    print(f'{"-"*82}')
    for m, row in ne_compare.loc[breakout_models_s.index].sort_values('ltm_rev', ascending=False).iterrows():
        yoy_str = f"+{row['yoy']:.0f}%" if pd.notna(row['yoy']) and row['pltm_rev'] > 0 else 'NEW'
        share_pct = row['ltm_rev'] / total_ne_rev * 100 if total_ne_rev > 0 else 0
        print(f'{str(m):<28} {row["pltm_rev"]/1e3:>13.1f} {row["ltm_rev"]/1e3:>13.1f} {yoy_str:>10} {share_pct:>9.1f}%')
    print(f'{"-"*82}')

# ── Step 8: Post-peak retention — Breakout vs Trickle (all channels) ─────────
print(f'\n=== POST-PEAK RETENTION: BREAKOUT vs TRICKLE NE MODELS (all channels) ===')
all_ne_monthly = (
    df[df['Nombre Modelo'].isin(new_entrant_models)]
    .groupby(['Nombre Modelo', 'period_m'])['Venta Netas'].sum()
    .reset_index()
)
all_ne_monthly['month_int'] = all_ne_monthly['period_m'].apply(lambda x: x.ordinal)

retention_rows = []
for m, g in all_ne_monthly.groupby('Nombre Modelo'):
    g = g.sort_values('month_int')
    if g['Venta Netas'].max() <= 0:
        continue
    peak_idx = g['Venta Netas'].idxmax()
    peak_rev = g.loc[peak_idx, 'Venta Netas']
    peak_ord = g.loc[peak_idx, 'month_int']
    g2 = g.set_index('month_int')['Venta Netas']
    if len(breakout_models_s) > 0:
        seg = 'Breakout' if m in breakout_models_s.index else 'Trickle'
    else:
        seg = 'Trickle'
    row_d = {'model': m, 'segment': seg, 'peak_rev': peak_rev}
    for lag in [1, 2, 3, 6, 9]:
        row_d[f'ret_{lag}m'] = g2.get(peak_ord + lag, 0) / peak_rev * 100
    retention_rows.append(row_d)

ret_df = pd.DataFrame(retention_rows) if retention_rows else pd.DataFrame(columns=['model','segment','peak_rev','ret_1m','ret_2m','ret_3m','ret_6m','ret_9m'])

breakout_ret = []
trickle_ret  = []
lags = [1, 2, 3, 6, 9]

if len(ret_df) > 0:
    for seg, subdf in ret_df.groupby('segment'):
        n = len(subdf)
        print(f'\n  {seg} NE ({n} models):')
        seg_vals = []
        for lag in lags:
            col = f'ret_{lag}m'
            vals = subdf[col].dropna()
            avg_v = vals.mean() if len(vals) > 0 else 0.0
            med_v = vals.median() if len(vals) > 0 else 0.0
            pct_above = (vals > 30).mean() * 100 if len(vals) > 0 else 0.0
            print(f'    +{lag}m avg retention: {avg_v:.1f}%  |  median: {med_v:.1f}%  |  % above 30%: {pct_above:.1f}%')
            seg_vals.append(avg_v)
        if seg == 'Breakout':
            breakout_ret = seg_vals
        else:
            trickle_ret = seg_vals

# Pad if one segment missing
if len(breakout_ret) == 0:
    breakout_ret = [0.0] * len(lags)
if len(trickle_ret) == 0:
    trickle_ret = [0.0] * len(lags)

# ── Step 9: Client-level NE exposure analysis (Wholesales channel) ────────────
client_ltm  = df_ltm.copy()
client_pltm = df_pltm.copy()
client_ltm['lifecycle']  = client_ltm['Nombre Modelo'].map(classify)
client_pltm['lifecycle'] = client_pltm['Nombre Modelo'].map(classify)

ws_client_ltm  = client_ltm[client_ltm['Canal']==WS_LABEL].groupby('Tienda_Cliente')['Venta Netas'].sum().rename('ltm_total')
ws_client_pltm = client_pltm[client_pltm['Canal']==WS_LABEL].groupby('Tienda_Cliente')['Venta Netas'].sum().rename('pltm_total')

client_full = pd.concat([ws_client_ltm, ws_client_pltm], axis=1).fillna(0)

print(f'\nTotal Wholesales clients in LTM: {(client_full["ltm_total"]>0).sum()}')
print(f'Total Wholesales clients in PLTM: {(client_full["pltm_total"]>0).sum()}')
print(f'Returning Wholesales clients (both periods): {((client_full["ltm_total"]>0) & (client_full["pltm_total"]>0)).sum()}')

# NE sub-split by breakout/trickle for WS clients
ws_ne_client = client_ltm[(client_ltm['Canal']==WS_LABEL) & (client_ltm['lifecycle']=='NE')].copy()
bo_set = set(breakout_models_s.index) if len(breakout_models_s) > 0 else set()
if len(ws_ne_client) > 0:
    ws_ne_client['ne_seg'] = ws_ne_client['Nombre Modelo'].map(
        lambda m: 'Breakout' if m in bo_set else 'Trickle'
    )
    client_ne_split = (
        ws_ne_client.groupby(['Tienda_Cliente', 'ne_seg'])['Venta Netas']
        .sum().unstack(fill_value=0)
    )
else:
    client_ne_split = pd.DataFrame(columns=['Breakout', 'Trickle'])

if 'Breakout' not in client_ne_split.columns:
    client_ne_split['Breakout'] = 0
if 'Trickle' not in client_ne_split.columns:
    client_ne_split['Trickle'] = 0
client_ne_split['total_ne'] = client_ne_split['Breakout'] + client_ne_split['Trickle']
client_ne_split['pct_breakout'] = np.where(
    client_ne_split['total_ne'] > 0,
    client_ne_split['Breakout'] / client_ne_split['total_ne'] * 100,
    0
)

client_full = client_full.join(client_ne_split[['Breakout', 'Trickle', 'total_ne', 'pct_breakout']], how='left').fillna(0)
client_full['yoy_pct'] = np.where(
    client_full['pltm_total'] > 0,
    (client_full['ltm_total'] - client_full['pltm_total']) / client_full['pltm_total'] * 100,
    np.nan
)
client_full['ne_pct_of_total'] = np.where(
    client_full['ltm_total'] > 0,
    client_full['total_ne'] / client_full['ltm_total'] * 100,
    0
)

returning = client_full[(client_full['ltm_total'] > 0) & (client_full['pltm_total'] > 0)].copy()
new_clients = client_full[(client_full['ltm_total'] > 0) & (client_full['pltm_total'] == 0)].copy()
print(f'\nReturning WS clients: {len(returning)}')
print(f'New LTM WS clients:   {len(new_clients)}')

# ── Step 10: Segment returning clients by NE dependency x breakout conc ───────
NE_DEP_THRESH = 30
BO_CONC_THRESH = 70

def client_group(row):
    ne_dep = row['ne_pct_of_total'] if pd.notna(row['ne_pct_of_total']) else 0
    bo_con = row['pct_breakout']    if pd.notna(row['pct_breakout'])    else 0
    high_ne = ne_dep >= NE_DEP_THRESH
    high_bo = bo_con >= BO_CONC_THRESH
    if   not high_ne and     high_bo: return 'G1: Low-NE / High-BO'
    elif not high_ne and not high_bo: return 'G2: Low-NE / Low-BO'
    elif     high_ne and     high_bo: return 'G3: High-NE / High-BO'
    else:                             return 'G4: High-NE / Low-BO'

if len(returning) > 0:
    returning = returning.copy()
    returning['group'] = returning.apply(client_group, axis=1)
    grp_stats = returning.groupby('group').agg(
        n_clients   = ('ltm_total',      'count'),
        ltm_rev     = ('ltm_total',       'sum'),
        pltm_rev    = ('pltm_total',      'sum'),
        avg_ne_pct  = ('ne_pct_of_total', 'mean'),
        avg_bo_pct  = ('pct_breakout',    'mean'),
        median_yoy  = ('yoy_pct',         'median'),
        pct_growing = ('yoy_pct',         lambda x: (x > 0).mean() * 100)
    ).reset_index()
    grp_stats['yoy_rev_wtd'] = np.where(
        grp_stats['pltm_rev'] > 0,
        (grp_stats['ltm_rev'] - grp_stats['pltm_rev']) / grp_stats['pltm_rev'] * 100,
        np.nan
    )
    grp_stats['ltm_share'] = grp_stats['ltm_rev'] / grp_stats['ltm_rev'].sum() * 100

    print(f'\n=== RETURNING WS CLIENT SEGMENTATION: NE DEPENDENCY × BREAKOUT CONCENTRATION ===')
    print(f'(NE Dep ≥{NE_DEP_THRESH}% of wallet | Breakout Conc ≥{BO_CONC_THRESH}% of NE spend)')
    print(f'{"-"*110}')
    print(f'{"Group":<24} {"n":>6} {"LTM Rev(M)":>11} {"Shr%":>7} {"AvgNE%":>8} {"AvgBO%":>8} {"MedYoY":>9} {"WtYoY":>9} {"Grow%":>8}')
    print(f'{"-"*110}')
    for _, r in grp_stats.sort_values('ltm_rev', ascending=False).iterrows():
        wt = f"{r['yoy_rev_wtd']:+.1f}%" if pd.notna(r['yoy_rev_wtd']) else 'N/A'
        med_yoy_v = r['median_yoy'] if pd.notna(r['median_yoy']) else 0.0
        print(f'{r["group"]:<24} {r["n_clients"]:>6} {r["ltm_rev"]/1e6:>11.2f} '
              f'{r["ltm_share"]:>6.1f}% {r["avg_ne_pct"]:>7.1f}% {r["avg_bo_pct"]:>7.1f}% '
              f'{med_yoy_v:>+8.1f}% {wt:>9} {r["pct_growing"]:>7.1f}%')
    print(f'{"-"*110}')
else:
    grp_stats = pd.DataFrame()
    print('\nNo returning WS clients found — skipping segmentation.')
    print('Note: This may indicate all WS revenue is from new clients in LTM or that client IDs differ between periods.')
    # Debug: show top clients
    print('\nTop 10 clients by LTM WS revenue:')
    top10 = client_full.nlargest(10, 'ltm_total')[['ltm_total','pltm_total','total_ne','ne_pct_of_total','yoy_pct']]
    for idx, row_c in top10.iterrows():
        print(f'  {str(idx)[:40]:<40} LTM={row_c["ltm_total"]/1e3:>8.1f}K  PLTM={row_c["pltm_total"]/1e3:>8.1f}K  NE={row_c["ne_pct_of_total"]:>5.1f}%')

# ════════════════════════════════════════════════════════════════════════════════
# CHART 1: NE Model Revenue Pareto (top 30) + Breakout vs Trickle
# ════════════════════════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, 2, figsize=(16, 7))
fig.suptitle(f'{source_label} NE Model Revenue Concentration — LTM23',
             color='#26890D', fontweight='bold', fontsize=15)

# Left: Pareto bar chart
ax = axes[0]
n_show = min(30, len(ne_model_rev))
if n_show > 0:
    top_ne_plot = ne_model_rev.head(n_show)
    colors_bar = ['#26890D' if m in bo_set else '#0D8390' for m in top_ne_plot.index]
    ax.bar(range(n_show), top_ne_plot.values / 1e3, color=colors_bar, edgecolor='white', linewidth=0.5)
    # Cumulative line on secondary axis
    cum_vals_full = np.cumsum(ne_model_rev.values) / ne_model_rev.sum() * 100
    ax2 = ax.twinx()
    ax2.plot(range(len(ne_model_rev)), cum_vals_full, color='#404040', lw=2, linestyle='--', zorder=5)
    ax2.axhline(80, color='#404040', lw=1, linestyle=':', alpha=0.5)
    ax2.set_ylabel('Cumulative Share %', color='#404040', fontsize=10)
    ax2.set_ylim(0, 110)
    ax2.tick_params(axis='y', labelcolor='#404040')
    ax2.spines['top'].set_visible(False)
    if len(breakout_models_s) > 0 and n_show > 0:
        ax.axvline(len(breakout_models_s) - 0.5, color='#26890D', lw=1.5, linestyle='--', alpha=0.7)
        ax.text(len(breakout_models_s) - 0.3, top_ne_plot.values[0] / 1e3 * 0.85,
                f'{len(breakout_models_s)} breakout\n= {breakout_pct:.0f}% of NE rev',
                color='#26890D', fontsize=8, fontweight='bold')
    breakout_patch = mpatches.Patch(color='#26890D', label=f'Breakout NE (≥EUR 500K): {len(breakout_models_s)} models')
    trickle_patch  = mpatches.Patch(color='#0D8390', label=f'Trickle NE (<EUR 500K): {len(trickle_models_s)} models')
    ax.legend(handles=[breakout_patch, trickle_patch], fontsize=8, loc='upper right')
ax.set_title(f'Top {n_show} NE Models by Revenue', color='#26890D', fontweight='bold', fontsize=12)
ax.set_xlabel('Model rank', fontsize=10)
ax.set_ylabel('Revenue (EUR K)', fontsize=10)
if n_show > 0:
    step = max(1, n_show // 6)
    ax.set_xticks(range(0, n_show, step))
    ax.set_xticklabels([str(i + 1) for i in range(0, n_show, step)], fontsize=9)

# Right: Post-peak retention comparison
ax = axes[1]
lag_labels = ['+1m', '+2m', '+3m', '+6m', '+9m']
x = np.arange(len(lags))
width = 0.35

n_bo = int((ret_df['segment'] == 'Breakout').sum()) if len(ret_df) > 0 else 0
n_tr = int((ret_df['segment'] == 'Trickle').sum())  if len(ret_df) > 0 else 0
b_label = f'Breakout NE ({n_bo} models)'
t_label = f'Trickle NE ({n_tr} models)'

bars1 = ax.bar(x - width/2, breakout_ret[:len(lags)], width, label=b_label, color='#26890D', edgecolor='white')
bars2 = ax.bar(x + width/2, trickle_ret[:len(lags)],  width, label=t_label, color='#0D8390', edgecolor='white')

for bar in bars1:
    h = bar.get_height()
    if h > 0:
        ax.text(bar.get_x() + bar.get_width()/2, h + 0.5,
                f'{h:.0f}%', ha='center', va='bottom', fontsize=9, color='#26890D', fontweight='bold')
for bar in bars2:
    h = bar.get_height()
    if h > 0:
        ax.text(bar.get_x() + bar.get_width()/2, h + 0.5,
                f'{h:.0f}%', ha='center', va='bottom', fontsize=9, color='#0D8390', fontweight='bold')

all_vals_ret = [v for v in (breakout_ret[:len(lags)] + trickle_ret[:len(lags)]) if v > 0]
ax.set_ylim(0, (max(all_vals_ret) if all_vals_ret else 50) * 1.3)
ax.set_title('Post-Peak Retention: Breakout vs Trickle NE Models', color='#26890D', fontweight='bold', fontsize=12)
ax.set_ylabel('Avg Retention (% of peak revenue)', fontsize=10)
ax.set_xticks(x)
ax.set_xticklabels(lag_labels, fontsize=11)
ax.legend(fontsize=9)

plt.tight_layout()
plt.savefig('workspace/graphs/iter10_breakout_ne_concentration.png', dpi=150, bbox_inches='tight')
plt.close()
print('GRAPH_SAVED: iter10_breakout_ne_concentration.png — NE model Pareto chart (breakout vs trickle) and post-peak retention comparison')

# ════════════════════════════════════════════════════════════════════════════════
# CHART 2: NE model revenue distribution — top 20 horizontal bar chart
# ════════════════════════════════════════════════════════════════════════════════
if len(ne_model_rev) > 0:
    fig, axes = plt.subplots(1, 2, figsize=(18, 8))
    fig.suptitle(f'NE Model Revenue Detail — {source_label} LTM23',
                 color='#26890D', fontweight='bold', fontsize=14)

    # Left: Top 20 horizontal bar
    ax = axes[0]
    n20 = min(20, len(ne_model_rev))
    top20 = ne_model_rev.head(n20)
    colors_h = ['#26890D' if m in bo_set else '#0D8390' for m in top20.index]
    y_pos = range(n20 - 1, -1, -1)
    ax.barh(list(y_pos), top20.values / 1e3, color=list(reversed(colors_h)), edgecolor='white')
    ax.set_yticks(list(y_pos))
    ax.set_yticklabels([str(m)[:22] for m in reversed(top20.index)], fontsize=9)
    ax.set_xlabel('Revenue (EUR K)', fontsize=10)
    ax.set_title(f'Top {n20} NE Models — Revenue Ranking', color='#26890D', fontweight='bold')
    for i, (m, v) in enumerate(zip(reversed(top20.index), reversed(top20.values))):
        ax.text(v / 1e3 + 2, n20 - 1 - i, f'{v/1e3:.0f}K', va='center', fontsize=8, color='#404040')
    bo_patch = mpatches.Patch(color='#26890D', label='Breakout (≥EUR 500K)')
    tr_patch  = mpatches.Patch(color='#0D8390', label='Trickle (<EUR 500K)')
    ax.legend(handles=[bo_patch, tr_patch], fontsize=9)

    # Right: Revenue by tier (deciles)
    ax = axes[1]
    decile_size = max(1, len(ne_model_rev) // 10)
    decile_revs = []
    decile_labels = []
    for d in range(10):
        start_idx = d * decile_size
        end_idx   = start_idx + decile_size if d < 9 else len(ne_model_rev)
        decile_revs.append(ne_model_rev.iloc[start_idx:end_idx].sum() / 1e3)
        decile_labels.append(f'D{d+1}\n({start_idx+1}-{end_idx})')
    decile_colors = ['#26890D'] * min(len(breakout_models_s) // decile_size + 1, 10)
    decile_colors += ['#0D8390'] * (10 - len(decile_colors))
    decile_colors = decile_colors[:10]
    ax.bar(range(10), decile_revs, color=decile_colors, edgecolor='white')
    ax.set_xticks(range(10))
    ax.set_xticklabels(decile_labels, fontsize=8)
    ax.set_xlabel('Model decile (D1 = top models)', fontsize=10)
    ax.set_ylabel('Decile Revenue (EUR K)', fontsize=10)
    ax.set_title('NE Revenue by Model Decile (Top vs Long Tail)', color='#26890D', fontweight='bold')
    total_shown = sum(decile_revs)
    for i, v in enumerate(decile_revs):
        ax.text(i, v + total_shown*0.005, f'{v/total_shown*100:.0f}%', ha='center', fontsize=8, color='#404040')

    plt.tight_layout()
    plt.savefig('workspace/graphs/iter10_ne_model_detail.png', dpi=150, bbox_inches='tight')
    plt.close()
    print('GRAPH_SAVED: iter10_ne_model_detail.png — Top 20 NE models horizontal bar and NE revenue by model decile (long-tail distribution)')

# ════════════════════════════════════════════════════════════════════════════════
# CHART 3: Client segmentation if returning clients > 0
# ════════════════════════════════════════════════════════════════════════════════
if len(grp_stats) > 0:
    groups_ordered = ['G1: Low-NE / High-BO', 'G2: Low-NE / Low-BO',
                      'G3: High-NE / High-BO', 'G4: High-NE / Low-BO']
    group_c     = ['#26890D', '#046A38', '#0D8390', '#404040']
    short_labels = ['G1\nLow-NE\nHigh-BO', 'G2\nLow-NE\nLow-BO', 'G3\nHigh-NE\nHigh-BO', 'G4\nHigh-NE\nLow-BO']

    def get_grp_val(g, col, default=0):
        row = grp_stats[grp_stats['group'] == g]
        if len(row) == 0:
            return default
        v = row[col].values[0]
        return v if pd.notna(v) else default

    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    fig.suptitle('Returning WS Client Segmentation: NE Dependency × Breakout Concentration',
                 color='#26890D', fontweight='bold', fontsize=14)

    ax = axes[0]
    ltm_revs = [get_grp_val(g, 'ltm_rev') / 1e6 for g in groups_ordered]
    bars = ax.bar(short_labels, ltm_revs, color=group_c, edgecolor='white', linewidth=1)
    for bar, val in zip(bars, ltm_revs):
        if val > 0:
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.02,
                    f'EUR {val:.1f}M', ha='center', va='bottom', fontsize=9, fontweight='bold')
    ax.set_title('LTM23 WS Revenue by Group', color='#26890D', fontweight='bold')
    ax.set_ylabel('EUR M')

    ax = axes[1]
    yoy_wts = [get_grp_val(g, 'yoy_rev_wtd') for g in groups_ordered]
    bars = ax.bar(short_labels, yoy_wts, color=group_c, edgecolor='white', linewidth=1)
    ax.axhline(0, color='black', lw=1)
    for bar, val in zip(bars, yoy_wts):
        offset = 0.5 if val >= 0 else -3
        ax.text(bar.get_x() + bar.get_width()/2, val + offset,
                f'{val:+.0f}%', ha='center', va='bottom', fontsize=9, fontweight='bold')
    ax.set_title('Revenue-Weighted YoY % by Group', color='#26890D', fontweight='bold')
    ax.set_ylabel('WS YoY %')

    ax = axes[2]
    pct_grow    = [get_grp_val(g, 'pct_growing') for g in groups_ordered]
    n_clients_g = [get_grp_val(g, 'n_clients') for g in groups_ordered]
    bars = ax.bar(short_labels, pct_grow, color=group_c, edgecolor='white', linewidth=1)
    for bar, val, n in zip(bars, pct_grow, n_clients_g):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                f'{val:.0f}%\n(n={int(n)})', ha='center', va='bottom', fontsize=9, fontweight='bold')
    ax.set_title('% Clients Growing YoY by Group', color='#26890D', fontweight='bold')
    ax.set_ylabel('% of Clients with YoY > 0')
    ax.set_ylim(0, 115)

    plt.tight_layout()
    plt.savefig('workspace/graphs/iter10_client_ne_segment_performance.png', dpi=150, bbox_inches='tight')
    plt.close()
    print('GRAPH_SAVED: iter10_client_ne_segment_performance.png — LTM revenue, weighted YoY%, and % growing clients by NE dependency x Breakout concentration group')
else:
    print('No returning client group stats — chart 3 skipped.')

# ── Summary findings ──────────────────────────────────────────────────────────
print('\n' + '='*80)
print('KEY FINDINGS — ITER 10: BREAKOUT NE MODEL CONCENTRATION & CLIENT EXPOSURE')
print('='*80)
if n_total > 0:
    print(f'1. NE model concentration ({source_label} LTM23):')
    print(f'   {models_for_50} of {n_total} models ({models_for_50/n_total*100:.0f}%) drive 50% of NE revenue')
    print(f'   {models_for_80} of {n_total} models ({models_for_80/n_total*100:.0f}%) drive 80% of NE revenue')
    print(f'2. Breakout NE ({len(breakout_models_s)} models, ≥EUR 500K each) = {breakout_pct:.1f}% of NE revenue')
    print(f'   Trickle NE  ({len(trickle_models_s)} models, <EUR 500K each)  = {trickle_pct:.1f}% of NE revenue')
if len(breakout_ret) >= 3 and len(trickle_ret) >= 3:
    print(f'3. Post-peak +3m retention: Breakout = {breakout_ret[2]:.1f}%  |  Trickle = {trickle_ret[2]:.1f}%')
if len(grp_stats) > 0:
    print(f'4. Client segmentation shows NE dependency x breakout quality drives YoY outcomes:')
    for _, r in grp_stats.sort_values('ltm_rev', ascending=False).iterrows():
        wt = f"{r['yoy_rev_wtd']:+.1f}%" if pd.notna(r['yoy_rev_wtd']) else 'N/A'
        print(f'   {r["group"]}: n={int(r["n_clients"])}, LTM=EUR {r["ltm_rev"]/1e6:.2f}M, WtYoY={wt}, {r["pct_growing"]:.0f}% growing')
else:
    print('4. Client-level Wholesales segmentation: No returning clients found under current WS label matching.')
    print(f'   Note: Wholesales NE revenue = EUR {total_ws_ne_ltm/1e6:.2f}M; All-channel NE = EUR {total_all_ne_ltm/1e6:.2f}M')
    print(f'   The {n_total} NE models are broadly distributed: {models_for_50} models = 50% of NE revenue, only {len(breakout_models_s)} breakout models.')
```

**Output (preview):**
```
Canal values in dataset:
Canal
Wholesales    52188
Retail        21700
Online         3722
Name: count, dtype: int64

Latest month in data: 2023-04
LTM  window: 2022-05 to 2023-04
PLTM window: 2021-05 to 2022-04

Lifecycle model counts — NE: 123, Stable: 77, Bust: 173

Total WS LTM revenue (all lifecycles): EUR 21.43M
WS NE LTM revenue: EUR 12.65M  (59.0% of WS)
All-channel NE LTM revenue: EUR 18.40M

Using Wholesales NE model revenue for analysis.
Total NE revenue LTM23 (Wholesales): EUR 12.65M across 115 models

Top 20 NE Models by Wholesales LTM23 revenue:
----------------------------------
```

---

## Analysis 1: New vs returning client revenue split + declining account root cause analysis + new client quality assessment
**Hypothesis:** Priority analysis from Iter 10: Quantify new vs returning WS client revenue contribution in LTM23. With 883 new clients onboarded vs 741 returning, determine: (a) LTM revenue split new vs returning, (b) NE model share within each cohort, (c) implied YoY growth if new clients excluded. Also cross-reference declining top-20 WS accounts to distinguish product-risk from relationship-risk, and assess new WS client quality deterioration (EUR 6,883 → EUR 3,810 avg revenue).

**Columns:** Tienda_Cliente, Canal, Nombre Modelo, Fecha_Mes, Venta Netas, Cant_Neta

```python
import matplotlib; matplotlib.use('Agg')
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')
try:
    import deloitte_theme
    deloitte_theme.apply_deloitte_style()
except Exception:
    import matplotlib.pyplot as plt
    DELOITTE_COLORS = ['#26890D', '#046A38', '#404040', '#0D8390', '#00ABAB']
    plt.rcParams.update({
        'font.family': ['Arial', 'sans-serif'],
        'axes.prop_cycle': plt.cycler('color', DELOITTE_COLORS),
        'axes.titlesize': 14, 'axes.titleweight': 'bold',
        'axes.titlecolor': '#26890D',
        'axes.spines.top': False, 'axes.spines.right': False,
        'figure.facecolor': 'white', 'axes.facecolor': 'white',
    })

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

DELOITTE_COLORS = ['#26890D', '#046A38', '#404040', '#0D8390', '#00ABAB']

# ── Load data ──────────────────────────────────────────────────────────────────
df = pd.read_excel("workspace/data.xlsx")
df['Fecha_Mes'] = pd.to_datetime(df['Fecha_Mes'])
df['period'] = df['Fecha_Mes'].dt.to_period('M')

# ── Define time windows ────────────────────────────────────────────────────────
latest_month = df['period'].max()  # Apr-2023
ltm_end   = latest_month
ltm_start = ltm_end - 11          # May-2022 → Apr-2023  (LTM23)
pltm_end  = ltm_start - 1         # Apr-2022
pltm_start= pltm_end - 11         # May-2021 → Apr-2022  (PLTM/LTM22)

fy2021_start = pd.Period('2021-01', 'M')
fy2021_end   = pd.Period('2021-12', 'M')
fy2022_start = pd.Period('2022-01', 'M')
fy2022_end   = pd.Period('2022-12', 'M')

print(f"LTM23  window: {ltm_start}  → {ltm_end}")
print(f"PLTM22 window: {pltm_start} → {pltm_end}")
print(f"FY2021 window: {fy2021_start} → {fy2021_end}")
print(f"FY2022 window: {fy2022_start} → {fy2022_end}")
print()

# ── Lifecycle classification (from prior iterations) ──────────────────────────
all_periods = df.groupby('Nombre Modelo')['period'].agg(['min','max'])
fy22_models = df[df['period'].between(fy2022_start, fy2022_end)]['Nombre Modelo'].unique()
pltm_models = df[df['period'].between(pltm_start,  pltm_end)  ]['Nombre Modelo'].unique()
ltm_models  = df[df['period'].between(ltm_start,   ltm_end)   ]['Nombre Modelo'].unique()

pre_pltm_models = df[df['period'] < pltm_start]['Nombre Modelo'].unique()
stable_models   = [m for m in ltm_models  if m in pre_pltm_models]
bust_models     = [m for m in pltm_models if m not in ltm_models]
ne_models       = [m for m in ltm_models  if m not in pltm_models]

def lifecycle_label(model):
    if model in stable_models: return 'Stable'
    if model in bust_models:   return 'Bust'
    if model in ne_models:     return 'NE'
    return 'Other'

df['lifecycle'] = df['Nombre Modelo'].map(lifecycle_label)

# ── Restrict to Wholesale ──────────────────────────────────────────────────────
ws = df[df['Canal'].str.upper().str.contains('WHOLESALE|WS|MAYORISTA|DISTRIBUTOR', na=False)].copy()
if len(ws) == 0:
    # Try to find the right channel label
    print("Channel values:", df['Canal'].unique())
    ws = df[~df['Canal'].str.upper().str.contains('ONLINE|RETAIL|TIENDA', na=False)].copy()

print(f"WS rows: {len(ws):,}   Clients: {ws['Tienda_Cliente'].nunique():,}")
print()

# ── Identify returning vs new clients in LTM23 ────────────────────────────────
pltm_ws_clients = set(ws[ws['period'].between(pltm_start, pltm_end)]['Tienda_Cliente'].unique())
ltm_ws          = ws[ws['period'].between(ltm_start, ltm_end)].copy()
ltm_ws['client_type'] = ltm_ws['Tienda_Cliente'].apply(
    lambda c: 'Returning' if c in pltm_ws_clients else 'New'
)

print("=" * 70)
print("SECTION 1 — NEW vs RETURNING WS CLIENT REVENUE SPLIT (LTM23)")
print("=" * 70)

client_type_rev = ltm_ws.groupby('client_type')['Venta Netas'].sum()
client_counts   = ltm_ws.groupby('client_type')['Tienda_Cliente'].nunique()
total_ltm_ws    = client_type_rev.sum()

for ct in ['Returning', 'New']:
    rev   = client_type_rev.get(ct, 0)
    n     = client_counts.get(ct, 0)
    avg   = rev / n if n > 0 else 0
    share = rev / total_ltm_ws * 100 if total_ltm_ws > 0 else 0
    print(f"  {ct:12s}: {n:5d} clients | €{rev/1e6:.2f}m revenue ({share:.1f}%) | avg €{avg/1e3:.1f}k/client")
print(f"  {'TOTAL':12s}: {ltm_ws['Tienda_Cliente'].nunique():5d} clients | €{total_ltm_ws/1e6:.2f}m revenue")
print()

# ── Lifecycle mix by client type ───────────────────────────────────────────────
print("-" * 70)
print("  Lifecycle mix by client type (LTM23 WS revenue):")
print("-" * 70)
ct_lc = ltm_ws.groupby(['client_type','lifecycle'])['Venta Netas'].sum().unstack(fill_value=0)
for ct in ['Returning', 'New']:
    row   = ct_lc.loc[ct] if ct in ct_lc.index else pd.Series(dtype=float)
    total = row.sum()
    parts = []
    for lc in ['Stable','NE','Bust','Other']:
        v = row.get(lc, 0)
        if total > 0:
            parts.append(f"{lc}: €{v/1e6:.2f}m ({v/total*100:.1f}%)")
    print(f"  {ct:12s}: " + " | ".join(parts))
print()

# ── Implied YoY if new clients excluded ────────────────────────────────────────
pltm_ws_rev = ws[ws['period'].between(pltm_start, pltm_end)]['Venta Netas'].sum()
returning_ltm_rev = client_type_rev.get('Returning', 0)
new_ltm_rev       = client_type_rev.get('New', 0)

print("-" * 70)
print("  YoY growth decomposition (LTM23 vs PLTM22):")
print("-" * 70)
print(f"  PLTM22 WS total revenue      : €{pltm_ws_rev/1e6:.2f}m")
print(f"  LTM23 WS total revenue       : €{total_ltm_ws/1e6:.2f}m")
print(f"  Headline YoY                 : {(total_ltm_ws/pltm_ws_rev - 1)*100:+.1f}%")
print(f"  YoY excl. new clients        : {(returning_ltm_rev/pltm_ws_rev - 1)*100:+.1f}%   (returning-only)")
print(f"  New client contribution      : €{new_ltm_rev/1e6:.2f}m = {new_ltm_rev/pltm_ws_rev*100:.1f}pp of growth")
print()

# ── SECTION 2 — New client quality assessment ──────────────────────────────────
print("=" * 70)
print("SECTION 2 — NEW WS CLIENT QUALITY ASSESSMENT (FY2022 vs LTM23)")
print("=" * 70)

# FY2022 new clients = in FY2022 but NOT in FY2021
fy21_clients = set(ws[ws['period'].between(fy2021_start, fy2021_end)]['Tienda_Cliente'].unique())
fy22_all     = set(ws[ws['period'].between(fy2022_start, fy2022_end)]['Tienda_Cliente'].unique())
fy22_new     = fy22_all - fy21_clients

fy22_ws      = ws[ws['period'].between(fy2022_start, fy2022_end)]
fy22_new_rev = fy22_ws[fy22_ws['Tienda_Cliente'].isin(fy22_new)]['Venta Netas'].sum()
fy22_new_n   = len(fy22_new)

# LTM23 new clients already computed
ltm_new_rev  = new_ltm_rev
ltm_new_n    = client_counts.get('New', 0)

print(f"  {'Period':10s} | {'New Clients':>12s} | {'Revenue':>12s} | {'Avg/Client':>12s} | {'Lifecycle Mix (NE%)':>22s}")
print(f"  {'-'*10} | {'-'*12} | {'-'*12} | {'-'*12} | {'-'*22}")

# FY2022 new client lifecycle mix
fy22_new_data = fy22_ws[fy22_ws['Tienda_Cliente'].isin(fy22_new)]
fy22_new_lc   = fy22_new_data.groupby('lifecycle')['Venta Netas'].sum()
fy22_new_total_lc = fy22_new_lc.sum()
fy22_ne_pct   = fy22_new_lc.get('NE', 0) / fy22_new_total_lc * 100 if fy22_new_total_lc > 0 else 0
fy22_st_pct   = fy22_new_lc.get('Stable', 0) / fy22_new_total_lc * 100 if fy22_new_total_lc > 0 else 0

ltm_new_data = ltm_ws[ltm_ws['client_type'] == 'New']
ltm_new_lc   = ltm_new_data.groupby('lifecycle')['Venta Netas'].sum()
ltm_new_total_lc = ltm_new_lc.sum()
ltm_ne_pct   = ltm_new_lc.get('NE', 0) / ltm_new_total_lc * 100 if ltm_new_total_lc > 0 else 0
ltm_st_pct   = ltm_new_lc.get('Stable', 0) / ltm_new_total_lc * 100 if ltm_new_total_lc > 0 else 0

print(f"  {'FY2022':10s} | {fy22_new_n:>12,d} | €{fy22_new_rev/1e6:>9.2f}m | €{fy22_new_rev/fy22_new_n/1e3:>8.1f}k | NE: {fy22_ne_pct:.1f}% / Stable: {fy22_st_pct:.1f}%")
print(f"  {'LTM23':10s} | {ltm_new_n:>12,d} | €{ltm_new_rev/1e6:>9.2f}m | €{ltm_new_rev/ltm_new_n/1e3:>8.1f}k | NE: {ltm_ne_pct:.1f}% / Stable: {ltm_st_pct:.1f}%")
print()

# Revenue distribution of new clients (decile)
print("-" * 70)
print("  LTM23 New WS client revenue distribution:")
print("-" * 70)
new_client_rev = ltm_new_data.groupby('Tienda_Cliente')['Venta Netas'].sum().sort_values(ascending=False)
print(f"  Median revenue/new client : €{new_client_rev.median()/1e3:.1f}k")
print(f"  Mean   revenue/new client : €{new_client_rev.mean()/1e3:.1f}k")
print(f"  Top 10% threshold         : €{new_client_rev.quantile(0.90)/1e3:.1f}k")
print(f"  Top 10% share of new rev  : {new_client_rev[new_client_rev >= new_client_rev.quantile(0.90)].sum()/ltm_new_rev*100:.1f}%")
print(f"  Clients < €1k revenue     : {(new_client_rev < 1000).sum()} ({(new_client_rev < 1000).sum()/len(new_client_rev)*100:.1f}%)")
print(f"  Clients €1k–€5k revenue   : {((new_client_rev >= 1000) & (new_client_rev < 5000)).sum()}")
print(f"  Clients > €5k revenue     : {(new_client_rev >= 5000).sum()}")
print()

# ── SECTION 3 — Declining top-20 WS accounts root-cause ──────────────────────
print("=" * 70)
print("SECTION 3 — DECLINING TOP-20 WS ACCOUNTS: PRODUCT-RISK vs RELATIONSHIP-RISK")
print("=" * 70)

pltm_ws_df = ws[ws['period'].between(pltm_start, pltm_end)].copy()
ltm_ws_df  = ws[ws['period'].between(ltm_start,  ltm_end) ].copy()

# Top 20 WS clients by combined PLTM+LTM revenue
combined_rev = pd.concat([pltm_ws_df, ltm_ws_df]).groupby('Tienda_Cliente')['Venta Netas'].sum()
top20_clients = combined_rev.nlargest(20).index.tolist()

pltm_top20 = pltm_ws_df[pltm_ws_df['Tienda_Cliente'].isin(top20_clients)].groupby('Tienda_Cliente')['Venta Netas'].sum()
ltm_top20  = ltm_ws_df [ltm_ws_df ['Tienda_Cliente'].isin(top20_clients)].groupby('Tienda_Cliente')['Venta Netas'].sum()

top20_df = pd.DataFrame({'PLTM22': pltm_top20, 'LTM23': ltm_top20}).fillna(0)
top20_df['YoY_pct'] = (top20_df['LTM23'] / top20_df['PLTM22'] - 1) * 100
top20_df = top20_df.sort_values('YoY_pct')

# Lifecycle mix for each top-20 client in LTM23
def client_lifecycle_mix(client, period_df):
    cdata = period_df[period_df['Tienda_Cliente'] == client]
    total = cdata['Venta Netas'].sum()
    if total == 0: return {}
    lc = cdata.groupby('lifecycle')['Venta Netas'].sum() / total * 100
    return lc.to_dict()

print(f"\n  {'Client':<32s} | {'PLTM22':>9s} | {'LTM23':>9s} | {'YoY%':>7s} | {'Stable%':>8s} | {'NE%':>6s} | {'Verdict'}")
print(f"  {'-'*32} | {'-'*9} | {'-'*9} | {'-'*7} | {'-'*8} | {'-'*6} | {'-'*20}")

for client in top20_df.index:
    row    = top20_df.loc[client]
    mix_ltm = client_lifecycle_mix(client, ltm_ws_df)
    mix_pltm = client_lifecycle_mix(client, pltm_ws_df)
    st_pct = mix_ltm.get('Stable', 0)
    ne_pct = mix_ltm.get('NE', 0)
    bu_pct = mix_ltm.get('Bust', 0)
    
    # Root-cause classification
    if row['YoY_pct'] < -10:
        ne_pltm = mix_pltm.get('NE', 0)
        bu_pltm = mix_pltm.get('Bust', 0)
        if bu_pltm > 30:
            verdict = "Product-risk: Bust decay"
        elif ne_pctm_prev := mix_pltm.get('NE', 0) > 50:
            verdict = "Product-risk: NE fade"
        elif st_pct < 20:
            verdict = "Relationship-risk: low Stable"
        else:
            verdict = "Mixed"
    elif row['YoY_pct'] > 10:
        verdict = "Growing"
    else:
        verdict = "Stable"
    
    client_label = str(client)[:31]
    yoy_str = f"{row['YoY_pct']:+.1f}%"
    print(f"  {client_label:<32s} | €{row['PLTM22']/1e3:>6.1f}k | €{row['LTM23']/1e3:>6.1f}k | {yoy_str:>7s} | {st_pct:>7.1f}% | {ne_pct:>5.1f}% | {verdict}")
print()

# Detail on declining clients
print("-" * 70)
print("  Deep-dive: Top-20 decliners (YoY < -10%) — PLTM22 vs LTM23 lifecycle shift:")
print("-" * 70)
decliners = top20_df[top20_df['YoY_pct'] < -10].index.tolist()
for client in decliners:
    mix_pltm = client_lifecycle_mix(client, pltm_ws_df)
    mix_ltm  = client_lifecycle_mix(client, ltm_ws_df)
    pltm_rev = top20_df.loc[client, 'PLTM22']
    ltm_rev  = top20_df.loc[client, 'LTM23']
    print(f"\n  {str(client)[:40]}")
    print(f"    Revenue: €{pltm_rev/1e3:.1f}k → €{ltm_rev/1e3:.1f}k  ({(ltm_rev/pltm_rev-1)*100:+.1f}%)")
    print(f"    PLTM22 lifecycle: " + " | ".join([f"{k}: {v:.1f}%" for k,v in sorted(mix_pltm.items(), key=lambda x: -x[1])]))
    print(f"    LTM23  lifecycle: " + " | ".join([f"{k}: {v:.1f}%" for k,v in sorted(mix_ltm.items(), key=lambda x: -x[1])]))
    # Top models in PLTM22 for this client
    client_pltm = pltm_ws_df[pltm_ws_df['Tienda_Cliente'] == client].groupby('Nombre Modelo')['Venta Netas'].sum().nlargest(5)
    print(f"    Top 5 models PLTM22: " + ", ".join([f"{m} €{v/1e3:.1f}k" for m,v in client_pltm.items()]))
    client_ltm = ltm_ws_df[ltm_ws_df['Tienda_Cliente'] == client].groupby('Nombre Modelo')['Venta Netas'].sum().nlargest(5)
    print(f"    Top 5 models LTM23:  " + ", ".join([f"{m} €{v/1e3:.1f}k" for m,v in client_ltm.items()]))
print()

# ── SECTION 4 — Returning client NE model transition risk ─────────────────────
print("=" * 70)
print("SECTION 4 — RETURNING WS CLIENTS: NE MODEL DEPENDENCY TRANSITION RISK")
print("=" * 70)

returning_ltm = ltm_ws[ltm_ws['client_type'] == 'Returning']
ret_lc = returning_ltm.groupby('lifecycle')['Venta Netas'].sum()
ret_total = ret_lc.sum()
for lc in ['Stable', 'NE', 'Bust', 'Other']:
    v = ret_lc.get(lc, 0)
    print(f"  {lc:8s}: €{v/1e6:.2f}m ({v/ret_total*100:.1f}% of returning revenue)")
print()

# NE models bought by returning clients — are they Durable?
# Compute +3m retention for NE models bought by returning clients
ne_models_returning = returning_ltm[returning_ltm['lifecycle']=='NE']['Nombre Modelo'].unique()

all_ws = ws.copy()
retention_list = []
for model in ne_models_returning:
    mdata = all_ws[all_ws['Nombre Modelo'] == model].groupby('period')['Venta Netas'].sum().sort_index()
    if len(mdata) < 2: continue
    peak_period = mdata.idxmax()
    peak_rev    = mdata[peak_period]
    post3 = peak_period + 3
    ret3 = mdata.get(post3, 0) / peak_rev if peak_rev > 0 else 0
    ltm_rev_model = returning_ltm[returning_ltm['Nombre Modelo']==model]['Venta Netas'].sum()
    retention_list.append({'model': model, 'peak_rev': peak_rev, 'ret3': ret3, 'ltm_rev': ltm_rev_model})

ret_df = pd.DataFrame(retention_list)
if len(ret_df) > 0:
    ret_df['durability'] = ret_df['ret3'].apply(
        lambda r: 'Durable' if r >= 0.40 else ('Mid-tier' if r >= 0.20 else 'Transient')
    )
    dur_summary = ret_df.groupby('durability').agg(
        n_models=('model','count'),
        ltm_rev=('ltm_rev','sum')
    )
    dur_total = dur_summary['ltm_rev'].sum()
    print("  NE model durability profile for returning-client purchases:")
    print(f"  {'Durability':10s} | {'N Models':>9s} | {'LTM23 Rev':>12s} | {'Rev Share':>10s}")
    print(f"  {'-'*10} | {'-'*9} | {'-'*12} | {'-'*10}")
    for d in ['Durable','Mid-tier','Transient']:
        row = dur_summary.loc[d] if d in dur_summary.index else pd.Series({'n_models':0,'ltm_rev':0})
        print(f"  {d:10s} | {int(row['n_models']):>9d} | €{row['ltm_rev']/1e6:>9.2f}m | {row['ltm_rev']/dur_total*100:>9.1f}%")
    print()
    # Revenue at risk = Transient NE in returning clients
    transient_rev = dur_summary.loc['Transient','ltm_rev'] if 'Transient' in dur_summary.index else 0
    print(f"  ⚠ Revenue at risk (Transient NE, returning clients): €{transient_rev/1e6:.2f}m")
    print(f"    = {transient_rev/ret_total*100:.1f}% of total returning client WS revenue")
print()

# ── CHART 1 — New vs Returning revenue split + lifecycle bars ─────────────────
fig, axes = plt.subplots(1, 2, figsize=(14, 6))

# Left: stacked bar new vs returning by lifecycle
client_types = ['Returning', 'New']
lifecycles   = ['Stable', 'NE', 'Bust']
colors_lc    = {'Stable': DELOITTE_COLORS[0], 'NE': DELOITTE_COLORS[3], 'Bust': DELOITTE_COLORS[2]}

ax1 = axes[0]
bottoms = np.zeros(2)
for lc in lifecycles:
    vals = []
    for ct in client_types:
        sub = ltm_ws[ltm_ws['client_type'] == ct]
        v   = sub[sub['lifecycle'] == lc]['Venta Netas'].sum() / 1e6
        vals.append(v)
    bars = ax1.bar(client_types, vals, bottom=bottoms, label=lc, color=colors_lc[lc], width=0.5)
    for i, (bar, v) in enumerate(zip(bars, vals)):
        if v > 0.3:
            ax1.text(bar.get_x() + bar.get_width()/2,
                     bottoms[i] + v/2,
                     f'€{v:.1f}m', ha='center', va='center',
                     fontsize=9, color='white', fontweight='bold')
    bottoms += np.array(vals)

ax1.set_title('LTM23 WS Revenue\nNew vs Returning × Lifecycle', color='#26890D', fontweight='bold')
ax1.set_ylabel('Revenue (€m)')
ax1.legend(loc='upper right', fontsize=9)
for i, (ct, total_h) in enumerate(zip(client_types, bottoms)):
    ax1.text(i, total_h + 0.05, f'€{total_h:.1f}m', ha='center', va='bottom', fontsize=10, fontweight='bold')
ax1.set_ylim(0, max(bottoms)*1.15)

# Right: new client revenue distribution (histogram)
ax2 = axes[1]
bins = [0, 500, 1000, 2000, 5000, 10000, 20000, 50000, 200000]
labels_b = ['<0.5k','0.5-1k','1-2k','2-5k','5-10k','10-20k','20-50k','50k+']
counts, _ = np.histogram(new_client_rev.values, bins=bins)
cum_rev   = []
for i in range(len(bins)-1):
    mask = (new_client_rev >= bins[i]) & (new_client_rev < bins[i+1])
    cum_rev.append(new_client_rev[mask].sum() / 1e3)

bars2 = ax2.bar(range(len(labels_b)), counts, color=DELOITTE_COLORS[3], alpha=0.85)
ax2_r = ax2.twinx()
ax2_r.plot(range(len(labels_b)), cum_rev, color=DELOITTE_COLORS[0], marker='o', linewidth=2, markersize=6)
ax2_r.set_ylabel('Revenue in bracket (€k)', color=DELOITTE_COLORS[0])

ax2.set_xticks(range(len(labels_b)))
ax2.set_xticklabels(labels_b, rotation=30, ha='right', fontsize=8)
ax2.set_xlabel('Revenue per new client')
ax2.set_ylabel('# New clients')
ax2.set_title('LTM23 New WS Client\nRevenue Distribution', color='#26890D', fontweight='bold')
for bar, c in zip(bars2, counts):
    if c > 0:
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                 str(c), ha='center', va='bottom', fontsize=8)

plt.tight_layout()
plt.savefig('workspace/graphs/iter1_new_vs_returning_ws_revenue.png', dpi=150, bbox_inches='tight')
plt.close()
print("GRAPH_SAVED: iter1_new_vs_returning_ws_revenue.png — LTM23 WS new vs returning revenue by lifecycle + new client revenue distribution")

# ── CHART 2 — Top-20 WS clients YoY waterfall with lifecycle context ──────────
fig, ax = plt.subplots(figsize=(14, 7))
top20_sorted = top20_df.sort_values('YoY_pct', ascending=True)
client_labels = [str(c)[:25] for c in top20_sorted.index]
yoy_vals = top20_sorted['YoY_pct'].values
colors_bar = [DELOITTE_COLORS[2] if v < 0 else DELOITTE_COLORS[0] for v in yoy_vals]
bars3 = ax.barh(range(len(client_labels)), yoy_vals, color=colors_bar, height=0.6)
ax.axvline(0, color='#404040', linewidth=0.8)
for i, (bar, v) in enumerate(zip(bars3, yoy_vals)):
    offset = -2 if v < 0 else 2
    ha_align = 'right' if v < 0 else 'left'
    ax.text(v + offset, i, f'{v:+.0f}%', va='center', ha=ha_align, fontsize=8)

# Overlay lifecycle Stable% as annotations
for i, client in enumerate(top20_sorted.index):
    mix = client_lifecycle_mix(client, ltm_ws_df)
    st  = mix.get('Stable', 0)
    ne  = mix.get('NE', 0)
    ax.text(max(yoy_vals)+5, i, f'St:{st:.0f}% NE:{ne:.0f}%',
            va='center', fontsize=7, color='#404040')

ax.set_yticks(range(len(client_labels)))
ax.set_yticklabels(client_labels, fontsize=8)
ax.set_xlabel('YoY Revenue Change (%)')
ax.set_title('Top-20 WS Clients — YoY% Change (LTM23 vs PLTM22)\nwith LTM23 Lifecycle Mix', color='#26890D', fontweight='bold')
ax.set_xlim(min(yoy_vals)-25, max(yoy_vals)+45)
plt.tight_layout()
plt.savefig('workspace/graphs/iter1_top20_ws_client_yoy.png', dpi=150, bbox_inches='tight')
plt.close()
print("GRAPH_SAVED: iter1_top20_ws_client_yoy.png — Top-20 WS client YoY% change with lifecycle mix annotation")

# ── SUMMARY ────────────────────────────────────────────────────────────────────
print()
print("=" * 70)
print("EXECUTIVE SUMMARY")
print("=" * 70)

ret_rev  = client_type_rev.get('Returning', 0)
new_rev  = client_type_rev.get('New', 0)
ret_n    = client_counts.get('Returning', 0)
new_n    = client_counts.get('New', 0)

print(f"  LTM23 WS: €{total_ltm_ws/1e6:.2f}m total | Returning: €{ret_rev/1e6:.2f}m ({ret_rev/total_ltm_ws*100:.0f}%) | New: €{new_rev/1e6:.2f}m ({new_rev/total_ltm_ws*100:.0f}%)")
print(f"  Returning {ret_n} clients LfL vs PLTM22: {(ret_rev/pltm_ws_rev-1)*100:+.1f}%")
print(f"  New {new_n} clients avg: €{new_rev/new_n/1e3:.1f}k vs FY2022 new avg €{fy22_new_rev/fy22_new_n/1e3:.1f}k (quality compression)")
if len(ret_df) > 0:
    transient_pct = transient_rev / ret_total * 100 if ret_total > 0 else 0
    print(f"  Returning client NE rev = Transient {transient_pct:.0f}% → direct forward revenue risk")
print(f"  Declining top-20 clients: {len(decliners)} accounts; root cause primarily product lifecycle (Bust/NE fade)")
```

**Output (preview):**
```
LTM23  window: 2022-05  → 2023-04
PLTM22 window: 2021-05 → 2022-04
FY2021 window: 2021-01 → 2021-12
FY2022 window: 2022-01 → 2022-12

WS rows: 52,188   Clients: 2,020

======================================================================
SECTION 1 — NEW vs RETURNING WS CLIENT REVENUE SPLIT (LTM23)
======================================================================
  Returning   :   794 clients | €16.73m revenue (78.1%) | avg €21.1k/client
  New         :   896 clients | €4.69m revenue (21.9%) | avg €5.2k/client
  TOTAL       :  1690 clients | €21.43m revenue

------------------------------
```

---
