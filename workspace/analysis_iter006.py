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
import warnings
warnings.filterwarnings('ignore')

DELOITTE_COLORS = ['#26890D', '#046A38', '#404040', '#0D8390', '#00ABAB']

# ── Load data ──────────────────────────────────────────────────────────────────
df = pd.read_excel("workspace/data.xlsx")
df.columns = df.columns.str.strip()
df['Fecha_Mes'] = pd.to_datetime(df['Fecha_Mes'])
df['period'] = df['Fecha_Mes'].dt.to_period('M')

# ── Define periods ─────────────────────────────────────────────────────────────
latest_month = df['period'].max()  # 2023-04
ltm23_start = latest_month - 11    # 2022-05
ltm22_start = ltm23_start - 12     # 2021-05
fy21_start  = pd.Period('2021-01', 'M')
fy21_end    = pd.Period('2021-12', 'M')
fy22_start  = pd.Period('2022-01', 'M')
fy22_end    = pd.Period('2022-12', 'M')

mask_fy21  = (df['period'] >= fy21_start)  & (df['period'] <= fy21_end)
mask_fy22  = (df['period'] >= fy22_start)  & (df['period'] <= fy22_end)
mask_ltm23 = (df['period'] >= ltm23_start) & (df['period'] <= latest_month)
mask_ltm22 = (df['period'] >= ltm22_start) & (df['period'] < ltm23_start)

print(f"Periods defined:")
print(f"  FY21  : {fy21_start} - {fy21_end}")
print(f"  FY22  : {fy22_start} - {fy22_end}")
print(f"  LTM22 : {ltm22_start} - {ltm23_start-1}")
print(f"  LTM23 : {ltm23_start} - {latest_month}")
print()

# ── Helper: PxQ summary for a given subset ────────────────────────────────────
def pxq_summary(subset):
    rev   = subset['Venta Netas'].sum()
    units = subset['Cant_Neta'].sum()
    asp   = rev / units if units != 0 else np.nan
    return rev, units, asp

def pxq_decompose(rev0, units0, asp0, rev1, units1, asp1):
    """
    Delta Revenue = Delta_Volume_Effect + Delta_Price_Effect + Cross_Effect
    Volume effect  = ASP_0  * (Q1 - Q0)
    Price  effect  = Q0     * (P1 - P0)
    Cross  effect  = (P1-P0)*(Q1-Q0)  allocated to price
    """
    delta_rev    = rev1   - rev0
    delta_units  = units1 - units0
    delta_asp    = asp1   - asp0 if (not np.isnan(asp0) and not np.isnan(asp1)) else np.nan
    if np.isnan(asp0) or np.isnan(asp1):
        vol_effect   = np.nan
        price_total  = np.nan
    else:
        vol_effect   = asp0   * delta_units
        price_effect = units0 * delta_asp
        cross_effect = delta_asp * delta_units
        price_total  = price_effect + cross_effect
    vol_pct   = vol_effect  / abs(delta_rev) * 100 if (delta_rev != 0 and not np.isnan(vol_effect))  else np.nan
    price_pct = price_total / abs(delta_rev) * 100 if (delta_rev != 0 and not np.isnan(price_total)) else np.nan
    return {
        'rev0': rev0, 'units0': units0, 'asp0': asp0,
        'rev1': rev1, 'units1': units1, 'asp1': asp1,
        'delta_rev':    delta_rev,
        'vol_effect':   vol_effect,
        'price_effect': price_total,
        'vol_pct':      vol_pct,
        'price_pct':    price_pct,
    }

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 - Total Company PxQ: FY21 -> FY22 -> LTM23
# ═══════════════════════════════════════════════════════════════════════════════
print("=" * 70)
print("SECTION 1 - Total Company Price x Quantity Decomposition")
print("=" * 70)

rev_fy21,  u_fy21,  p_fy21  = pxq_summary(df[mask_fy21])
rev_fy22,  u_fy22,  p_fy22  = pxq_summary(df[mask_fy22])
rev_ltm23, u_ltm23, p_ltm23 = pxq_summary(df[mask_ltm23])
rev_ltm22, u_ltm22, p_ltm22 = pxq_summary(df[mask_ltm22])

# Compute CAGR
N_months = (latest_month - fy21_start).n
N_years  = N_months / 12
if rev_fy21 > 0:
    cagr_rev   = (rev_ltm23 / rev_fy21) ** (1 / N_years) - 1
    cagr_units = (u_ltm23   / u_fy21)   ** (1 / N_years) - 1
    cagr_asp   = (p_ltm23   / p_fy21)   ** (1 / N_years) - 1
else:
    cagr_rev = cagr_units = cagr_asp = np.nan

print(f"\n**Revenue, Volume & ASP Summary**")
print(f"| Metric | FY21 | FY22 | LTM23 | CAGR (N={N_years:.2f}y) |")
print(f"|:---|---:|---:|---:|---:|")
print(f"| Revenue (EUR k) | {rev_fy21/1e3:,.1f} | {rev_fy22/1e3:,.1f} | {rev_ltm23/1e3:,.1f} | {cagr_rev:.1%} |")
print(f"| Units (000s) | {u_fy21/1e3:,.1f} | {u_fy22/1e3:,.1f} | {u_ltm23/1e3:,.1f} | {cagr_units:.1%} |")
print(f"| Avg Selling Price (EUR) | {p_fy21:.2f} | {p_fy22:.2f} | {p_ltm23:.2f} | {cagr_asp:.1%} |")
print(f"Source: Internal Dataset")
print()

# Bridge FY21 -> FY22
b1 = pxq_decompose(rev_fy21, u_fy21, p_fy21, rev_fy22, u_fy22, p_fy22)
# Bridge LTM22 -> LTM23
b2 = pxq_decompose(rev_ltm22, u_ltm22, p_ltm22, rev_ltm23, u_ltm23, p_ltm23)

print("**PxQ Revenue Bridge: FY21 -> FY22**")
print(f"| Component | Amount (EUR k) | % of Revenue Change |")
print(f"|:---|---:|---:|")
print(f"| Base Period Revenue (FY21) | {b1['rev0']/1e3:,.1f} | - |")
print(f"| Volume Effect (DeltaQty x Base ASP) | {b1['vol_effect']/1e3:+,.1f} | {b1['vol_pct']:+.1f}% |")
print(f"| Price Effect (DeltaPrice x Base Qty + Cross) | {b1['price_effect']/1e3:+,.1f} | {b1['price_pct']:+.1f}% |")
print(f"| **Total Revenue Change** | **{b1['delta_rev']/1e3:+,.1f}** | **100.0%** |")
print(f"| **End Period Revenue (FY22)** | **{b1['rev1']/1e3:,.1f}** | - |")
print(f"Source: Internal Dataset")
print()

print("**PxQ Revenue Bridge: LTM22 -> LTM23**")
print(f"| Component | Amount (EUR k) | % of Revenue Change |")
print(f"|:---|---:|---:|")
print(f"| Base Period Revenue (LTM22) | {b2['rev0']/1e3:,.1f} | - |")
print(f"| Volume Effect (DeltaQty x Base ASP) | {b2['vol_effect']/1e3:+,.1f} | {b2['vol_pct']:+.1f}% |")
print(f"| Price Effect (DeltaPrice x Base Qty + Cross) | {b2['price_effect']/1e3:+,.1f} | {b2['price_pct']:+.1f}% |")
print(f"| **Total Revenue Change** | **{b2['delta_rev']/1e3:+,.1f}** | **100.0%** |")
print(f"| **End Period Revenue (LTM23)** | **{b2['rev1']/1e3:,.1f}** | - |")
print(f"Source: Internal Dataset")

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 - Channel-Level PxQ Decomposition
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("SECTION 2 - Channel-Level PxQ Decomposition (LTM22 -> LTM23)")
print("=" * 70)

channels = df['Canal'].dropna().unique()
channel_rows = []
for ch in sorted(channels):
    r_fy21_ch, u_fy21_ch, p_fy21_ch = pxq_summary(df[mask_fy21 & (df['Canal'] == ch)])
    r0, u0, p0 = pxq_summary(df[mask_ltm22 & (df['Canal'] == ch)])
    r1, u1, p1 = pxq_summary(df[mask_ltm23 & (df['Canal'] == ch)])
    b = pxq_decompose(r0, u0, p0, r1, u1, p1)
    asp_chg_pct = (p1/p0 - 1) if (p0 and not np.isnan(p0) and p0 != 0 and not np.isnan(p1)) else np.nan
    channel_rows.append({
        'Channel':        ch,
        'FY21_ASP':       p_fy21_ch,
        'LTM22_Rev_k':    r0/1e3,
        'LTM23_Rev_k':    r1/1e3,
        'LTM22_Units_k':  u0/1e3,
        'LTM23_Units_k':  u1/1e3,
        'LTM22_ASP':      p0,
        'LTM23_ASP':      p1,
        'Delta_Rev_k':    b['delta_rev']/1e3,
        'Vol_Effect_k':   b['vol_effect']/1e3,
        'Price_Effect_k': b['price_effect']/1e3,
        'Vol_Pct':        b['vol_pct'],
        'Price_Pct':      b['price_pct'],
        'ASP_Chg_Pct':    asp_chg_pct,
    })

ch_df = pd.DataFrame(channel_rows)

print("\n**Channel Revenue & ASP Summary (LTM22 -> LTM23)**")
print(f"| Channel | LTM22 Rev (EUR k) | LTM23 Rev (EUR k) | YoY Delta (EUR k) | LTM22 ASP (EUR) | LTM23 ASP (EUR) | ASP Delta% |")
print(f"|:---|---:|---:|---:|---:|---:|---:|")
total_r0 = ch_df['LTM22_Rev_k'].sum()
total_r1 = ch_df['LTM23_Rev_k'].sum()
for _, row in ch_df.iterrows():
    asp_chg_str = f"{row['ASP_Chg_Pct']:+.1%}" if not np.isnan(row['ASP_Chg_Pct']) else 'n/a'
    print(f"| {row['Channel']} | {row['LTM22_Rev_k']:,.1f} | {row['LTM23_Rev_k']:,.1f} | {row['Delta_Rev_k']:+,.1f} | {row['LTM22_ASP']:.2f} | {row['LTM23_ASP']:.2f} | {asp_chg_str} |")
print(f"| **Total** | **{total_r0:,.1f}** | **{total_r1:,.1f}** | **{total_r1-total_r0:+,.1f}** | - | - | - |")
print(f"Source: Internal Dataset")

print("\n**PxQ Bridge by Channel (LTM22 -> LTM23)**")
print(f"| Channel | Vol Effect (EUR k) | Price Effect (EUR k) | Vol % | Price % |")
print(f"|:---|---:|---:|---:|---:|")
for _, row in ch_df.iterrows():
    v_pct = f"{row['Vol_Pct']:+.1f}%" if not np.isnan(row['Vol_Pct']) else 'n/a'
    p_pct = f"{row['Price_Pct']:+.1f}%" if not np.isnan(row['Price_Pct']) else 'n/a'
    print(f"| {row['Channel']} | {row['Vol_Effect_k']:+,.1f} | {row['Price_Effect_k']:+,.1f} | {v_pct} | {p_pct} |")
print(f"Source: Internal Dataset")

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 - Top 10 SKU PxQ Decomposition (LTM22 -> LTM23)
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("SECTION 3 - Top 10 SKU PxQ Decomposition (LTM22 -> LTM23)")
print("=" * 70)

top_skus = (
    df[mask_ltm23]
    .groupby('Nombre Modelo')['Venta Netas']
    .sum()
    .nlargest(10)
    .index.tolist()
)

sku_rows = []
for sku in top_skus:
    r0, u0, p0 = pxq_summary(df[mask_ltm22 & (df['Nombre Modelo'] == sku)])
    r1, u1, p1 = pxq_summary(df[mask_ltm23 & (df['Nombre Modelo'] == sku)])
    b = pxq_decompose(r0, u0, p0, r1, u1, p1)
    rev_chg_pct = (r1/r0 - 1) if (r0 and r0 != 0) else np.nan
    asp_chg_pct = (p1/p0 - 1) if (p0 and not np.isnan(p0) and p0 != 0 and not np.isnan(p1)) else np.nan
    sku_rows.append({
        'SKU':            sku,
        'LTM22_Rev_k':    r0/1e3,
        'LTM23_Rev_k':    r1/1e3,
        'LTM22_Units':    u0,
        'LTM23_Units':    u1,
        'LTM22_ASP':      p0,
        'LTM23_ASP':      p1,
        'Delta_Rev_k':    b['delta_rev']/1e3,
        'Vol_Effect_k':   b['vol_effect']/1e3,
        'Price_Effect_k': b['price_effect']/1e3,
        'Rev_Chg_Pct':    rev_chg_pct,
        'ASP_Chg_Pct':    asp_chg_pct,
        'Vol_Pct':        b['vol_pct'],
        'Price_Pct':      b['price_pct'],
    })

sku_df = pd.DataFrame(sku_rows)

print("\n**Top 10 SKU Revenue & ASP (LTM22 -> LTM23)**")
print(f"| SKU | LTM22 Rev (EUR k) | LTM23 Rev (EUR k) | Rev YoY | LTM22 ASP (EUR) | LTM23 ASP (EUR) | ASP Delta% |")
print(f"|:---|---:|---:|---:|---:|---:|---:|")
for _, row in sku_df.iterrows():
    rev_pct_str = f"{row['Rev_Chg_Pct']:+.1%}" if not np.isnan(row['Rev_Chg_Pct']) else 'NEW'
    asp_pct_str = f"{row['ASP_Chg_Pct']:+.1%}" if not np.isnan(row['ASP_Chg_Pct']) else 'NEW'
    ltm22_asp_str = f"{row['LTM22_ASP']:.2f}" if (not np.isnan(row['LTM22_ASP']) and row['LTM22_ASP'] > 0) else '-'
    print(f"| {row['SKU']} | {row['LTM22_Rev_k']:,.1f} | {row['LTM23_Rev_k']:,.1f} | {rev_pct_str} | {ltm22_asp_str} | {row['LTM23_ASP']:.2f} | {asp_pct_str} |")
print(f"Source: Internal Dataset")

print("\n**PxQ Bridge for Top 10 SKUs (LTM22 -> LTM23)**")
print(f"| SKU | DeltaRev (EUR k) | Vol Effect (EUR k) | Price Effect (EUR k) | Vol % | Price % |")
print(f"|:---|---:|---:|---:|---:|---:|")
for _, row in sku_df.iterrows():
    v_eff_str = f"{row['Vol_Effect_k']:+,.1f}"   if not np.isnan(row['Vol_Effect_k'])   else 'n/a'
    p_eff_str = f"{row['Price_Effect_k']:+,.1f}" if not np.isnan(row['Price_Effect_k']) else 'n/a'
    v_pct_str = f"{row['Vol_Pct']:+.1f}%"         if not np.isnan(row['Vol_Pct'])         else 'n/a'
    p_pct_str = f"{row['Price_Pct']:+.1f}%"       if not np.isnan(row['Price_Pct'])       else 'n/a'
    print(f"| {row['SKU']} | {row['Delta_Rev_k']:+,.1f} | {v_eff_str} | {p_eff_str} | {v_pct_str} | {p_pct_str} |")
print(f"Source: Internal Dataset")

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 - Monthly ASP trend commentary
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("SECTION 4 - Monthly ASP Trend by Channel (Jan 2021 - Apr 2023)")
print("=" * 70)
print(f"\nTotal company LTM22 ASP: EUR{p_ltm22:.2f} | LTM23 ASP: EUR{p_ltm23:.2f} | Delta: {(p_ltm23/p_ltm22-1):+.2%}")
for _, row in ch_df.iterrows():
    asp_chg_str = f"{row['ASP_Chg_Pct']:+.2%}" if not np.isnan(row['ASP_Chg_Pct']) else 'n/a'
    print(f"  {row['Channel']}: LTM22 EUR{row['LTM22_ASP']:.2f} | LTM23 EUR{row['LTM23_ASP']:.2f} | Delta: {asp_chg_str}")

# Monthly ASP for chart
monthly_asp = (
    df.groupby(['period', 'Canal'])
    .apply(lambda x: x['Venta Netas'].sum() / x['Cant_Neta'].sum() if x['Cant_Neta'].sum() != 0 else np.nan)
    .reset_index()
)
monthly_asp.columns = ['period', 'Canal', 'ASP']

total_asp_ts = (
    df.groupby('period')
    .apply(lambda x: x['Venta Netas'].sum() / x['Cant_Neta'].sum() if x['Cant_Neta'].sum() != 0 else np.nan)
    .reset_index()
)
total_asp_ts.columns = ['period', 'ASP']

# ═══════════════════════════════════════════════════════════════════════════════
# CHARTS
# ═══════════════════════════════════════════════════════════════════════════════
DC = DELOITTE_COLORS

fig, axes = plt.subplots(2, 2, figsize=(16, 11))
fig.suptitle('Price x Quantity Decomposition: Revenue Growth Drivers',
             color='#26890D', fontweight='bold', fontsize=15, y=0.98)

# ── Chart 1: PxQ waterfall FY21 -> FY22 ──────────────────────────────────────
ax1 = axes[0, 0]
vals1   = [b1['rev0']/1e6, b1['vol_effect']/1e6, b1['price_effect']/1e6, b1['rev1']/1e6]
bots1   = [0, b1['rev0']/1e6, b1['rev0']/1e6 + b1['vol_effect']/1e6, 0]
cols1   = [DC[0],
           DC[0] if b1['vol_effect'] >= 0   else DC[2],
           DC[3] if b1['price_effect'] >= 0 else DC[2],
           DC[1]]
labels1 = ['FY21 Base', 'Volume\nEffect', 'Price\nEffect', 'FY22']
for i, (val, bot, col, lbl) in enumerate(zip(vals1, bots1, cols1, labels1)):
    if i in [0, 3]:
        ax1.bar(i, val, color=col, width=0.6, edgecolor='white', linewidth=0.5)
        ax1.text(i, val + 0.05, f'EUR{val:.1f}m', ha='center', va='bottom', fontsize=8.5, fontweight='bold')
    else:
        ax1.bar(i, val, bottom=bot, color=col, width=0.6, edgecolor='white', linewidth=0.5)
        sign = '+' if val >= 0 else ''
        ax1.text(i, bot + val/2, f'{sign}EUR{val:.2f}m', ha='center', va='center',
                 fontsize=8, color='white', fontweight='bold')
ax1.set_xticks(range(4))
ax1.set_xticklabels(labels1, fontsize=9)
ax1.set_title('FY21 -> FY22: PxQ Bridge', color='#26890D', fontweight='bold', fontsize=11)
ax1.set_ylabel('EURm')
ax1.yaxis.set_major_formatter(mticker.FormatStrFormatter('%.1f'))

# ── Chart 2: PxQ waterfall LTM22 -> LTM23 ────────────────────────────────────
ax2 = axes[0, 1]
vals2   = [b2['rev0']/1e6, b2['vol_effect']/1e6, b2['price_effect']/1e6, b2['rev1']/1e6]
bots2   = [0, b2['rev0']/1e6, b2['rev0']/1e6 + b2['vol_effect']/1e6, 0]
cols2   = [DC[0],
           DC[0] if b2['vol_effect'] >= 0   else DC[2],
           DC[3] if b2['price_effect'] >= 0 else DC[2],
           DC[1]]
labels2 = ['LTM22 Base', 'Volume\nEffect', 'Price\nEffect', 'LTM23']
for i, (val, bot, col, lbl) in enumerate(zip(vals2, bots2, cols2, labels2)):
    if i in [0, 3]:
        ax2.bar(i, val, color=col, width=0.6, edgecolor='white', linewidth=0.5)
        ax2.text(i, val + 0.15, f'EUR{val:.1f}m', ha='center', va='bottom', fontsize=8.5, fontweight='bold')
    else:
        ax2.bar(i, val, bottom=bot, color=col, width=0.6, edgecolor='white', linewidth=0.5)
        sign = '+' if val >= 0 else ''
        ax2.text(i, bot + val/2, f'{sign}EUR{val:.2f}m', ha='center', va='center',
                 fontsize=8, color='white', fontweight='bold')
ax2.set_xticks(range(4))
ax2.set_xticklabels(labels2, fontsize=9)
ax2.set_title('LTM22 -> LTM23: PxQ Bridge', color='#26890D', fontweight='bold', fontsize=11)
ax2.set_ylabel('EURm')
ax2.yaxis.set_major_formatter(mticker.FormatStrFormatter('%.1f'))

# ── Chart 3: Channel-level ASP comparison ─────────────────────────────────────
ax3 = axes[1, 0]
channel_list = sorted(channels)
fy21_asp_ch  = []
ltm22_asp_ch = []
ltm23_asp_ch = []
for ch in channel_list:
    _, _, _p21  = pxq_summary(df[mask_fy21  & (df['Canal'] == ch)])
    _, _, _pl22 = pxq_summary(df[mask_ltm22 & (df['Canal'] == ch)])
    _, _, _pl23 = pxq_summary(df[mask_ltm23 & (df['Canal'] == ch)])
    fy21_asp_ch.append(_p21  if not np.isnan(_p21)  else 0)
    ltm22_asp_ch.append(_pl22 if not np.isnan(_pl22) else 0)
    ltm23_asp_ch.append(_pl23 if not np.isnan(_pl23) else 0)

x = np.arange(len(channel_list))
w = 0.25
ax3.bar(x - w, fy21_asp_ch,  width=w, label='FY21',  color=DC[2], edgecolor='white')
ax3.bar(x,     ltm22_asp_ch, width=w, label='LTM22', color=DC[1], edgecolor='white')
ax3.bar(x + w, ltm23_asp_ch, width=w, label='LTM23', color=DC[0], edgecolor='white')
for i, (v21, v22, v23) in enumerate(zip(fy21_asp_ch, ltm22_asp_ch, ltm23_asp_ch)):
    ax3.text(i - w, v21 + 0.3, f'EUR{v21:.0f}', ha='center', va='bottom', fontsize=7.5)
    ax3.text(i,     v22 + 0.3, f'EUR{v22:.0f}', ha='center', va='bottom', fontsize=7.5)
    ax3.text(i + w, v23 + 0.3, f'EUR{v23:.0f}', ha='center', va='bottom', fontsize=7.5)
ax3.set_xticks(x)
ax3.set_xticklabels(channel_list, fontsize=9)
ax3.set_title('Avg Selling Price by Channel (FY21 / LTM22 / LTM23)', color='#26890D', fontweight='bold', fontsize=11)
ax3.set_ylabel('EUR per unit')
ax3.legend(fontsize=8)

# ── Chart 4: Monthly ASP trend (3M rolling) ───────────────────────────────────
ax4 = axes[1, 1]
monthly_asp_pivot = monthly_asp.pivot(index='period', columns='Canal', values='ASP')
monthly_asp_pivot.index = monthly_asp_pivot.index.to_timestamp()
total_asp_ts['period_ts'] = total_asp_ts['period'].dt.to_timestamp()

for i, ch in enumerate(sorted(monthly_asp_pivot.columns)):
    series = monthly_asp_pivot[ch].rolling(3, min_periods=1).mean()
    ax4.plot(series.index, series.values, label=ch, color=DC[i % len(DC)],
             linewidth=1.8, marker='o', markersize=3)
total_smooth = total_asp_ts.set_index('period_ts')['ASP'].rolling(3, min_periods=1).mean()
ax4.plot(total_smooth.index, total_smooth.values, label='Total', color='black',
         linewidth=2, linestyle='--')
ax4.set_title('Monthly ASP Trend by Channel (3M Rolling Avg)', color='#26890D', fontweight='bold', fontsize=11)
ax4.set_ylabel('EUR per unit')
ax4.legend(fontsize=8, ncol=2)
ax4.tick_params(axis='x', rotation=30)

fig.tight_layout(rect=[0, 0, 1, 0.96])
plt.savefig('workspace/graphs/iter12_pxq_decomposition.png', dpi=150, bbox_inches='tight')
plt.close()
print("\nGRAPH_SAVED: iter12_pxq_decomposition.png - PxQ decomposition: waterfall bridges FY21->FY22 and LTM22->LTM23, channel ASP comparison, and monthly ASP trend by channel")

# ── Chart 2: Top 10 SKU PxQ breakdown ─────────────────────────────────────────
from matplotlib.patches import Patch

fig2, (ax_a, ax_b) = plt.subplots(1, 2, figsize=(16, 6))
fig2.suptitle('Top 10 SKU: Price x Quantity Decomposition (LTM22 -> LTM23)',
              color='#26890D', fontweight='bold', fontsize=13)

skus_sorted = sku_df.sort_values('LTM23_Rev_k', ascending=True)
y_pos = np.arange(len(skus_sorted))

# Panel A: Revenue LTM22 vs LTM23
ax_a.barh(y_pos - 0.2, skus_sorted['LTM22_Rev_k'], height=0.4, label='LTM22', color=DC[2])
ax_a.barh(y_pos + 0.2, skus_sorted['LTM23_Rev_k'], height=0.4, label='LTM23', color=DC[0])
ax_a.set_yticks(y_pos)
ax_a.set_yticklabels(skus_sorted['SKU'], fontsize=9)
ax_a.set_xlabel('EUR k')
ax_a.set_title('Revenue LTM22 vs LTM23', color='#26890D', fontweight='bold')
ax_a.legend()
ax_a.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:,.0f}'))

# Panel B: PxQ bridge (volume vs price effect)
vol_vals   = skus_sorted['Vol_Effect_k'].fillna(0).values
price_vals = skus_sorted['Price_Effect_k'].fillna(0).values
col_vol    = [DC[0] if v >= 0 else DC[2] for v in vol_vals]
col_price  = [DC[3] if v >= 0 else DC[4] for v in price_vals]

for i, (vv, pv, cv, cp) in enumerate(zip(vol_vals, price_vals, col_vol, col_price)):
    ax_b.barh(i, vv, height=0.6, color=cv, label='_nolegend_')
    ax_b.barh(i, pv, height=0.6, left=vv, color=cp, label='_nolegend_', alpha=0.85)

ax_b.axvline(x=0, color='grey', linewidth=0.8)
ax_b.set_yticks(y_pos)
ax_b.set_yticklabels(skus_sorted['SKU'], fontsize=9)
ax_b.set_xlabel('EUR k')
ax_b.set_title('PxQ Bridge Components', color='#26890D', fontweight='bold')
legend_els = [
    Patch(color=DC[0], label='Volume Effect (+)'),
    Patch(color=DC[2], label='Volume Effect (-)'),
    Patch(color=DC[3], label='Price Effect (+)'),
    Patch(color=DC[4], label='Price Effect (-)'),
]
ax_b.legend(handles=legend_els, fontsize=8, loc='lower right')
ax_b.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:+,.0f}'))

fig2.tight_layout()
plt.savefig('workspace/graphs/iter12_top10_sku_pxq.png', dpi=150, bbox_inches='tight')
plt.close()
print("GRAPH_SAVED: iter12_top10_sku_pxq.png - Top 10 SKU PxQ decomposition: LTM22 vs LTM23 revenue bars and volume/price effect breakdown")

# ── Summary print ──────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("KEY FINDINGS SUMMARY")
print("=" * 70)
print(f"\n1. TOTAL COMPANY - FY21->FY22 Bridge:")
print(f"   Revenue grew +EUR{b1['delta_rev']/1e6:.1f}m")
print(f"   Volume effect = +EUR{b1['vol_effect']/1e6:.1f}m ({b1['vol_pct']:.1f}% of change)")
print(f"   Price effect  = EUR{b1['price_effect']/1e6:+.2f}m ({b1['price_pct']:.1f}% of change)")
print(f"\n2. TOTAL COMPANY - LTM22->LTM23 Bridge:")
print(f"   Revenue grew +EUR{b2['delta_rev']/1e6:.1f}m")
print(f"   Volume effect = +EUR{b2['vol_effect']/1e6:.1f}m ({b2['vol_pct']:.1f}% of change)")
print(f"   Price effect  = EUR{b2['price_effect']/1e6:+.2f}m ({b2['price_pct']:.1f}% of change)")
print(f"\n3. ASP CAGR (N={N_years:.2f}y): {cagr_asp:.1%} - essentially flat, confirming volume as the growth engine")
print(f"\n4. CHANNEL ASP EVOLUTION (LTM22 -> LTM23):")
for _, row in ch_df.iterrows():
    asp_d = f"{row['ASP_Chg_Pct']:+.1%}" if not np.isnan(row['ASP_Chg_Pct']) else 'n/a'
    print(f"   {row['Channel']}: FY21 EUR{row['FY21_ASP']:.2f} | LTM22 EUR{row['LTM22_ASP']:.2f} | LTM23 EUR{row['LTM23_ASP']:.2f} | LTM22->LTM23: {asp_d}")
print(f"\n5. TOP SKU ASP RANGE (LTM23): EUR{sku_df['LTM23_ASP'].min():.2f} - EUR{sku_df['LTM23_ASP'].max():.2f}")
print(f"   SKUs with significant ASP change LTM22->LTM23 (>5%):")
for _, row in sku_df.iterrows():
    if not np.isnan(row['ASP_Chg_Pct']) and abs(row['ASP_Chg_Pct']) > 0.05:
        print(f"     {row['SKU']}: {row['ASP_Chg_Pct']:+.1%} ASP change (LTM22 EUR{row['LTM22_ASP']:.2f} -> LTM23 EUR{row['LTM23_ASP']:.2f})")
