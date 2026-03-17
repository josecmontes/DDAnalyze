import pandas as pd
import numpy as np
from datetime import datetime

df = pd.read_excel("workspace/data.xlsx")
df.columns = ['Modelo', 'Fecha_Mes', 'Canal', 'Tienda_Cliente', 'Ventas_Netas', 'Cant_Neta']
df['Fecha_Mes'] = pd.to_datetime(df['Fecha_Mes'])
df['YM'] = df['Fecha_Mes'].dt.to_period('M')

latest = df['Fecha_Mes'].max()
fy21 = (df['Fecha_Mes'] >= '2021-01-01') & (df['Fecha_Mes'] <= '2021-12-31')
fy22 = (df['Fecha_Mes'] >= '2022-01-01') & (df['Fecha_Mes'] <= '2022-12-31')
ltm22 = (df['Fecha_Mes'] >= '2021-05-01') & (df['Fecha_Mes'] <= '2022-04-30')
ltm23 = (df['Fecha_Mes'] >= '2022-05-01') & (df['Fecha_Mes'] <= '2023-04-30')

def rev(mask, canal=None):
    d = df[mask]
    if canal:
        d = d[d['Canal'] == canal]
    return d['Ventas_Netas'].sum()

def units(mask, canal=None):
    d = df[mask]
    if canal:
        d = d[d['Canal'] == canal]
    return d['Cant_Neta'].sum()

CAGR_N = 2.25
channels = ['Online', 'Retail', 'Wholesales']

OUTPUT = "workspace/exports/databook_20260317_000807.xlsx"

with pd.ExcelWriter(OUTPUT, engine='openpyxl') as writer:

    # SHEET 1: Revenue Overview
    periods = ['FY21', 'FY22', 'LTM22', 'LTM23']
    masks = [fy21, fy22, ltm22, ltm23]

    rows = []
    for ch in channels + ['Total']:
        row = {'Channel': ch}
        for p, m in zip(periods, masks):
            row[p+'_rev'] = rev(m, ch if ch != 'Total' else None)
        row['YoY_LTM'] = (row['LTM23_rev'] / row['LTM22_rev'] - 1) if row['LTM22_rev'] else None
        row['YoY_FY'] = (row['FY22_rev'] / row['FY21_rev'] - 1) if row['FY21_rev'] else None
        row['CAGR'] = (row['LTM23_rev'] / row['FY21_rev']) ** (1 / CAGR_N) - 1 if row['FY21_rev'] else None
        rows.append(row)
    t1a = pd.DataFrame(rows)
    for p in periods:
        t1a[p+'_rev_k'] = (t1a[p+'_rev'] / 1000).round(1)
    t1a_out = t1a[['Channel'] + [p+'_rev_k' for p in periods] + ['YoY_FY', 'YoY_LTM', 'CAGR']].copy()
    t1a_out.columns = ['Channel', 'FY21 Rev (€k)', 'FY22 Rev (€k)', 'LTM22 Rev (€k)', 'LTM23 Rev (€k)', 'YoY FY22/21', 'YoY LTM23/22', 'CAGR (2.25y)']

    total_revs = {p: rev(m) for p, m in zip(periods, masks)}
    mix_rows = []
    for ch in channels:
        row = {'Channel': ch}
        for p, m in zip(periods, masks):
            row[p] = rev(m, ch) / total_revs[p] if total_revs[p] else None
        mix_rows.append(row)
    t1b = pd.DataFrame(mix_rows)
    t1b.columns = ['Channel', 'FY21 Mix%', 'FY22 Mix%', 'LTM22 Mix%', 'LTM23 Mix%']

    df['Quarter'] = df['Fecha_Mes'].dt.quarter
    df['Year'] = df['Fecha_Mes'].dt.year
    qrows = []
    for ch in channels:
        for q in [1,2,3,4]:
            r21 = df[(df['Year']==2021)&(df['Quarter']==q)&(df['Canal']==ch)]['Ventas_Netas'].sum()
            r22 = df[(df['Year']==2022)&(df['Quarter']==q)&(df['Canal']==ch)]['Ventas_Netas'].sum()
            yoy = (r22/r21-1) if r21 else None
            qrows.append({'Channel': ch, 'Quarter': f'Q{q}', 'FY21 Rev (€k)': round(r21/1000,1), 'FY22 Rev (€k)': round(r22/1000,1), 'YoY%': yoy})
    t1c = pd.DataFrame(qrows)

    price_rows = []
    for p, m in zip(periods, masks):
        r = rev(m); u = units(m)
        price_rows.append({'Period': p, 'Revenue (€k)': round(r/1000,1), 'Net Units': int(u), 'Avg Unit Price (€)': round(r/u,2) if u else None})
    t1d = pd.DataFrame(price_rows)

    row_offset = 1
    t1a_out.to_excel(writer, sheet_name='1. Revenue Overview', index=False, startrow=row_offset)
    row_offset += len(t1a_out) + 3
    t1b.to_excel(writer, sheet_name='1. Revenue Overview', index=False, startrow=row_offset)
    row_offset += len(t1b) + 3
    t1c.to_excel(writer, sheet_name='1. Revenue Overview', index=False, startrow=row_offset)
    row_offset += len(t1c) + 3
    t1d.to_excel(writer, sheet_name='1. Revenue Overview', index=False, startrow=row_offset)

    ws = writer.sheets['1. Revenue Overview']
    ws['A1'] = 'Table 1: Revenue by Channel (€k)'
    ws[f'A{len(t1a_out)+4}'] = 'Table 2: Revenue Mix % by Channel'
    ws[f'A{len(t1a_out)+len(t1b)+7}'] = 'Table 3: Quarterly Revenue by Channel'
    ws[f'A{len(t1a_out)+len(t1b)+len(t1c)+10}'] = 'Table 4: Avg Unit Price & Net Units'

    # SHEET 2: Channel Deep-Dive
    cc_rows = []
    for ch in channels:
        row = {'Channel': ch}
        for p, m in zip(periods, masks):
            row[p] = df[m & (df['Canal']==ch)]['Tienda_Cliente'].nunique()
        row['Growth FY21-LTM23'] = (row['LTM23'] / row['FY21'] - 1) if row['FY21'] else None
        row['Growth LTM22-LTM23'] = (row['LTM23'] / row['LTM22'] - 1) if row['LTM22'] else None
        cc_rows.append(row)
    t2a = pd.DataFrame(cc_rows)
    t2a.columns = ['Channel', 'FY21 Clients', 'FY22 Clients', 'LTM22 Clients', 'LTM23 Clients', 'Growth FY21-LTM23', 'Growth LTM22-LTM23']

    arc_rows = []
    for ch in channels:
        row = {'Channel': ch}
        for p, m in zip(periods, masks):
            r = rev(m, ch); c = df[m & (df['Canal']==ch)]['Tienda_Cliente'].nunique()
            row[p] = round(r/c/1000, 1) if c else None
        row['Change LTM22-LTM23'] = (row['LTM23']/row['LTM22']-1) if row['LTM22'] else None
        arc_rows.append(row)
    t2b = pd.DataFrame(arc_rows)
    t2b.columns = ['Channel', 'FY21 Avg Rev/Client (€k)', 'FY22 Avg Rev/Client (€k)', 'LTM22 Avg Rev/Client (€k)', 'LTM23 Avg Rev/Client (€k)', 'Change LTM22-LTM23']

    online_q = []
    for yr in [2021,2022]:
        for q in [1,2,3,4]:
            r = df[(df['Year']==yr)&(df['Quarter']==q)&(df['Canal']=='Online')]['Ventas_Netas'].sum()
            online_q.append({'Year': yr, 'Quarter': f'Q{q}', 'Revenue (€k)': round(r/1000,1)})
    for q in [1,2,3,4]:
        m_start = {1:'2022-05', 2:'2022-08', 3:'2022-11', 4:'2023-02'}[q]
        m_end = {1:'2022-07', 2:'2022-10', 3:'2023-01', 4:'2023-04'}[q]
        r = df[(df['Fecha_Mes']>=m_start)&(df['Fecha_Mes']<=m_end)&(df['Canal']=='Online')]['Ventas_Netas'].sum()
        online_q.append({'Year': 'LTM23', 'Quarter': f'Q{q}', 'Revenue (€k)': round(r/1000,1)})
    t2c = pd.DataFrame(online_q)

    retail_q = []
    for yr in [2021,2022]:
        for q in [1,2,3,4]:
            mask_q = (df['Year']==yr)&(df['Quarter']==q)&(df['Canal']=='Retail')
            r = df[mask_q]['Ventas_Netas'].sum()
            sc = df[mask_q]['Tienda_Cliente'].nunique()
            retail_q.append({'Year': yr, 'Quarter': f'Q{q}', 'Revenue (€k)': round(r/1000,1), 'Store Count': sc})
    for q in [1,2,3,4]:
        m_start = {1:'2022-05', 2:'2022-08', 3:'2022-11', 4:'2023-02'}[q]
        m_end = {1:'2022-07', 2:'2022-10', 3:'2023-01', 4:'2023-04'}[q]
        mask_q = (df['Fecha_Mes']>=m_start)&(df['Fecha_Mes']<=m_end)&(df['Canal']=='Retail')
        r = df[mask_q]['Ventas_Netas'].sum(); sc = df[mask_q]['Tienda_Cliente'].nunique()
        retail_q.append({'Year': 'LTM23', 'Quarter': f'Q{q}', 'Revenue (€k)': round(r/1000,1), 'Store Count': sc})
    t2d = pd.DataFrame(retail_q)

    ro = 1
    t2a.to_excel(writer, sheet_name='2. Channel Deep-Dive', index=False, startrow=ro)
    ro += len(t2a)+3
    t2b.to_excel(writer, sheet_name='2. Channel Deep-Dive', index=False, startrow=ro)
    ro += len(t2b)+3
    t2c.to_excel(writer, sheet_name='2. Channel Deep-Dive', index=False, startrow=ro)
    ro += len(t2c)+3
    t2d.to_excel(writer, sheet_name='2. Channel Deep-Dive', index=False, startrow=ro)

    ws2 = writer.sheets['2. Channel Deep-Dive']
    ws2['A1'] = 'Table 1: Unique Client/Store Count by Channel'
    ws2[f'A{len(t2a)+4}'] = 'Table 2: Avg Revenue per Client/Store (€k)'
    ws2[f'A{len(t2a)+len(t2b)+7}'] = 'Table 3: Online Revenue by Quarter'
    ws2[f'A{len(t2a)+len(t2b)+len(t2c)+10}'] = 'Table 4: Retail Quarterly Revenue with Store Count'

    # SHEET 3: Wholesale Account Analysis
    ws_df = df[df['Canal']=='Wholesales'].copy()
    # Fix: use masks applied directly to ws_df using its own index
    ltm22_ws = (ws_df['Fecha_Mes'] >= '2021-05-01') & (ws_df['Fecha_Mes'] <= '2022-04-30')
    ltm23_ws = (ws_df['Fecha_Mes'] >= '2022-05-01') & (ws_df['Fecha_Mes'] <= '2023-04-30')

    clients_ltm22 = set(ws_df[ltm22_ws]['Tienda_Cliente'].unique())
    clients_ltm23 = set(ws_df[ltm23_ws]['Tienda_Cliente'].unique())
    retained = clients_ltm22 & clients_ltm23
    new_acc = clients_ltm23 - clients_ltm22
    churned = clients_ltm22 - clients_ltm23

    rev_ltm22_by_client = ws_df[ltm22_ws].groupby('Tienda_Cliente')['Ventas_Netas'].sum()
    rev_ltm23_by_client = ws_df[ltm23_ws].groupby('Tienda_Cliente')['Ventas_Netas'].sum()

    retained_rev_ltm22 = rev_ltm22_by_client[rev_ltm22_by_client.index.isin(retained)].sum()
    retained_rev_ltm23 = rev_ltm23_by_client[rev_ltm23_by_client.index.isin(retained)].sum()
    new_rev_ltm23 = rev_ltm23_by_client[rev_ltm23_by_client.index.isin(new_acc)].sum()
    churned_rev = rev_ltm22_by_client[rev_ltm22_by_client.index.isin(churned)].sum()
    total_ltm22 = rev_ltm22_by_client.sum(); total_ltm23 = rev_ltm23_by_client.sum()

    t3a = pd.DataFrame([
        {'Component': 'LTM22 Total Wholesale Revenue', 'Revenue (€k)': round(total_ltm22/1000,1), 'Accounts': len(clients_ltm22)},
        {'Component': 'Retained Accounts LFL Change', 'Revenue (€k)': round((retained_rev_ltm23-retained_rev_ltm22)/1000,1), 'Accounts': len(retained)},
        {'Component': '  of which: LTM22 retained base', 'Revenue (€k)': round(retained_rev_ltm22/1000,1), 'Accounts': ''},
        {'Component': '  of which: LTM23 retained', 'Revenue (€k)': round(retained_rev_ltm23/1000,1), 'Accounts': ''},
        {'Component': 'New Accounts Revenue', 'Revenue (€k)': round(new_rev_ltm23/1000,1), 'Accounts': len(new_acc)},
        {'Component': 'Churned Accounts Revenue (LTM22)', 'Revenue (€k)': round(-churned_rev/1000,1), 'Accounts': len(churned)},
        {'Component': 'LTM23 Total Wholesale Revenue', 'Revenue (€k)': round(total_ltm23/1000,1), 'Accounts': len(clients_ltm23)},
    ])

    def tier(v):
        if v < 1000: return '<€1k'
        elif v < 5000: return '€1k-5k'
        elif v < 10000: return '€5k-10k'
        elif v < 50000: return '€10k-50k'
        elif v < 100000: return '€50k-100k'
        else: return '>€100k'
    tier_order = ['<€1k','€1k-5k','€5k-10k','€10k-50k','€50k-100k','>€100k']
    retained_tiers = rev_ltm23_by_client[rev_ltm23_by_client.index.isin(retained)].apply(tier)
    new_tiers = rev_ltm23_by_client[rev_ltm23_by_client.index.isin(new_acc)].apply(tier)
    tier_rows = []
    for t in tier_order:
        r_cnt = (retained_tiers==t).sum(); r_rev = rev_ltm23_by_client[retained_tiers[retained_tiers==t].index].sum()
        n_cnt = (new_tiers==t).sum(); n_rev = rev_ltm23_by_client[new_tiers[new_tiers==t].index].sum()
        tier_rows.append({'Tier': t, 'Retained #': r_cnt, 'Retained Rev (€k)': round(r_rev/1000,1), 'New #': n_cnt, 'New Rev (€k)': round(n_rev/1000,1)})
    t3b = pd.DataFrame(tier_rows)

    top10 = rev_ltm23_by_client.nlargest(10).index
    top10_rows = []
    for cl in top10:
        r22 = rev_ltm22_by_client.get(cl, 0); r23 = rev_ltm23_by_client.get(cl, 0)
        yoy = (r23/r22-1) if r22 > 0 else None
        shr = r23/total_ltm23
        traj = 'New' if cl in new_acc else ('Growing' if yoy and yoy>0.1 else ('Stable' if yoy and abs(yoy)<=0.1 else 'Declining'))
        top10_rows.append({'Account': cl, 'LTM22 Rev (€k)': round(r22/1000,1), 'LTM23 Rev (€k)': round(r23/1000,1), 'YoY%': yoy, 'LTM23 Share': shr, 'Trajectory': traj})
    t3c = pd.DataFrame(top10_rows)

    lfl = rev_ltm23_by_client[rev_ltm23_by_client.index.isin(retained)] / rev_ltm22_by_client[rev_ltm22_by_client.index.isin(retained)]
    lfl = lfl.dropna()
    growing = lfl[lfl>1.2]; stable = lfl[(lfl>=0.8)&(lfl<=1.2)]; declining = lfl[lfl<0.8]
    def lfl_row(label, idx):
        revs = rev_ltm23_by_client[idx.index].sum()
        return {'Cohort': label, 'Account Count': len(idx), 'LTM23 Revenue (€k)': round(revs/1000,1),
                'Revenue Share': revs/retained_rev_ltm23 if retained_rev_ltm23 else None, 'Avg Growth': idx.mean()-1}
    t3d = pd.DataFrame([lfl_row('Growing >20%', growing), lfl_row('Stable ±20%', stable), lfl_row('Declining >20%', declining)])

    ro = 1
    t3a.to_excel(writer, sheet_name='3. Wholesale Accounts', index=False, startrow=ro)
    ro += len(t3a)+3
    t3b.to_excel(writer, sheet_name='3. Wholesale Accounts', index=False, startrow=ro)
    ro += len(t3b)+3
    t3c.to_excel(writer, sheet_name='3. Wholesale Accounts', index=False, startrow=ro)
    ro += len(t3c)+3
    t3d.to_excel(writer, sheet_name='3. Wholesale Accounts', index=False, startrow=ro)

    ws3 = writer.sheets['3. Wholesale Accounts']
    ws3['A1'] = 'Table 1: Wholesale Revenue Bridge LTM22 to LTM23'
    ws3[f'A{len(t3a)+4}'] = 'Table 2: Account Count & Revenue by Tier (Retained vs New)'
    ws3[f'A{len(t3a)+len(t3b)+7}'] = 'Table 3: Top 10 Wholesale Accounts - LTM22 vs LTM23'
    ws3[f'A{len(t3a)+len(t3b)+len(t3c)+10}'] = 'Table 4: Retained Account LFL Distribution'

    # SHEET 4: Product Portfolio & Lifecycle
    sku_rev = {}
    for p, m in zip(periods, masks):
        sku_rev[p] = df[m].groupby('Modelo')['Ventas_Netas'].sum()

    all_skus = set(df['Modelo'].unique())
    ltm23_sku = sku_rev['LTM23'].reindex(list(all_skus), fill_value=0)
    ltm22_sku = sku_rev['LTM22'].reindex(list(all_skus), fill_value=0)
    fy21_sku = sku_rev['FY21'].reindex(list(all_skus), fill_value=0)
    fy22_sku = sku_rev['FY22'].reindex(list(all_skus), fill_value=0)

    top20 = ltm23_sku.nlargest(20).index
    top20_rows = []
    total_ltm23_rev = ltm23_sku.sum()
    cum = 0
    for sku in top20:
        r21=fy21_sku[sku]; r22=fy22_sku[sku]; rl22=ltm22_sku[sku]; rl23=ltm23_sku[sku]
        cagr = (rl23/r21)**(1/CAGR_N)-1 if r21>0 else None
        shr = rl23/total_ltm23_rev; cum+=shr
        top20_rows.append({'SKU': sku, 'FY21 (€k)': round(r21/1000,1), 'FY22 (€k)': round(r22/1000,1),
                           'LTM22 (€k)': round(rl22/1000,1), 'LTM23 (€k)': round(rl23/1000,1),
                           'CAGR': cagr, 'LTM23 Share': shr, 'Cum Share': cum})
    t4a = pd.DataFrame(top20_rows)

    def classify_sku(sku):
        r21=fy21_sku[sku]; r22=fy22_sku[sku]; rl22=ltm22_sku[sku]; rl23=ltm23_sku[sku]
        if rl23==0 and rl22==0 and r22==0: return 'Inactive'
        if r21==0 and rl22==0 and rl23>0: return 'New'
        if r21==0 and rl22>0 and rl23>0:
            return 'Growing / Stable' if rl23>=rl22*0.8 else 'Fading'
        if r21>0 and rl23>0:
            peak = max(r21,r22,rl22,rl23)
            if rl23<peak*0.4 and peak>r21*1.5: return 'Boom-Bust'
            if rl23<r21*0.5: return 'Declining'
            if rl23>r21*1.1: return 'Growing'
            return 'Stable'
        if r21>0 and rl23==0: return 'Declining'
        return 'Other'

    lifecycle = {sku: classify_sku(sku) for sku in all_skus}
    lc_df = pd.DataFrame({'SKU': list(lifecycle.keys()), 'Class': list(lifecycle.values())})
    lc_df['LTM23_rev'] = lc_df['SKU'].map(ltm23_sku)
    lc_summary = lc_df.groupby('Class').agg(SKU_Count=('SKU','count'), LTM23_Rev=('LTM23_rev','sum')).reset_index()
    lc_summary['LTM23 Rev (€k)'] = (lc_summary['LTM23_Rev']/1000).round(1)
    lc_summary['% of LTM23'] = lc_summary['LTM23_Rev']/total_ltm23_rev
    t4b = lc_summary[['Class','SKU_Count','LTM23 Rev (€k)','% of LTM23']].rename(columns={'Class':'Lifecycle Class','SKU_Count':'SKU Count'})

    fy21_legacy = fy21_sku[fy21_sku>0].index
    fy22_launch = fy22_sku[(fy22_sku>0)&(fy21_sku==0)].index
    ltm23_launch = ltm23_sku[(ltm23_sku>0)&(fy22_sku==0)&(fy21_sku==0)].index
    vintage_rows = []
    for label, idx in [('FY21 Legacy', fy21_legacy),('FY22 Launch', fy22_launch),('LTM23 Launch', ltm23_launch)]:
        row = {'Vintage': label, 'SKU Count': len(idx)}
        for p, s in zip(periods, [fy21_sku,fy22_sku,ltm22_sku,ltm23_sku]):
            row[f'{p} Rev (€k)'] = round(s[s.index.isin(idx)].sum()/1000,1)
        vintage_rows.append(row)
    t4c = pd.DataFrame(vintage_rows)

    first_sale = df.groupby('Modelo')['Fecha_Mes'].min()
    def half(dt):
        return f"{dt.year}H{'1' if dt.month<=6 else '2'}"
    new_skus_list = [s for s in all_skus if fy21_sku[s]==0]
    hs_rows = []
    for sku in new_skus_list:
        if sku in first_sale.index:
            hs_rows.append({'SKU': sku, 'launch_half': half(first_sale[sku]), 'LTM23_rev': ltm23_sku[sku]})
    hs_df = pd.DataFrame(hs_rows)
    t4d = hs_df.groupby('launch_half').agg(SKU_Count=('SKU','count'), LTM23_Rev=('LTM23_rev','sum')).reset_index()
    t4d['LTM23 Rev (€k)'] = (t4d['LTM23_Rev']/1000).round(1)
    t4d = t4d[['launch_half','SKU_Count','LTM23 Rev (€k)']].rename(columns={'launch_half':'Launch Half','SKU_Count':'SKU Count'})

    bb_skus = [s for s in all_skus if classify_sku(s)=='Boom-Bust']
    bb_rows = []
    for sku in bb_skus:
        revs = {'FY21':fy21_sku[sku],'FY22':fy22_sku[sku],'LTM22':ltm22_sku[sku],'LTM23':ltm23_sku[sku]}
        peak_p = max(revs, key=revs.get); peak_v = revs[peak_p]
        bb_rows.append({'SKU':sku,'Peak Period':peak_p,'Peak Rev (€k)':round(peak_v/1000,1),'LTM23 Rev (€k)':round(ltm23_sku[sku]/1000,1),'Decline from Peak':(ltm23_sku[sku]/peak_v-1) if peak_v else None})
    t4e = pd.DataFrame(bb_rows).sort_values('Peak Rev (€k)',ascending=False).head(25)

    ro = 1
    for tbl, label in [(t4a,'Table 1: Top 20 SKUs by LTM23 Revenue'),(t4b,'Table 2: SKU Lifecycle Classification'),(t4c,'Table 3: Revenue by SKU Vintage'),(t4d,'Table 4: New SKU Launch by Half-Year'),(t4e,'Table 5: Boom-Bust SKU Detail (Top 25)')]:
        ws4_ = writer.sheets.get('4. Product Portfolio')
        if ws4_ is None:
            tbl.to_excel(writer, sheet_name='4. Product Portfolio', index=False, startrow=ro)
            ws4_ = writer.sheets['4. Product Portfolio']
            ws4_['A1'] = label
            ro += len(tbl)+3
        else:
            tbl.to_excel(writer, sheet_name='4. Product Portfolio', index=False, startrow=ro)
            ws4_[f'A{ro}'] = label
            ro += len(tbl)+3

    # SHEET 5: New SKU Durability & Risk
    newly_active = [s for s in all_skus if ltm22_sku[s]==0 and ltm23_sku[s]>0 and fy21_sku[s]==0]

    ltm23_months = pd.date_range('2022-05-01','2023-04-01',freq='MS')
    sku_monthly = df[ltm23 & df['Modelo'].isin(newly_active)].groupby(['Modelo','Fecha_Mes'])['Ventas_Netas'].sum().unstack(fill_value=0)

    def classify_newly(sku):
        if sku not in sku_monthly.index: return 'Early Stage'
        row = sku_monthly.loc[sku]
        recent3 = row[row.index >= pd.Timestamp('2023-02-01')].sum()
        total = row.sum()
        if total==0: return 'Early Stage'
        recent_share = recent3/total
        if recent_share >= 0.25: return 'Sustaining'
        elif recent_share >= 0.05: return 'Fading'
        else: return 'Early Stage'

    na_class = {s: classify_newly(s) for s in newly_active}
    na_df = pd.DataFrame({'SKU': list(na_class.keys()), 'Sustainability': list(na_class.values())})
    na_df['LTM23_rev'] = na_df['SKU'].map(ltm23_sku)
    na_df['Recent3M'] = na_df['SKU'].apply(lambda s: sku_monthly.loc[s][sku_monthly.columns >= pd.Timestamp('2023-02-01')].sum() if s in sku_monthly.index else 0)
    t5a = na_df.groupby('Sustainability').agg(SKU_Count=('SKU','count'), LTM23_Rev=('LTM23_rev','sum'), Recent3M=('Recent3M','sum')).reset_index()
    t5a['LTM23 Rev (€k)'] = (t5a['LTM23_Rev']/1000).round(1)
    t5a['Recent 3M (€k)'] = (t5a['Recent3M']/1000).round(1)
    na_total_rev = na_df['LTM23_rev'].sum()
    t5a['% LTM23'] = t5a['LTM23_Rev']/na_total_rev if na_total_rev else 0
    t5a = t5a[['Sustainability','SKU_Count','LTM23 Rev (€k)','Recent 3M (€k)','% LTM23']].rename(columns={'SKU_Count':'SKU Count'})

    na_ch = df[ltm23 & df['Modelo'].isin(newly_active)].copy()
    na_ch['Sustainability'] = na_ch['Modelo'].map(na_class)
    t5b = na_ch.groupby(['Canal','Sustainability'])['Ventas_Netas'].sum().reset_index()
    t5b['Rev (€k)'] = (t5b['Ventas_Netas']/1000).round(1)
    t5b = t5b.pivot(index='Canal',columns='Sustainability',values='Rev (€k)').reset_index().fillna(0)
    t5b.columns.name = None

    fading_skus = na_df[na_df['Sustainability']=='Fading'].sort_values('LTM23_rev',ascending=False)
    fad_rows = []
    for _, row in fading_skus.head(15).iterrows():
        sku = row['SKU']
        if sku in sku_monthly.index:
            sm = sku_monthly.loc[sku]
            months_active = (sm>0).sum()
            peak_rev = sm.max()
            recent = sm[sm.index>=pd.Timestamp('2023-02-01')].sum()
            decay = (recent/(peak_rev*3)-1) if peak_rev>0 else None
        else:
            months_active=0; peak_rev=0; recent=0; decay=None
        fad_rows.append({'SKU':sku,'LTM23 Rev (€k)':round(row['LTM23_rev']/1000,1),'Recent 3M (€k)':round(recent/1000,1),'Months Active':months_active,'Post-Peak Decay':decay})
    t5c = pd.DataFrame(fad_rows) if fad_rows else pd.DataFrame(columns=['SKU','LTM23 Rev (€k)','Recent 3M (€k)','Months Active','Post-Peak Decay'])

    sust_skus = na_df[na_df['Sustainability']=='Sustaining'].sort_values('LTM23_rev',ascending=False)
    sust_rows = []
    for _, row in sust_skus.head(15).iterrows():
        sku = row['SKU']
        recent = row['Recent3M']
        ann = recent*(12/3)
        if sku in sku_monthly.index:
            months_active = (sku_monthly.loc[sku]>0).sum()
        else:
            months_active=0
        sust_rows.append({'SKU':sku,'LTM23 Rev (€k)':round(row['LTM23_rev']/1000,1),'Recent 3M (€k)':round(recent/1000,1),'Annualised Run-Rate (€k)':round(ann/1000,1),'Months Active':months_active})
    t5d = pd.DataFrame(sust_rows) if sust_rows else pd.DataFrame(columns=['SKU','LTM23 Rev (€k)','Recent 3M (€k)','Annualised Run-Rate (€k)','Months Active'])

    sust_total = na_df[na_df['Sustainability']=='Sustaining']['Recent3M'].sum()*(12/3)
    fading_total = na_df[na_df['Sustainability']=='Fading']['Recent3M'].sum()*(12/3)
    legacy_skus_rev = sum(ltm23_sku[s] for s in all_skus if s not in newly_active and ltm23_sku[s]>0)
    legacy_growth_base = legacy_skus_rev*1.05
    base_ltm24 = sust_total + fading_total + legacy_growth_base
    bear_ltm24 = sust_total*0.95 + fading_total*0.5 + legacy_skus_rev
    t5e = pd.DataFrame([
        {'Scenario': 'LTM23 Actual', 'Sustaining SKUs (€k)': round(na_df[na_df['Sustainability']=='Sustaining']['LTM23_rev'].sum()/1000,1), 'Fading SKUs (€k)': round(na_df[na_df['Sustainability']=='Fading']['LTM23_rev'].sum()/1000,1), 'Legacy SKUs (€k)': round(legacy_skus_rev/1000,1), 'Total (€k)': round(total_ltm23_rev/1000,1)},
        {'Scenario': 'LTM24 Base', 'Sustaining SKUs (€k)': round(sust_total/1000,1), 'Fading SKUs (€k)': round(fading_total/1000,1), 'Legacy SKUs (€k)': round(legacy_growth_base/1000,1), 'Total (€k)': round(base_ltm24/1000,1)},
        {'Scenario': 'LTM24 Bear', 'Sustaining SKUs (€k)': round(sust_total*0.95/1000,1), 'Fading SKUs (€k)': round(fading_total*0.5/1000,1), 'Legacy SKUs (€k)': round(legacy_skus_rev/1000,1), 'Total (€k)': round(bear_ltm24/1000,1)},
    ])

    ro = 1
    for tbl, label in [(t5a,'Table 1: Newly Active SKU Sustainability Classification'),(t5b,'Table 2: Newly Active SKU Revenue by Channel x Sustainability'),(t5c,'Table 3: Top 15 Fading SKUs'),(t5d,'Table 4: Top 15 Sustaining SKUs'),(t5e,'Table 5: Revenue Bridge LTM23 to LTM24')]:
        ws5_ = writer.sheets.get('5. New SKU Durability')
        if ws5_ is None:
            tbl.to_excel(writer, sheet_name='5. New SKU Durability', index=False, startrow=ro)
            ws5_ = writer.sheets['5. New SKU Durability']
            ws5_['A1'] = label
            ro += len(tbl)+3
        else:
            tbl.to_excel(writer, sheet_name='5. New SKU Durability', index=False, startrow=ro)
            ws5_[f'A{ro}'] = label
            ro += len(tbl)+3

    # SHEET 6: Seasonality & Ordering Patterns
    fy_seas = df[fy21|fy22].copy()
    fy_seas['Month'] = fy_seas['Fecha_Mes'].dt.month
    monthly_avg = fy_seas.groupby(['Year','Month'])['Ventas_Netas'].sum().reset_index()
    monthly_avg2 = monthly_avg.groupby('Month')['Ventas_Netas'].mean().reset_index()
    overall_avg = monthly_avg2['Ventas_Netas'].mean()
    monthly_avg2['Index'] = (monthly_avg2['Ventas_Netas']/overall_avg*100).round(1)
    monthly_avg2['Flag'] = monthly_avg2['Index'].apply(lambda x: 'Above avg' if x>100 else 'Below avg')
    month_names = {1:'Jan',2:'Feb',3:'Mar',4:'Apr',5:'May',6:'Jun',7:'Jul',8:'Aug',9:'Sep',10:'Oct',11:'Nov',12:'Dec'}
    monthly_avg2['Month Name'] = monthly_avg2['Month'].map(month_names)
    t6a = monthly_avg2[['Month Name','Index','Flag']].copy()

    qch_rows = []
    for ch in channels:
        for q in [1,2,3,4]:
            r21 = df[(df['Year']==2021)&(df['Quarter']==q)&(df['Canal']==ch)]['Ventas_Netas'].sum()
            r22 = df[(df['Year']==2022)&(df['Quarter']==q)&(df['Canal']==ch)]['Ventas_Netas'].sum()
            yoy = (r22/r21-1) if r21 else None
            qch_rows.append({'Channel':ch,'Quarter':f'Q{q}','FY21 (€k)':round(r21/1000,1),'FY22 (€k)':round(r22/1000,1),'YoY%':yoy})
    t6b = pd.DataFrame(qch_rows)

    seas_ch_rows = []
    for ch in channels:
        ch_df = fy_seas[fy_seas['Canal']==ch]
        ch_monthly = ch_df.groupby(['Year','Month'])['Ventas_Netas'].sum().reset_index()
        ch_avg = ch_monthly.groupby('Month')['Ventas_Netas'].mean()
        ch_mean = ch_avg.mean()
        for mo in range(1,13):
            v = ch_avg.get(mo,0)
            idx = round(v/ch_mean*100,1) if ch_mean else 0
            seas_ch_rows.append({'Channel':ch,'Month':month_names[mo],'Index':idx})
    t6c = pd.DataFrame(seas_ch_rows).pivot(index='Month',columns='Channel',values='Index').reset_index()
    t6c.columns.name = None

    q1_22 = df[(df['Year']==2022)&(df['Quarter']==1)&(df['Canal']=='Wholesales')].groupby('Tienda_Cliente')['Ventas_Netas'].sum()
    q1_23 = df[(df['Year']==2023)&(df['Quarter']==1)&(df['Canal']=='Wholesales')].groupby('Tienda_Cliente')['Ventas_Netas'].sum()
    top10_ws = rev_ltm23_by_client.nlargest(10).index
    t6d_rows = []
    for cl in top10_ws:
        r22q=q1_22.get(cl,0); r23q=q1_23.get(cl,0)
        yoy=(r23q/r22q-1) if r22q!=0 else None
        trend = 'Growing' if yoy and yoy>0.05 else ('Declining' if yoy and yoy<-0.05 else 'Stable')
        t6d_rows.append({'Account':cl,'Q1-22 (€k)':round(r22q/1000,1),'Q1-23 (€k)':round(r23q/1000,1),'YoY%':yoy,'Trend':trend})
    t6d = pd.DataFrame(t6d_rows)

    jan_apr_rows = []
    for mo in [1,2,3,4]:
        r22 = df[(df['Year']==2022)&(df['Fecha_Mes'].dt.month==mo)&(df['Canal']=='Wholesales')]['Ventas_Netas'].sum()
        r23 = df[(df['Year']==2023)&(df['Fecha_Mes'].dt.month==mo)&(df['Canal']=='Wholesales')]['Ventas_Netas'].sum()
        yoy=(r23/r22-1) if r22 else None
        jan_apr_rows.append({'Month':month_names[mo],'Jan-Apr 2022 (€k)':round(r22/1000,1),'Jan-Apr 2023 (€k)':round(r23/1000,1),'YoY%':yoy})
    t6e = pd.DataFrame(jan_apr_rows)

    ro = 1
    for tbl, label in [(t6a,'Table 1: Monthly Seasonality Index (Total, FY21-FY22 avg)'),(t6b,'Table 2: Quarterly Revenue by Channel FY21 vs FY22'),(t6c,'Table 3: Monthly Seasonality Index by Channel'),(t6d,'Table 4: Top 10 Wholesale Accounts Q1-22 vs Q1-23'),(t6e,'Table 5: Wholesale Jan-Apr 2022 vs 2023')]:
        ws6_ = writer.sheets.get('6. Seasonality')
        if ws6_ is None:
            tbl.to_excel(writer, sheet_name='6. Seasonality', index=False, startrow=ro)
            ws6_ = writer.sheets['6. Seasonality']
            ws6_['A1'] = label
            ro += len(tbl)+3
        else:
            tbl.to_excel(writer, sheet_name='6. Seasonality', index=False, startrow=ro)
            ws6_[f'A{ro}'] = label
            ro += len(tbl)+3

    # SHEET 7: Risk Summary
    sust_ann = na_df[na_df['Sustainability']=='Sustaining']['Recent3M'].sum()*(12/3)
    fading_ann = na_df[na_df['Sustainability']=='Fading']['Recent3M'].sum()*(12/3)
    fading_ltm23_rev = na_df[na_df['Sustainability']=='Fading']['LTM23_rev'].sum()
    top5_ws_rev = rev_ltm23_by_client.nlargest(5).sum()
    top10_ws_rev = rev_ltm23_by_client.nlargest(10).sum()
    top28_ws_rev = rev_ltm23_by_client.nlargest(28).sum()
    neg_rows = (df['Ventas_Netas']<0).sum(); neg_rev = df[df['Ventas_Netas']<0]['Ventas_Netas'].sum()
    retail_fy21_stores = df[fy21&(df['Canal']=='Retail')]['Tienda_Cliente'].nunique()
    retail_ltm23_stores = df[ltm23&(df['Canal']=='Retail')]['Tienda_Cliente'].nunique()
    retail_fy21_rev = rev(fy21, 'Retail')
    retail_ltm23_rev_val = rev(ltm23, 'Retail')
    retail_fy21_avg_v = retail_fy21_rev/retail_fy21_stores if retail_fy21_stores else 0
    retail_ltm23_avg_v = retail_ltm23_rev_val/retail_ltm23_stores if retail_ltm23_stores else 0

    t7a = pd.DataFrame([
        {'Risk Theme':'Fading SKU Revenue','Description':'Newly Active SKUs with rapid post-launch decay','Revenue Impact (€k)':round((fading_ltm23_rev-fading_ann)/1000,1),'Supporting Evidence':f'LTM23 vs annualised run-rate','Rating':'High'},
        {'Risk Theme':'New SKU Concentration','Description':'High share of LTM23 revenue from SKUs with no FY21 history','Revenue Impact (€k)':round(total_ltm23_rev*0.54/1000,1),'Supporting Evidence':'Many new SKUs, durability unproven','Rating':'High'},
        {'Risk Theme':'Wholesale Account Concentration','Description':'Top 28 accounts represent large share of wholesale','Revenue Impact (€k)':round(top28_ws_rev/1000,1),'Supporting Evidence':f'Top 10: {top10_ws_rev/total_ltm23_rev*100:.1f}% of company revenue','Rating':'High'},
        {'Risk Theme':'Retail Store Dilution','Description':'Avg revenue/store declining as new stores added','Revenue Impact (€k)':round((retail_fy21_avg_v-retail_ltm23_avg_v)/1000,1),'Supporting Evidence':f'€{retail_fy21_avg_v/1000:.0f}k FY21 vs €{retail_ltm23_avg_v/1000:.0f}k LTM23 per store','Rating':'Medium'},
        {'Risk Theme':'Stable SKU Base Very Small','Description':'Few stable evergreen SKUs as share of LTM23','Revenue Impact (€k)':round(total_ltm23_rev*0.035/1000,1),'Supporting Evidence':'Revenue quality depends on continuous new launches','Rating':'Medium'},
        {'Risk Theme':'Returns Exposure','Description':'Negative revenue rows indicating returns/credits','Revenue Impact (€k)':round(neg_rev/1000,1),'Supporting Evidence':f'{neg_rows} rows ({neg_rows/len(df)*100:.1f}% of records)','Rating':'Low'},
        {'Risk Theme':'Wholesale Churn','Description':'Accounts churned LTM22 to LTM23','Revenue Impact (€k)':round(churned_rev/1000,1),'Supporting Evidence':f'Churned revenue partially offset by new accounts','Rating':'Medium'},
    ])

    stable_rev = lc_df[lc_df['Class']=='Stable']['LTM23_rev'].sum()
    growing_rev = lc_df[lc_df['Class']=='Growing']['LTM23_rev'].sum()
    new_sust_rev = na_df[na_df['Sustainability']=='Sustaining']['LTM23_rev'].sum()
    new_fad_rev = na_df[na_df['Sustainability']=='Fading']['LTM23_rev'].sum()
    other_rev = total_ltm23_rev - stable_rev - growing_rev - new_sust_rev - new_fad_rev
    t7b = pd.DataFrame([
        {'SKU Tier':'Stable Evergreen','LTM23 Rev (€k)':round(stable_rev/1000,1),'% of Total':stable_rev/total_ltm23_rev},
        {'SKU Tier':'Growing','LTM23 Rev (€k)':round(growing_rev/1000,1),'% of Total':growing_rev/total_ltm23_rev},
        {'SKU Tier':'New - Sustaining','LTM23 Rev (€k)':round(new_sust_rev/1000,1),'% of Total':new_sust_rev/total_ltm23_rev},
        {'SKU Tier':'New - Fading (at risk)','LTM23 Rev (€k)':round(new_fad_rev/1000,1),'% of Total':new_fad_rev/total_ltm23_rev},
        {'SKU Tier':'Declining / Other','LTM23 Rev (€k)':round(other_rev/1000,1),'% of Total':other_rev/total_ltm23_rev},
        {'SKU Tier':'Total','LTM23 Rev (€k)':round(total_ltm23_rev/1000,1),'% of Total':1.0},
    ])

    t7c = pd.DataFrame([
        {'Group':'Top 5 Accounts','Revenue (€k)':round(top5_ws_rev/1000,1),'% Wholesale':top5_ws_rev/total_ltm23 if total_ltm23 else None,'% Company':top5_ws_rev/total_ltm23_rev},
        {'Group':'Top 10 Accounts','Revenue (€k)':round(top10_ws_rev/1000,1),'% Wholesale':top10_ws_rev/total_ltm23 if total_ltm23 else None,'% Company':top10_ws_rev/total_ltm23_rev},
        {'Group':'Top 28 (>€100k Accounts)','Revenue (€k)':round(top28_ws_rev/1000,1),'% Wholesale':top28_ws_rev/total_ltm23 if total_ltm23 else None,'% Company':top28_ws_rev/total_ltm23_rev},
        {'Group':'All Wholesale','Revenue (€k)':round(total_ltm23/1000,1),'% Wholesale':1.0,'% Company':total_ltm23/total_ltm23_rev},
    ])

    ltm22_total_rev = rev(ltm22)
    t7d = pd.DataFrame([
        {'Scenario':'LTM23 Actual','Total Rev (€k)':round(total_ltm23_rev/1000,1),'YoY vs LTM22':total_ltm23_rev/ltm22_total_rev-1 if ltm22_total_rev else None,'Key Assumption':'Actual reported'},
        {'Scenario':'LTM24 Base','Total Rev (€k)':round(base_ltm24/1000,1),'YoY vs LTM23':base_ltm24/total_ltm23_rev-1 if total_ltm23_rev else None,'Key Assumption':'Sustaining SKUs full run-rate, legacy +5%, fading at run-rate'},
        {'Scenario':'LTM24 Bear','Total Rev (€k)':round(bear_ltm24/1000,1),'YoY vs LTM23':bear_ltm24/total_ltm23_rev-1 if total_ltm23_rev else None,'Key Assumption':'Sustaining SKUs -5%, fading -50%, legacy flat'},
    ])

    t7e = pd.DataFrame([
        {'Metric':'Total rows','Value':len(df)},
        {'Metric':'Negative revenue rows','Value':int(neg_rows)},
        {'Metric':'Negative revenue rows %','Value':f'{neg_rows/len(df)*100:.1f}%'},
        {'Metric':'Negative revenue total (€k)','Value':round(neg_rev/1000,1)},
        {'Metric':'Negative unit rows','Value':int((df['Cant_Neta']<0).sum())},
        {'Metric':'Negative unit rows %','Value':f'{(df["Cant_Neta"]<0).sum()/len(df)*100:.1f}%'},
    ])

    ro = 1
    for tbl, label in [(t7a,'Table 1: Risk Register'),(t7b,'Table 2: Revenue Quality Decomposition'),(t7c,'Table 3: Wholesale Account Concentration'),(t7d,'Table 4: Forward Revenue Scenarios LTM23 to LTM24'),(t7e,'Table 5: Returns Exposure Summary')]:
        ws7_ = writer.sheets.get('7. Risk Summary')
        if ws7_ is None:
            tbl.to_excel(writer, sheet_name='7. Risk Summary', index=False, startrow=ro)
            ws7_ = writer.sheets['7. Risk Summary']
            ws7_['A1'] = label
            ro += len(tbl)+3
        else:
            tbl.to_excel(writer, sheet_name='7. Risk Summary', index=False, startrow=ro)
            ws7_[f'A{ro}'] = label
            ro += len(tbl)+3

print(f"Saved: {OUTPUT}")
print("Sheets:")
for s in ['1. Revenue Overview','2. Channel Deep-Dive','3. Wholesale Accounts','4. Product Portfolio','5. New SKU Durability','6. Seasonality','7. Risk Summary']:
    print(f"  - {s}")
