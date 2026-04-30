#!/usr/bin/env python3
"""
Trap Pattern Detector - Final Version
Top-down scanning with liq jump strength
"""

import os
from datetime import datetime
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment
import requests

COINGLASS_XLSX = os.path.join(os.getcwd(), "Trading_Journal.xlsx")
OUTPUT_XLSX = os.path.join(os.getcwd(), "swing_candidates.xlsx")
TELEGRAM_BOT_TOKEN = "8379400350:AAGmS7tgqrSq064XDDSdguWkKHKaztOXSj4"
TELEGRAM_CHAT_ID = "7783879593"

PAIRS = ["Btc", "Sui", "Ar", "Sol", "Ena", "Atom", "Xrp", "Wld", "Hbar", "Ada",
         "Ondo", "Uni", "Sei", "Avax", "Etc", "Hype", "Vet", "Arb", "Tao", "Dot",
         "Zec", "Inj", "Near", "Fet", "Dexe", "Tia", "Bch", "Ltc", "Aave", "Link",
         "Render", "Bnb"]

def send_telegram(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}, timeout=10)
        return True
    except:
        return False

def get_data(pair):
    try:
        wb = load_workbook(COINGLASS_XLSX, data_only=True)
        if pair not in wb.sheetnames:
            wb.close()
            return None
        rows = [list(r) for r in wb[pair].iter_rows(min_row=2, values_only=True) if r[0]]
        wb.close()
        return rows if rows else None
    except:
        return None

def parse_pct(val):
    if not val:
        return None
    try:
        return float(str(val).split('/')[0].replace('%', '').replace('+', '').strip())
    except:
        return None

def calc_liq(val):
    if not val or '/' not in str(val):
        return None
    try:
        parts = str(val).split('/')
        parse = lambda s: float(s.replace('$', '').replace(',', '').replace('K', '').replace('M', ''))
        long_v = parse(parts[0])
        short_v = parse(parts[1])
        if 'M' in parts[0]:
            long_v *= 1000
        if 'M' in parts[1]:
            short_v *= 1000
        return round((long_v - short_v) / (long_v + short_v), 4) if (long_v + short_v) > 0 else 0
    except:
        return None

def parse_row(row):
    price = parse_pct(row[8]) if len(row) > 8 else None
    vol_raw = row[7] if len(row) > 7 else None
    vol_dir = None
    if vol_raw and '/' in str(vol_raw):
        try:
            parts = str(vol_raw).split('/')
            fut = float(parts[0].replace('%', '').replace('+', '').strip())
            spot = float(parts[1].replace('%', '').replace('+', '').strip())
            vol_dir = "Low" if (fut < 0 and spot < 0) else "High"
        except:
            pass
    liq = calc_liq(row[2]) if len(row) > 2 else None
    return {'price': price, 'vol': vol_dir, 'liq': liq} if (price is not None and vol_dir and liq is not None) else None

def check_c1(parsed, start):
    for length in range(5, 13):
        if start + length > len(parsed):
            break
        subset = parsed[start:start+length]
        buy = all(r['price'] < 0 and r['vol'] == "Low" and r['liq'] > 0 for r in subset)
        if buy:
            return {'direction': 'BUY', 'c1_rows': length, 'end_pos': start+length, 'last_liq': subset[-1]['liq']}
        sell = all(r['price'] > 0 and r['vol'] == "Low" and r['liq'] < 0 for r in subset)
        if sell:
            return {'direction': 'SELL', 'c1_rows': length, 'end_pos': start+length, 'last_liq': subset[-1]['liq']}
    return None

def check_c2(parsed, start, direction):
    for length in range(1, 8):
        if start + length > len(parsed):
            break
        subset = parsed[start:start+length]
        vol_high = sum(1 for r in subset if r['vol'] == "High")
        if vol_high < 2:
            continue
        
        # Price CONTINUES same direction, Liq SAME type
        if direction == "BUY":
            # Price still DOWN, Liq still LONG
            match = all(r['price'] < 0 and r['liq'] > 0 for r in subset)
        else:
            # Price still UP, Liq still SHORT
            match = all(r['price'] > 0 and r['liq'] < 0 for r in subset)
        
        if match:
            return {'c2_rows': length, 'vol_spike': f"{vol_high}vol", 'end_pos': start+length}
    return None

def check_c3(parsed, start, direction, last_liq):
    for length in range(1, 8):
        if start + length > len(parsed):
            break
        subset = parsed[start:start+length]
        if direction == "BUY":
            rev = any(r['price'] > 0 for r in subset)
            sw = any(r['liq'] < 0 for r in subset)
            if rev and sw:
                new_liq = next(r['liq'] for r in subset if r['liq'] < 0)
                jump = abs(last_liq - new_liq)
                strength = "Strong" if jump > 0.7 else ("Medium" if jump > 0.4 else "Weak")
                return {'c3_rows': length, 'liq_jump': round(jump, 3), 'liq_strength': strength, 'end_pos': start+length}
        else:
            rev = any(r['price'] < 0 for r in subset)
            sw = any(r['liq'] > 0 for r in subset)
            if rev and sw:
                new_liq = next(r['liq'] for r in subset if r['liq'] > 0)
                jump = abs(last_liq - new_liq)
                strength = "Strong" if jump > 0.7 else ("Medium" if jump > 0.4 else "Weak")
                return {'c3_rows': length, 'liq_jump': round(jump, 3), 'liq_strength': strength, 'end_pos': start+length}
    return None

def analyze(rows):
    if len(rows) < 20:
        return None
    parsed = [parse_row(r) for r in rows]
    parsed = [p for p in parsed if p]
    if len(parsed) < 20:
        return None
    
    # Scan only latest 25 rows (not from beginning!)
    total = len(parsed)
    scan_start = max(0, total - 25)
    
    for start in range(scan_start, total-10):
        c1 = check_c1(parsed, start)
        if not c1:
            continue
        c2 = check_c2(parsed, c1['end_pos'], c1['direction'])
        if not c2:
            return {'signal_type': 'Coming', 'direction': c1['direction'], 'start_row': start+1, 'end_row': c1['end_pos'], 'rows_used': c1['end_pos']-start, **c1}
        c3 = check_c3(parsed, c2['end_pos'], c1['direction'], c1['last_liq'])
        if not c3:
            return {'signal_type': 'Smart Money', 'direction': c1['direction'], 'start_row': start+1, 'end_row': c2['end_pos'], 'rows_used': c2['end_pos']-start, **c1, **c2}
        return {'signal_type': 'Emergency', 'direction': c1['direction'], 'start_row': start+1, 'end_row': c3['end_pos'], 'rows_used': c3['end_pos']-start, **c1, **c2, **c3}
    return None

def main():
    print("="*70)
    print("🎯 TRAP PATTERN DETECTOR")
    print("="*70)
    results = []
    for idx, pair in enumerate(PAIRS, 1):
        print(f"[{idx:02d}/{len(PAIRS)}] {pair:<6}", end=' ', flush=True)
        rows = get_data(pair)
        if not rows:
            print("❌")
            continue
        a = analyze(rows)
        if a:
            results.append({'pair': pair, **a})
            print(f"✅ {a['signal_type'][:4]} {a['direction']} ({a['rows_used']}r) {a.get('liq_strength','')}")
        else:
            print("—")
    
    em = [r for r in results if r['signal_type'] == 'Emergency']
    sm = [r for r in results if r['signal_type'] == 'Smart Money']
    co = [r for r in results if r['signal_type'] == 'Coming']
    print(f"\n{'='*70}\nEmergency:{len(em)} | Smart:{len(sm)} | Coming:{len(co)}\n{'='*70}\n")
    
    if results:
        wb = Workbook()
        del wb["Sheet"]
        for data, name in [(em, "EMERGENCY"), (sm, "SMART"), (co, "COMING")]:
            if not data:
                continue
            ws = wb.create_sheet(name)
            headers = ["Pair", "Dir", "Rows", "Range", "Jump", "Strength", "Action"]
            for i, h in enumerate(headers, 1):
                c = ws.cell(1, i, h)
                c.font = Font(bold=True, color="FFFFFF")
                c.fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
            for i, item in enumerate(data, 2):
                ws.cell(i, 1, item['pair'])
                ws.cell(i, 2, item['direction'])
                ws.cell(i, 3, item['rows_used'])
                ws.cell(i, 4, f"{item['start_row']}→{item['end_row']}")
                ws.cell(i, 5, item.get('liq_jump', '-'))
                ws.cell(i, 6, item.get('liq_strength', '-'))
                act = f"Emergency: {item['direction']}" if item['signal_type'] == 'Emergency' else (f"Early: {item['direction']}" if item['signal_type'] == 'Smart Money' else f"Might {item['direction']}")
                ws.cell(i, 7, act)
                if item['signal_type'] == 'Emergency':
                    for col in range(1, 8):
                        ws.cell(i, col).fill = PatternFill(start_color="FF0000" if item['direction']=='SELL' else "00FF00", end_color="FF0000" if item['direction']=='SELL' else "00FF00", fill_type="solid")
                        ws.cell(i, col).font = Font(bold=True, color="FFFFFF")
        wb.save(OUTPUT_XLSX)
        print(f"✅ Excel: {OUTPUT_XLSX}\n")
        
        msg = f"🎯 <b>Trap Detector</b>\n⏰ {datetime.now().strftime('%H:%M')}\n\n"
        if em:
            msg += f"🚨 <b>EMERGENCY ({len(em)}):</b>\n"
            for r in em:
                msg += f"  {'🔴' if r['direction']=='SELL' else '🟢'} {r['pair']} {r['direction']} ({r.get('liq_strength','')}) {r['rows_used']}r\n"
        if sm:
            msg += f"\n💎 <b>SMART ({len(sm)}):</b>\n"
            for r in sm:
                msg += f"  {'📉' if r['direction']=='SELL' else '📈'} {r['pair']} {r['direction']} {r['rows_used']}r\n"
        if co:
            msg += f"\n⚠️ <b>COMING ({len(co)}):</b>\n"
            for r in co:
                msg += f"  • {r['pair']} {r['direction']} {r['rows_used']}r\n"
        send_telegram(msg)
        print("✅ Telegram sent!")

if __name__ == "__main__":
    main()