#!/usr/bin/env python3
"""
얼리버드 대시보드 자동 갱신 스크립트
매일 09:00 KST에 BigQuery에서 최신 데이터를 가져와 HTML 대시보드를 갱신합니다.

사전 설정:
  1. gcloud SDK 설치: https://cloud.google.com/sdk/docs/install
  2. 인증: gcloud auth application-default login
  3. 프로젝트 설정: gcloud config set project socar-data
"""

import json
import os
import sys
from datetime import datetime, timedelta

try:
    from google.cloud import bigquery
except ImportError:
    print("google-cloud-bigquery 패키지가 필요합니다.")
    print("설치: pip3 install google-cloud-bigquery")
    sys.exit(1)

OUTPUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "earlybird_dashboard.html")
PROJECT_ID = "socar-data"

# ── 쿼리 ──
Q_COUPON = """
SELECT
  EXTRACT(isoyear FROM DATE(return_at_kst)) AS year,
  EXTRACT(month FROM DATE(return_at_kst)) AS month,
  CASE
    WHEN coupon_policy_name LIKE '%미리예약%' THEN '미리예약'
    WHEN coupon_policy_name LIKE '%당일최저가%' THEN '당일최저가'
  END AS coupon_type,
  COUNT(DISTINCT reservation_id) AS cnt,
  SUM(utime_charged/60) AS utime_hrs,
  SUM(revenue) AS revenue,
  SUM(contribution_margin) AS cm,
  SUM(profit) AS profit,
  SUM(__rev_rent) AS rev_origin,
  SUM(__rev_rent_discount) AS rev_discount
FROM `socar-data.socar_biz_profit.profit_socar_reservation`
WHERE DATE(return_at_kst) >= DATE '2026-01-01'
  AND DATE(return_at_kst) <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  AND coupon_policy_id IS NOT NULL
  AND (coupon_policy_name LIKE '%미리예약%' OR coupon_policy_name LIKE '%당일최저가%')
GROUP BY 1,2,3
ORDER BY 1,2,3
"""

Q_TOTAL = """
SELECT
  EXTRACT(isoyear FROM DATE(return_at_kst)) AS year,
  EXTRACT(month FROM DATE(return_at_kst)) AS month,
  COUNT(DISTINCT reservation_id) AS total_cnt,
  SUM(revenue) AS total_revenue,
  SUM(contribution_margin) AS total_cm,
  SUM(profit) AS total_profit
FROM `socar-data.socar_biz_profit.profit_socar_reservation`
WHERE DATE(return_at_kst) >= DATE '2026-01-01'
  AND DATE(return_at_kst) <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY 1,2
ORDER BY 1,2
"""

Q_WEEKLY = """
SELECT
  EXTRACT(isoyear FROM DATE(return_at_kst)) AS year,
  EXTRACT(isoweek FROM DATE(return_at_kst)) AS week,
  MIN(DATE(return_at_kst)) AS week_start,
  CASE
    WHEN coupon_policy_name LIKE '%미리예약%' THEN '미리예약'
    WHEN coupon_policy_name LIKE '%당일최저가%' THEN '당일최저가'
  END AS coupon_type,
  COUNT(DISTINCT reservation_id) AS cnt,
  SUM(revenue) AS revenue,
  SUM(contribution_margin) AS cm,
  SUM(profit) AS profit
FROM `socar-data.socar_biz_profit.profit_socar_reservation`
WHERE DATE(return_at_kst) >= DATE '2026-01-01'
  AND DATE(return_at_kst) <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  AND coupon_policy_id IS NOT NULL
  AND (coupon_policy_name LIKE '%미리예약%' OR coupon_policy_name LIKE '%당일최저가%')
GROUP BY 1,2,4
ORDER BY 1,2,4
"""


def run_query(client, sql):
    """BigQuery 쿼리 실행 후 dict 리스트 반환"""
    job = client.query(sql)
    rows = job.result()
    return [dict(row) for row in rows]


def calc_metrics(coupon_rows, total_rows, weekly_rows):
    """원시 데이터로부터 대시보드에 필요한 메트릭 계산"""

    # 월별 쿠폰 데이터
    eb_monthly, sd_monthly = {}, {}
    for r in coupon_rows:
        m = r["month"]
        d = {
            "cnt": r["cnt"], "revenue": r["revenue"], "cm": r["cm"],
            "profit": r["profit"], "rev_origin": r["rev_origin"],
            "rev_discount": r["rev_discount"], "utime_hrs": r["utime_hrs"],
        }
        if r["coupon_type"] == "미리예약":
            eb_monthly[m] = d
        else:
            sd_monthly[m] = d

    # 전체 데이터
    tot_monthly = {}
    for r in total_rows:
        tot_monthly[r["month"]] = {
            "cnt": r["total_cnt"], "revenue": r["total_revenue"],
            "cm": r["total_cm"], "profit": r["total_profit"],
        }

    # 누적
    eb_total = {k: sum(v[k] for v in eb_monthly.values()) for k in ["cnt","revenue","cm","profit","rev_origin","rev_discount","utime_hrs"]}
    sd_total = {k: sum(v[k] for v in sd_monthly.values()) for k in ["cnt","revenue","cm","profit","rev_origin","rev_discount","utime_hrs"]}
    all_total = {k: sum(v[k] for v in tot_monthly.values()) for k in ["cnt","revenue","cm","profit"]}

    # GPM
    eb_gpm = eb_total["cm"] / eb_total["revenue"] * 100 if eb_total["revenue"] else 0
    sd_gpm = sd_total["cm"] / sd_total["revenue"] * 100 if sd_total["revenue"] else 0
    all_gpm = all_total["cm"] / all_total["revenue"] * 100 if all_total["revenue"] else 0

    # 쿠폰 제외 전체 GPM
    rest_rev = all_total["revenue"] - eb_total["revenue"] - sd_total["revenue"]
    rest_cm = all_total["cm"] - eb_total["cm"] - sd_total["cm"]
    rest_gpm = rest_cm / rest_rev * 100 if rest_rev else 0

    # 주간 데이터
    eb_weeks, sd_weeks = {}, {}
    for r in weekly_rows:
        key = r["week"]
        d = {"cnt": r["cnt"], "revenue": r["revenue"], "cm": r["cm"], "profit": r["profit"],
             "week_start": str(r["week_start"])}
        if r["coupon_type"] == "미리예약":
            eb_weeks[key] = d
        else:
            sd_weeks[key] = d

    all_weeks = sorted(set(list(eb_weeks.keys()) + list(sd_weeks.keys())))

    return {
        "eb_monthly": eb_monthly, "sd_monthly": sd_monthly, "tot_monthly": tot_monthly,
        "eb_total": eb_total, "sd_total": sd_total, "all_total": all_total,
        "eb_gpm": eb_gpm, "sd_gpm": sd_gpm, "all_gpm": all_gpm, "rest_gpm": rest_gpm,
        "eb_weeks": eb_weeks, "sd_weeks": sd_weeks, "all_weeks": all_weeks,
    }


def fmt(v, unit=""):
    """숫자 포맷"""
    if unit == "억":
        return f"{v / 1e8:.2f}"
    if unit == "%":
        return f"{v:.1f}"
    if unit == "원":
        return f"{v:,.0f}"
    return f"{v:,.0f}"


def generate_html(m):
    """메트릭 dict를 받아 HTML 대시보드 생성"""
    now = datetime.now().strftime("%Y.%m.%d %H:%M")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y.%m.%d")
    months = sorted(m["eb_monthly"].keys())

    # 주간 차트 데이터
    weeks_labels = []
    eb_cnt, eb_rev, eb_cm, eb_pft = [], [], [], []
    sd_cnt, sd_rev, sd_cm, sd_pft = [], [], [], []
    for w in m["all_weeks"]:
        e = m["eb_weeks"].get(w, {"cnt":0,"revenue":0,"cm":0,"profit":0,"week_start":""})
        s = m["sd_weeks"].get(w, {"cnt":0,"revenue":0,"cm":0,"profit":0,"week_start":""})
        ws = e.get("week_start") or s.get("week_start","")
        label = f"W{w}\\n{ws[5:10]}" if ws else f"W{w}"
        weeks_labels.append(label)
        eb_cnt.append(e["cnt"]); eb_rev.append(round(e["revenue"]/1e8,2)); eb_cm.append(round(e["cm"]/1e8,2)); eb_pft.append(round(e["profit"]/1e8,2))
        sd_cnt.append(s["cnt"]); sd_rev.append(round(s["revenue"]/1e8,2)); sd_cm.append(round(s["cm"]/1e8,2)); sd_pft.append(round(s["profit"]/1e8,2))

    # 월별 테이블 행
    def monthly_row(label, key, unit="", color_fn=None):
        cells = []
        for mo in months:
            e = m["eb_monthly"].get(mo, {})
            s = m["sd_monthly"].get(mo, {})
            ev = e.get(key, 0)
            sv = s.get(key, 0)
            if callable(color_fn):
                ec, sc = color_fn(ev), color_fn(sv)
            else:
                ec, sc = "", ""
            if unit == "억":
                cells.append(f'<td class="r {ec}">{ev/1e8:.2f}</td><td class="r {sc}">{sv/1e8:.2f}</td>')
            elif unit == "%":
                cells.append(f'<td class="r {ec}">{ev:.1f}%</td><td class="r {sc}">{sv:.1f}%</td>')
            elif unit == "원":
                cells.append(f'<td class="r {ec}">{ev:,.0f}</td><td class="r {sc}">{sv:,.0f}</td>')
            else:
                cells.append(f'<td class="r {ec}">{ev:,.0f}</td><td class="r {sc}">{sv:,.0f}</td>')
        return f'<tr><td class="row-label">{label}</td>{"".join(cells)}</tr>'

    # GPM / 순이익률 계산 행
    gpm_rows = ""
    profit_rows = ""
    for mo in months:
        e = m["eb_monthly"].get(mo, {})
        s = m["sd_monthly"].get(mo, {})
        egpm = e["cm"]/e["revenue"]*100 if e.get("revenue") else 0
        sgpm = s["cm"]/s["revenue"]*100 if s.get("revenue") else 0
        epr = e["profit"]/e["revenue"]*100 if e.get("revenue") else 0
        spr = s["profit"]/s["revenue"]*100 if s.get("revenue") else 0
        gpm_rows += f'<td class="r blue" style="font-weight:700;">{egpm:.1f}%</td><td class="r">{sgpm:.1f}%</td>'
        profit_rows += f'<td class="r {"green" if epr>=0 else "red"}">{epr:+.1f}%</td><td class="r {"green" if spr>=0 else "red"}">{spr:+.1f}%</td>'

    # 기여도 행
    contrib_rows = ""
    for mo in months:
        e = m["eb_monthly"].get(mo, {})
        s = m["sd_monthly"].get(mo, {})
        t = m["tot_monthly"].get(mo, {})
        ec = e.get("revenue",0)/t["revenue"]*100 if t.get("revenue") else 0
        sc = s.get("revenue",0)/t["revenue"]*100 if t.get("revenue") else 0
        contrib_rows += f'<td class="r {"blue" if ec>5 else ""}">{ec:.1f}%</td><td class="r">{sc:.1f}%</td>'

    # 할인율 행
    disc_rows = ""
    for mo in months:
        e = m["eb_monthly"].get(mo, {})
        s = m["sd_monthly"].get(mo, {})
        ed = abs(e.get("rev_discount",0))/e["rev_origin"]*100 if e.get("rev_origin") else 0
        sd_disc = abs(s.get("rev_discount",0))/s["rev_origin"]*100 if s.get("rev_origin") else 0
        disc_rows += f'<td class="r">{ed:.1f}%</td><td class="r red">{sd_disc:.1f}%</td>'

    # 건당 매출/이용시간 행
    per_rev_rows = ""
    per_time_rows = ""
    for mo in months:
        e = m["eb_monthly"].get(mo, {})
        s = m["sd_monthly"].get(mo, {})
        er = e["revenue"]/e["cnt"] if e.get("cnt") else 0
        sr = s["revenue"]/s["cnt"] if s.get("cnt") else 0
        et = e["utime_hrs"]/e["cnt"] if e.get("cnt") else 0
        st = s["utime_hrs"]/s["cnt"] if s.get("cnt") else 0
        per_rev_rows += f'<td class="r">{er:,.0f}</td><td class="r">{sr:,.0f}</td>'
        per_time_rows += f'<td class="r">{et:.1f}h</td><td class="r">{st:.1f}h</td>'

    # 월 헤더
    month_headers = ""
    for mo in months:
        month_headers += f'<th class="r" style="color:#60a5fa;">{mo}월 미리예약</th><th class="r" style="color:#facc15;">{mo}월 당일최저가</th>'

    # 현재 시나리오 판단
    latest_month = months[-1]
    latest_eb = m["eb_monthly"].get(latest_month, {})
    latest_tot = m["tot_monthly"].get(latest_month, {})
    latest_contrib = latest_eb.get("revenue",0)/latest_tot["revenue"]*100 if latest_tot.get("revenue") else 0
    latest_eb_gpm = latest_eb["cm"]/latest_eb["revenue"]*100 if latest_eb.get("revenue") else 0

    if latest_contrib < 3:
        current_case = "Case 2(공급 부족)"
        case_color = "red"
    elif latest_contrib > 10:
        current_case = "Case 3(과열)"
        case_color = "green"
    else:
        current_case = "Case 1(정상)"
        case_color = "yellow"

    eb_gpm_str = fmt(m["eb_gpm"], "%")
    sd_gpm_str = fmt(m["sd_gpm"], "%")
    rest_gpm_str = fmt(m["rest_gpm"], "%")
    gpm_diff = m["eb_gpm"] - m["sd_gpm"]

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>얼리버드 판매 전략 리포트</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-annotation"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}body{{font-family:'Apple SD Gothic Neo',-apple-system,sans-serif;background:#0c0e14;color:#d0d0d0;padding:40px;max-width:1360px;margin:0 auto}}.header{{display:flex;justify-content:space-between;align-items:flex-end;margin-bottom:10px}}.header h1{{font-size:26px;font-weight:800;color:#fff}}.header .meta{{text-align:right}}.header .meta .date{{font-size:12px;color:#666}}.header .meta .update{{font-size:11px;color:#444;margin-top:2px}}.divider{{height:1px;background:linear-gradient(90deg,#2a2d3a,transparent);margin-bottom:28px}}.exec-summary{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:32px}}.exec-card{{padding:20px 24px;border-radius:14px}}.exec-card.verdict{{background:linear-gradient(135deg,rgba(96,165,250,0.08),rgba(34,197,94,0.06));border:1px solid rgba(96,165,250,0.2)}}.exec-card.action{{background:rgba(250,204,21,0.06);border:1px solid rgba(250,204,21,0.18)}}.exec-card h2{{font-size:14px;font-weight:700;color:#fff;margin-bottom:10px}}.exec-card p{{font-size:13px;line-height:1.8;color:#bbb}}.exec-card p strong{{color:#fff}}.exec-card .big-num{{font-size:36px;font-weight:800;margin:8px 0}}.section{{margin-bottom:36px}}.sec-head{{display:flex;align-items:center;gap:10px;margin-bottom:16px}}.sec-head h2{{font-size:16px;font-weight:700;color:#fff}}.tag{{font-size:10px;font-weight:600;padding:3px 8px;border-radius:4px;letter-spacing:.5px}}.tag-blue{{background:rgba(96,165,250,0.12);color:#60a5fa}}.tag-green{{background:rgba(34,197,94,0.12);color:#22c55e}}.tag-yellow{{background:rgba(250,204,21,0.12);color:#facc15}}.tag-red{{background:rgba(239,68,68,0.12);color:#ef4444}}.sec-sub{{font-size:11px;color:#555;margin-left:auto}}.kpi-row{{display:grid;grid-template-columns:repeat(5,1fr);gap:12px}}.kpi{{background:#14161e;border-radius:12px;padding:18px;border:1px solid #1e2130}}.kpi .label{{font-size:11px;color:#777;margin-bottom:6px}}.kpi .val{{font-size:22px;font-weight:800;color:#fff}}.kpi .delta{{font-size:11px;margin-top:4px}}.blue{{color:#60a5fa}}.green{{color:#22c55e}}.red{{color:#ef4444}}.yellow{{color:#facc15}}.gray{{color:#777}}.card{{background:#14161e;border-radius:12px;padding:22px;border:1px solid #1e2130}}.g2{{display:grid;grid-template-columns:1fr 1fr;gap:14px}}.g3{{display:grid;grid-template-columns:repeat(3,1fr);gap:14px}}table{{width:100%;border-collapse:collapse;font-size:12.5px}}thead th{{text-align:left;color:#555;font-weight:600;padding:10px 12px;border-bottom:2px solid #1e2130;font-size:11px}}tbody td{{padding:10px 12px;border-bottom:1px solid #1a1c26}}tbody tr:last-child td{{border-bottom:none}}tbody tr:hover{{background:rgba(96,165,250,0.03)}}.r{{text-align:right;font-variant-numeric:tabular-nums}}.row-label{{font-weight:600;color:#ccc;white-space:nowrap}}.row-highlight{{background:rgba(96,165,250,0.04)}}.chart-box{{position:relative;height:270px}}.gauge-wrap{{margin-top:14px}}.gauge-row{{display:flex;align-items:center;gap:10px;margin-bottom:8px}}.gauge-label{{font-size:11px;min-width:80px}}.gauge-bar{{flex:1;height:7px;background:#0c0e14;border-radius:4px;overflow:hidden}}.gauge-fill{{height:100%;border-radius:4px}}.gauge-val{{font-size:12px;font-weight:700;min-width:55px;text-align:right}}.scenario{{background:#14161e;border-radius:12px;padding:20px;border:1px solid #1e2130;position:relative;overflow:hidden}}.scenario::before{{content:'';position:absolute;top:0;left:0;right:0;height:3px}}.scenario.s-red::before{{background:#ef4444}}.scenario.s-yellow::before{{background:#facc15}}.scenario.s-green::before{{background:#22c55e}}.scenario.active{{border-color:rgba(239,68,68,0.4);box-shadow:0 0 24px rgba(239,68,68,0.05)}}.s-header{{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px}}.s-label{{font-size:11px;font-weight:700;letter-spacing:.8px}}.s-status{{font-size:10px;padding:2px 8px;border-radius:10px;font-weight:600}}.s-metric{{font-size:26px;font-weight:800;margin:6px 0}}.s-desc{{font-size:12px;color:#888;line-height:1.6}}.s-action{{margin-top:10px;padding:10px;background:rgba(255,255,255,0.02);border-radius:6px;font-size:12px;color:#aaa;line-height:1.6}}.s-action strong{{color:#ddd}}.transform{{display:grid;grid-template-columns:1fr 40px 1fr;gap:0;align-items:stretch;margin-bottom:16px}}.t-side{{padding:20px;border-radius:12px}}.t-side.old{{background:rgba(239,68,68,0.04);border:1px solid rgba(239,68,68,0.12)}}.t-side.new{{background:rgba(34,197,94,0.04);border:1px solid rgba(34,197,94,0.12)}}.t-arrow{{display:flex;align-items:center;justify-content:center;font-size:20px;color:#444}}.t-side h4{{font-size:13px;font-weight:700;margin-bottom:10px}}.t-side p,.t-side ul{{font-size:12px;color:#aaa;line-height:1.8}}.t-side ul{{padding-left:16px}}.t-side li strong{{color:#fff}}.footer{{text-align:center;color:#333;font-size:10px;margin-top:48px;padding-top:14px;border-top:1px solid #1a1c26}}
</style>
</head>
<body>
<div class="header">
  <h1>얼리버드 판매 전략 리포트</h1>
  <div class="meta">
    <div class="date">데이터 기준: 2026.01.01 ~ {yesterday} (반납 기준)</div>
    <div class="update">마지막 갱신: {now}</div>
  </div>
</div>
<div class="divider"></div>

<div class="exec-summary">
  <div class="exec-card verdict">
    <h2>핵심 판단</h2>
    <div class="big-num blue">GPM {eb_gpm_str}%</div>
    <p>미리예약 GPM이 전체 평균({rest_gpm_str}%)보다 <strong>{m["eb_gpm"]-m["rest_gpm"]:.1f}%p 높아</strong>, 확대할수록 전체 수익성이 개선되는 구조.<br>
    당일최저가(GPM {sd_gpm_str}%)는 구조적 적자. <strong>현 GPM 수준에서는 미리예약을 끌 이유가 없습니다.</strong></p>
  </div>
  <div class="exec-card action">
    <h2>{latest_month}월 현황</h2>
    <div class="big-num {case_color}">기여도 {latest_contrib:.1f}%</div>
    <p><strong>{current_case}</strong> 해당. GPM {latest_eb_gpm:.1f}% 유지 중이므로 <strong>물량 확대해도 수익성 안전</strong>.</p>
  </div>
</div>

<div class="section">
  <div class="sec-head"><h2>핵심 지표</h2><span class="tag tag-blue">누적</span></div>
  <div class="kpi-row">
    <div class="kpi"><div class="label">미리예약 예약건수</div><div class="val">{eb_total["cnt"]:,.0f}</div><div class="delta gray">전체 {all_total["cnt"]:,.0f}건 중 {eb_total["cnt"]/all_total["cnt"]*100:.1f}%</div></div>
    <div class="kpi"><div class="label">미리예약 매출</div><div class="val">{eb_total["revenue"]/1e8:.1f}억</div><div class="delta gray">전체 {all_total["revenue"]/1e8:.1f}억 중 {eb_total["revenue"]/all_total["revenue"]*100:.1f}%</div></div>
    <div class="kpi"><div class="label">미리예약 GPM</div><div class="val blue">{eb_gpm_str}%</div><div class="delta green">당일최저가 대비 +{gpm_diff:.1f}%p</div></div>
    <div class="kpi"><div class="label">미리예약 순이익</div><div class="val green">+{eb_total["profit"]/1e8:.2f}억</div><div class="delta red">당일최저가 {sd_total["profit"]/1e8:.2f}억</div></div>
    <div class="kpi"><div class="label">미리예약 할인율</div><div class="val">{abs(eb_total["rev_discount"])/eb_total["rev_origin"]*100:.1f}%</div><div class="delta green">당일최저가({abs(sd_total["rev_discount"])/sd_total["rev_origin"]*100:.1f}%) 대비 낮음</div></div>
  </div>
  <div class="card" style="margin-top:14px;">
    <div style="font-size:13px;font-weight:700;color:#fff;margin-bottom:4px;">GPM 수준 비교</div>
    <div style="font-size:11px;color:#555;margin-bottom:12px;">미리예약 GPM > 전체 평균 → 확대할수록 전체 수익성 개선</div>
    <div class="gauge-wrap">
      <div class="gauge-row"><div class="gauge-label blue">미리예약</div><div class="gauge-bar"><div class="gauge-fill" style="width:{m["eb_gpm"]:.0f}%;background:linear-gradient(90deg,#3b82f6,#60a5fa);"></div></div><div class="gauge-val blue">{eb_gpm_str}%</div></div>
      <div class="gauge-row"><div class="gauge-label gray">전체 평균</div><div class="gauge-bar"><div class="gauge-fill" style="width:{m["rest_gpm"]:.0f}%;background:linear-gradient(90deg,#555,#888);"></div></div><div class="gauge-val gray">{rest_gpm_str}%</div></div>
      <div class="gauge-row"><div class="gauge-label yellow">당일최저가</div><div class="gauge-bar"><div class="gauge-fill" style="width:{m["sd_gpm"]:.0f}%;background:linear-gradient(90deg,#ca8a04,#facc15);"></div></div><div class="gauge-val yellow">{sd_gpm_str}%</div></div>
    </div>
  </div>
</div>

<div class="section">
  <div class="sec-head"><h2>미리예약 OFF 기준</h2><span class="tag tag-red">STOP-LOSS</span></div>
  <div class="card">
    <div style="margin-top:4px;padding:16px 18px;background:rgba(96,165,250,0.05);border-radius:10px;border-left:3px solid #60a5fa;">
      <div style="font-size:13px;font-weight:700;color:#fff;margin-bottom:8px;">GPM이 {rest_gpm_str}%(전체 평균) 아래로 2주 연속 하락할 때 중단</div>
      <div style="font-size:13px;color:#bbb;line-height:1.8;">현재 미리예약 GPM <strong class="blue">{eb_gpm_str}%</strong> → 임계점까지 <strong style="color:#fff;">{m["eb_gpm"]-m["rest_gpm"]:.1f}%p 여유</strong>. 기여도 10%+ 이어도 GPM 유지 시 끄지 않는 것이 유리.</div>
    </div>
  </div>
</div>

<div class="section">
  <div class="sec-head"><h2>월별 성과 비교</h2><span class="tag tag-blue">DETAIL</span></div>
  <div class="card">
    <table>
      <thead><tr><th>항목</th>{month_headers}</tr></thead>
      <tbody>
        {monthly_row("예약 건수", "cnt")}
        {monthly_row("매출 (억)", "revenue", "억")}
        {monthly_row("공헌이익 (억)", "cm", "억")}
        <tr><td class="row-label">순이익 (억)</td>{"".join(f'<td class="r {"green" if m["eb_monthly"].get(mo,{{}}).get("profit",0)>=0 else "red"}">{m["eb_monthly"].get(mo,{{}}).get("profit",0)/1e8:+.2f}</td><td class="r {"green" if m["sd_monthly"].get(mo,{{}}).get("profit",0)>=0 else "red"}">{m["sd_monthly"].get(mo,{{}}).get("profit",0)/1e8:+.2f}</td>' for mo in months)}</tr>
        <tr class="row-highlight"><td class="row-label">GPM</td>{gpm_rows}</tr>
        <tr><td class="row-label">순이익률</td>{profit_rows}</tr>
        <tr><td class="row-label">건당 매출</td>{per_rev_rows}</tr>
        <tr><td class="row-label">건당 이용시간</td>{per_time_rows}</tr>
        <tr><td class="row-label">할인율</td>{disc_rows}</tr>
        <tr><td class="row-label">매출 기여도</td>{contrib_rows}</tr>
      </tbody>
    </table>
  </div>
</div>

<div class="section">
  <div class="sec-head"><h2>주간 추이</h2><span class="tag tag-blue">TREND</span></div>
  <div class="g2">
    <div class="card"><div style="font-size:13px;font-weight:700;color:#fff;margin-bottom:10px;">예약 건수</div><div class="chart-box"><canvas id="cntChart"></canvas></div></div>
    <div class="card"><div style="font-size:13px;font-weight:700;color:#fff;margin-bottom:10px;">GPM (공헌이익률)</div><div class="chart-box"><canvas id="gpmChart"></canvas></div></div>
    <div class="card"><div style="font-size:13px;font-weight:700;color:#fff;margin-bottom:10px;">매출 (억원)</div><div class="chart-box"><canvas id="revChart"></canvas></div></div>
    <div class="card"><div style="font-size:13px;font-weight:700;color:#fff;margin-bottom:10px;">순이익 (억원)</div><div class="chart-box"><canvas id="profitChart"></canvas></div></div>
  </div>
</div>

<div class="section">
  <div class="sec-head"><h2>판매 기준 재설정 권고</h2><span class="tag tag-red">RECOMMENDATION</span></div>
  <div class="transform">
    <div class="t-side old"><h4 style="color:#ef4444;">AS-IS: 물량 기준</h4><p>매출 비중 <strong style="color:#fff;">10% 초과 시 무조건 중단</strong>. 수익성과 무관하게 물량만으로 판단.</p></div>
    <div class="t-arrow">→</div>
    <div class="t-side new"><h4 style="color:#22c55e;">TO-BE: GPM 기준</h4><ul><li><strong class="green">GPM 65%↑</strong> → 적극 확대</li><li><strong class="yellow">GPM 55~65%</strong> → 현상 유지</li><li><strong class="red">GPM 55%↓ 2주 연속</strong> → 중단</li></ul></div>
  </div>
</div>

<div class="footer">socar-data.socar_biz_profit.profit_socar_reservation | Auto-refresh: 매일 09:00 KST | {now}</div>

<script>
const W={json.dumps(weeks_labels)};
const eb={{cnt:{json.dumps(eb_cnt)},rev:{json.dumps(eb_rev)},cm:{json.dumps(eb_cm)},pft:{json.dumps(eb_pft)}}};
const sd={{cnt:{json.dumps(sd_cnt)},rev:{json.dumps(sd_rev)},cm:{json.dumps(sd_cm)},pft:{json.dumps(sd_pft)}}};
const ebG=eb.cm.map((v,i)=>eb.rev[i]>0?+(v/eb.rev[i]*100).toFixed(1):0);
const sdG=sd.cm.map((v,i)=>sd.rev[i]>0?+(v/sd.rev[i]*100).toFixed(1):0);
const C={{eb:'#60a5fa',ebB:'rgba(96,165,250,0.10)',sd:'#facc15',sdB:'rgba(250,204,21,0.10)'}};
const O={{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{labels:{{color:'#666',font:{{size:10}},usePointStyle:true,pointStyle:'circle'}}}}}},scales:{{x:{{ticks:{{color:'#444',font:{{size:9}},maxRotation:0}},grid:{{color:'#16181f'}}}},y:{{ticks:{{color:'#444',font:{{size:10}}}},grid:{{color:'#1a1c26'}}}}}}}};
new Chart(document.getElementById('cntChart'),{{type:'bar',data:{{labels:W,datasets:[{{label:'미리예약',data:eb.cnt,backgroundColor:C.eb,borderRadius:3,barPercentage:.65}},{{label:'당일최저가',data:sd.cnt,backgroundColor:C.sd,borderRadius:3,barPercentage:.65}}]}},options:O}});
new Chart(document.getElementById('gpmChart'),{{type:'line',data:{{labels:W,datasets:[{{label:'미리예약',data:ebG,borderColor:C.eb,backgroundColor:C.ebB,fill:true,tension:.3,pointRadius:3,borderWidth:2}},{{label:'당일최저가',data:sdG,borderColor:C.sd,backgroundColor:C.sdB,fill:true,tension:.3,pointRadius:3,borderWidth:2}}]}},options:{{...O,scales:{{...O.scales,y:{{...O.scales.y,min:30,max:80,ticks:{{...O.scales.y.ticks,callback:v=>v+'%'}}}}}},plugins:{{...O.plugins,annotation:{{annotations:{{g:{{type:'line',yMin:65,yMax:65,borderColor:'rgba(34,197,94,0.3)',borderWidth:1,borderDash:[6,4]}},r:{{type:'line',yMin:55,yMax:55,borderColor:'rgba(239,68,68,0.3)',borderWidth:1,borderDash:[6,4]}},a:{{type:'line',yMin:63,yMax:63,borderColor:'rgba(255,255,255,0.12)',borderWidth:1,borderDash:[3,3]}}}}}}}}}}}});
new Chart(document.getElementById('revChart'),{{type:'line',data:{{labels:W,datasets:[{{label:'미리예약',data:eb.rev,borderColor:C.eb,backgroundColor:C.ebB,fill:true,tension:.3,pointRadius:3,borderWidth:2}},{{label:'당일최저가',data:sd.rev,borderColor:C.sd,backgroundColor:C.sdB,fill:true,tension:.3,pointRadius:3,borderWidth:2}}]}},options:{{...O,scales:{{...O.scales,y:{{...O.scales.y,ticks:{{...O.scales.y.ticks,callback:v=>v+'억'}}}}}}}}}});
new Chart(document.getElementById('profitChart'),{{type:'line',data:{{labels:W,datasets:[{{label:'미리예약',data:eb.pft,borderColor:C.eb,backgroundColor:C.ebB,fill:true,tension:.3,pointRadius:3,borderWidth:2}},{{label:'당일최저가',data:sd.pft,borderColor:C.sd,backgroundColor:C.sdB,fill:true,tension:.3,pointRadius:3,borderWidth:2}}]}},options:{{...O,scales:{{...O.scales,y:{{...O.scales.y,ticks:{{...O.scales.y.ticks,callback:v=>v+'억'}}}}}},plugins:{{...O.plugins,annotation:{{annotations:{{z:{{type:'line',yMin:0,yMax:0,borderColor:'rgba(255,255,255,0.15)',borderWidth:1,borderDash:[4,4]}}}}}}}}}}}});
</script>
</body></html>"""
    return html


def main():
    print(f"[{datetime.now()}] 얼리버드 대시보드 갱신 시작...")
    client = bigquery.Client(project=PROJECT_ID)

    print("  BigQuery 쿼리 실행 중...")
    coupon_rows = run_query(client, Q_COUPON)
    total_rows = run_query(client, Q_TOTAL)
    weekly_rows = run_query(client, Q_WEEKLY)

    print("  메트릭 계산 중...")
    metrics = calc_metrics(coupon_rows, total_rows, weekly_rows)

    print("  HTML 생성 중...")
    html = generate_html(metrics)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"  저장 완료: {OUTPUT_PATH}")
    print(f"[{datetime.now()}] 갱신 완료!")


if __name__ == "__main__":
    main()
