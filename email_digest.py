"""
send_email.py  —  Standalone Monday morning email digest.
Reads leads added to Google Sheet in the last 7 days and sends
a formatted HTML briefing.  Run separately from the scraper.

Env vars required:
  GCP_SERVICE_ACCOUNT_JSON  — full service account JSON string
  GMAIL_FROM                — sender Gmail address
  GMAIL_APP_PASSWORD        — Gmail App Password (16 chars)
  GMAIL_TO                  — comma-separated recipients
"""
import os, json, sys, smtplib
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import gspread
from google.oauth2.service_account import Credentials

# ── CONFIG ────────────────────────────────────────────────────
SHEET_ID   = "172bpv-b2_nK5ENE1XPk5rWeokvnr1sjHvLBfVzHWh6c"
SHEET_NAME = "Leads"
DAYS_BACK  = 8    # leads added within last 8 days count as "this week"

COL = dict(council=0, ref=1, addr=2, desc=3, app_type=4,
           applicant=5, agent=6, date_rec=7, date_dec=8,
           triggers=10, score=11, keyword=12,
           portal=13, date_found=14, comments=15,
           est_value=16, developer=17, architect=18,
           impact_prob=19, ch_number=20, reg_addr=21, contact_link=22)

def cell(row, key):
    idx = COL.get(key, -1)
    if idx < 0 or idx >= len(row): return ""
    return str(row[idx]).strip()

# ── FETCH ─────────────────────────────────────────────────────
def load_new_leads():
    sa_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON","").strip()
    if not sa_json:
        print("❌ GCP_SERVICE_ACCOUNT_JSON not set"); sys.exit(1)
    info  = json.loads(sa_json)
    creds = Credentials.from_service_account_info(info, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ])
    gc = gspread.authorize(creds)
    ws = gc.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    rows = ws.get_all_values()[1:]   # skip header

    cutoff = datetime.now() - timedelta(days=DAYS_BACK)
    new_leads = []
    total_leads = len(rows)

    for row in rows:
        df_str = cell(row, "date_found")
        if not df_str:
            continue
        try:
            df = datetime.strptime(df_str[:16], "%Y-%m-%d %H:%M")
        except Exception:
            try: df = datetime.strptime(df_str[:10], "%Y-%m-%d")
            except Exception: continue
        if df >= cutoff:
            try:
                sc = int(cell(row, "score"))
            except Exception:
                sc = 0
            try:
                prob = int(str(cell(row,"impact_prob")).replace("%",""))
            except Exception:
                prob = 0
            new_leads.append({
                "council":      cell(row,"council"),
                "ref":          cell(row,"ref"),
                "addr":         cell(row,"addr"),
                "desc":         cell(row,"desc"),
                "applicant":    cell(row,"applicant"),
                "agent":        cell(row,"agent"),
                "date_dec":     cell(row,"date_dec"),
                "triggers":     cell(row,"triggers"),
                "score":        sc,
                "portal":       cell(row,"portal"),
                "est_value":    cell(row,"est_value"),
                "developer":    cell(row,"developer"),
                "architect":    cell(row,"architect"),
                "impact_prob":  prob,
                "contact_link": cell(row,"contact_link"),
                "ch_number":    cell(row,"ch_number"),
            })

    new_leads.sort(key=lambda x: x["score"], reverse=True)
    print(f"✅ Sheet has {total_leads} total leads, {len(new_leads)} added in last {DAYS_BACK} days")
    return new_leads, total_leads

# ── HTML BUILDERS ─────────────────────────────────────────────
def _sc_color(s):
    return "#16a34a" if s>=75 else "#d97706" if s>=55 else "#dc2626"

def _p_color(p):
    return "#dc2626" if p>=75 else "#d97706" if p>=50 else "#16a34a"

def _plabel(s):
    return "A — High Priority" if s>=75 else "B — Medium" if s>=55 else "C — Low"

def _card(lead):
    sc   = lead["score"]
    prob = lead["impact_prob"]
    col  = _sc_color(sc)
    pc   = _p_color(prob)
    dev  = lead["developer"] or lead["applicant"] or "—"
    arch = lead["architect"] or lead["agent"] or "—"
    ch   = lead["contact_link"]
    val  = lead["est_value"]

    chips = "".join(
        '<span style="background:#eff6ff;color:#1d4ed8;border:1px solid #bfdbfe;'
        'border-radius:20px;padding:1px 8px;font-size:11px;margin:2px 2px 0 0;'
        f'display:inline-block;">{t.strip()}</span>'
        for t in lead["triggers"].split(",") if t.strip()
    )
    dev_html = (f'<a href="{ch}" style="color:#1e40af;text-decoration:none;">{dev}</a>'
                if ch else dev)
    val_badge = (
        '<span style="background:#f0fdf4;color:#15803d;border:1px solid #bbf7d0;'
        'border-radius:20px;padding:2px 10px;font-size:11px;font-weight:600;margin-left:6px;">'
        f'&#128176; {val}</span>'
    ) if val else ""

    btns = ""
    if lead["portal"]:
        btns += (f'<a href="{lead["portal"]}" style="display:inline-block;margin-right:8px;'
                 f'padding:6px 14px;background:#1e40af;color:#fff;border-radius:6px;'
                 f'font-size:12px;text-decoration:none;">&#128196; View Application</a>')
    if ch:
        btns += (f'<a href="{ch}" style="display:inline-block;padding:6px 14px;'
                 f'background:#374151;color:#fff;border-radius:6px;font-size:12px;'
                 f'text-decoration:none;">&#127968; Companies House</a>')

    return (
        f'<div style="background:#fff;border:1px solid #e5e7eb;border-radius:10px;'
        f'margin-bottom:16px;border-left:4px solid {col};">'
        f'<div style="padding:16px 20px 14px;">'
        # header
        f'<div style="display:flex;align-items:center;flex-wrap:wrap;gap:6px;margin-bottom:8px;">'
        f'<span style="background:#dbeafe;color:#1e40af;border-radius:20px;font-size:11px;'
        f'font-weight:600;letter-spacing:.06em;text-transform:uppercase;padding:2px 9px;">{lead["council"]}</span>'
        f'<span style="font-family:monospace;font-size:11px;color:#6b7280;">{lead["ref"]}</span>'
        f'<span style="background:{col}20;color:{col};border:1px solid {col}40;'
        f'border-radius:20px;padding:2px 9px;font-size:11px;font-weight:600;">{_plabel(sc)}</span>'
        f'<span style="font-family:monospace;font-size:14px;font-weight:700;color:{col};margin-left:auto;">{sc}/100</span>'
        f'{val_badge}</div>'
        # desc
        f'<div style="font-size:14px;color:#111827;margin-bottom:5px;line-height:1.5;">'
        f'{lead["desc"][:220]}</div>'
        f'<div style="font-size:12px;color:#6b7280;margin-bottom:8px;">&#128205; {lead["addr"][:90]}'
        f'&nbsp;&#183;&nbsp;&#128197; {lead["date_dec"]}</div>'
        f'<div style="margin-bottom:10px;">{chips}</div>'
        # prob bar
        f'<div style="margin-bottom:12px;">'
        f'<div style="font-size:11px;color:#6b7280;margin-bottom:4px;">Impact study probability: '
        f'<strong style="color:{pc};">{prob}%</strong></div>'
        f'<div style="background:#e5e7eb;border-radius:3px;height:5px;">'
        f'<div style="background:{pc};height:5px;border-radius:3px;width:{min(prob,100)}%;"></div>'
        f'</div></div>'
        # sales intel
        f'<div style="background:#f9fafb;border-radius:6px;padding:10px 12px;margin-bottom:12px;font-size:12px;color:#374151;">'
        f'<div style="margin-bottom:4px;">&#127970; <strong>Developer:</strong> {dev_html}</div>'
        f'<div>&#128208; <strong>Architect / Agent:</strong> {arch}</div>'
        f'</div>'
        f'<div>{btns}</div>'
        f'</div></div>'
    )

def build_html(leads, total):
    n     = len(leads)
    high  = sum(1 for l in leads if l["score"]>=75)
    avg_s = int(sum(l["score"] for l in leads)/n) if n else 0
    avg_p = int(sum(l["impact_prob"] for l in leads)/n) if n else 0
    run_dt = datetime.now().strftime("%A %d %B %Y, %H:%M UTC")
    cutoff = (datetime.now()-timedelta(days=DAYS_BACK)).strftime("%d %b %Y")

    cards = "".join(_card(l) for l in leads)
    no_leads = (
        '<div style="text-align:center;padding:40px;color:#6b7280;font-size:14px;">'
        'No new qualified leads found this week.</div>'
    ) if not leads else ""

    stats = "".join(
        '<td style="width:25%;text-align:center;">'
        '<div style="background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:14px 8px;">'
        f'<div style="font-family:monospace;font-size:1.5rem;font-weight:700;color:{col};">{val}</div>'
        f'<div style="font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:.07em;margin-top:3px;">{lbl}</div>'
        '</div></td>'
        for val, lbl, col in [
            (n,          "New Leads",    "#1e40af"),
            (high,       "A Priority",   "#16a34a"),
            (f"{avg_s}", "Avg Score",    "#d97706"),
            (f"{avg_p}%","Impact Prob",  "#dc2626"),
        ]
    )

    return (
        '<!DOCTYPE html><html><head><meta charset="UTF-8"/></head>'
        '<body style="margin:0;padding:0;background:#f3f4f6;font-family:Helvetica Neue,Arial,sans-serif;">'
        '<div style="max-width:680px;margin:0 auto;padding:24px 16px;">'
        '<div style="background:#0f172a;border-radius:12px;padding:28px 32px;margin-bottom:16px;">'
        '<div style="font-size:22px;font-weight:700;color:#fff;margin-bottom:4px;">&#127959; MAPlanning</div>'
        '<div style="font-size:13px;color:#94a3b8;margin-bottom:12px;">Retail Lead Intelligence &middot; Weekly Digest</div>'
        f'<div style="font-size:12px;color:#64748b;">&#128197; Leads added since {cutoff} &nbsp;&middot;&nbsp; {run_dt}</div>'
        f'<div style="font-size:12px;color:#475569;margin-top:6px;">Total in sheet: <strong style="color:#94a3b8;">{total}</strong></div>'
        '</div>'
        f'<table style="width:100%;border-collapse:separate;border-spacing:8px;margin-bottom:16px;"><tr>{stats}</tr></table>'
        f'<div style="font-size:12px;font-weight:600;color:#374151;margin-bottom:12px;'
        f'letter-spacing:.06em;text-transform:uppercase;">{"New Qualified Leads This Week" if leads else "No New Leads This Week"}</div>'
        f'{cards}{no_leads}'
        '<div style="text-align:center;padding:20px 0 8px;font-size:11px;color:#9ca3af;">'
        f'MAPlanning Retail Intelligence &middot; Automated weekly digest &middot; {datetime.now().year}'
        '</div></div></body></html>'
    )

# ── SEND ──────────────────────────────────────────────────────
def send():
    leads, total = load_new_leads()

    gmail_user = os.environ.get("GMAIL_FROM","").strip()
    gmail_pass = os.environ.get("GMAIL_APP_PASSWORD","").strip()
    to_raw     = os.environ.get("GMAIL_TO","inger.balaj@gmail.com")
    recipients = [r.strip() for r in to_raw.split(",") if r.strip()]

    if not gmail_user or not gmail_pass:
        print("❌ GMAIL_FROM or GMAIL_APP_PASSWORD not set"); sys.exit(1)

    n = len(leads)
    subject = (
        f"MAPlanning · {n} new retail lead{'s' if n!=1 else ''} this week"
        if n else "MAPlanning · No new leads this week"
    )
    html = build_html(leads, total)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"MAPlanning <{gmail_user}>"
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(gmail_user, gmail_pass)
        smtp.sendmail(gmail_user, recipients, msg.as_string())

    print(f"✅ Email sent → {recipients}")
    print(f"   Subject: {subject}")
    print(f"   {n} new leads in digest (out of {total} total in sheet)")

if __name__ == "__main__":
    send()
