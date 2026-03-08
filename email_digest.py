import smtplib, os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime


def _score_color(score):
    if score >= 75: return "#16a34a"
    if score >= 55: return "#d97706"
    return "#dc2626"

def _priority_label(score):
    if score >= 75: return "A — High Priority"
    if score >= 55: return "B — Medium"
    return "C — Low"

def _prob_color(p):
    if p >= 75: return "#dc2626"
    if p >= 50: return "#d97706"
    return "#16a34a"

def _lead_card(lead):
    sc    = lead.get("score", 0)
    prob  = lead.get("impact_prob", 0)
    color = _score_color(sc)
    plbl  = _priority_label(sc)
    val   = lead.get("est_value", "")
    dev   = lead.get("developer") or lead.get("applicant") or ""
    arch  = lead.get("architect") or lead.get("agent") or ""
    ch    = lead.get("contact_link", "")
    trigg = lead.get("triggers", "")
    desc  = (lead.get("desc") or "")[:220]
    addr  = (lead.get("addr") or "")[:90]
    dated = lead.get("date_dec", "")
    url   = lead.get("url", "")
    doc   = lead.get("doc_url", "")

    val_badge = (
        '<span style="background:#f0fdf4;color:#15803d;border:1px solid #bbf7d0;'
        'border-radius:20px;padding:2px 10px;font-size:11px;font-weight:600;margin-left:6px;">'
        f'&#128176; {val}</span>'
    ) if val else ""

    chips = "".join(
        '<span style="background:#eff6ff;color:#1d4ed8;border:1px solid #bfdbfe;'
        'border-radius:20px;padding:1px 8px;font-size:11px;margin:2px 2px 0 0;'
        f'display:inline-block;">{t.strip()}</span>'
        for t in trigg.split(",") if t.strip()
    )

    dev_html = (
        f'<a href="{ch}" style="color:#1e40af;text-decoration:none;">{dev}</a>' if (dev and ch)
        else (dev or "&#8212;")
    )

    btns = ""
    if url:
        btns += (
            '<a href="{}" style="display:inline-block;margin-right:8px;padding:6px 14px;'
            'background:#1e40af;color:#fff;border-radius:6px;font-size:12px;'
            'text-decoration:none;">&#128196; View Application</a>'.format(url)
        )
    if doc:
        btns += (
            '<a href="{}" style="display:inline-block;margin-right:8px;padding:6px 14px;'
            'background:#374151;color:#fff;border-radius:6px;font-size:12px;'
            'text-decoration:none;">&#128209; Decision PDF</a>'.format(doc)
        )
    if ch:
        btns += (
            '<a href="{}" style="display:inline-block;padding:6px 14px;'
            'background:#374151;color:#fff;border-radius:6px;font-size:12px;'
            'text-decoration:none;">&#127968; Companies House</a>'.format(ch)
        )

    pc = _prob_color(prob)
    pw = min(max(prob, 0), 100)

    return (
        '<div style="background:#fff;border:1px solid #e5e7eb;border-radius:10px;'
        f'margin-bottom:16px;border-left:4px solid {color};">'
        '<div style="padding:16px 20px 14px;">'

        # Header row
        '<div style="display:flex;align-items:center;flex-wrap:wrap;gap:6px;margin-bottom:8px;">'
        '<span style="background:#dbeafe;color:#1e40af;border-radius:20px;font-size:11px;'
        f'font-weight:600;letter-spacing:.06em;text-transform:uppercase;padding:2px 9px;">{lead.get("council","")}</span>'
        f'<span style="font-family:monospace;font-size:11px;color:#6b7280;">{lead.get("ref","")}</span>'
        f'<span style="background:{color}20;color:{color};border:1px solid {color}40;'
        f'border-radius:20px;padding:2px 9px;font-size:11px;font-weight:600;">{plbl}</span>'
        f'<span style="font-family:monospace;font-size:14px;font-weight:700;color:{color};margin-left:auto;">{sc}/100</span>'
        f'{val_badge}'
        '</div>'

        # Description
        f'<div style="font-size:14px;color:#111827;margin-bottom:5px;line-height:1.5;">{desc}</div>'
        f'<div style="font-size:12px;color:#6b7280;margin-bottom:8px;">&#128205; {addr} &nbsp;&#183;&nbsp; &#128197; {dated}</div>'

        # Trigger chips
        f'<div style="margin-bottom:10px;">{chips}</div>'

        # Probability bar
        '<div style="margin-bottom:12px;">'
        f'<div style="font-size:11px;color:#6b7280;margin-bottom:4px;">Impact study probability: <strong style="color:{pc};">{prob}%</strong></div>'
        '<div style="background:#e5e7eb;border-radius:3px;height:5px;">'
        f'<div style="background:{pc};height:5px;border-radius:3px;width:{pw}%;"></div>'
        '</div></div>'

        # Sales intel
        '<div style="background:#f9fafb;border-radius:6px;padding:10px 12px;margin-bottom:12px;font-size:12px;color:#374151;">'
        f'<div style="margin-bottom:4px;">&#127970; <strong>Developer:</strong> {dev_html}</div>'
        f'<div>&#128208; <strong>Architect / Agent:</strong> {arch or "&#8212;"}</div>'
        '</div>'

        f'<div>{btns}</div>'
        '</div></div>'
    )


def build_html(new_leads, summary, failed, date_from, date_to):
    total    = len(new_leads)
    high     = sum(1 for l in new_leads if l.get("score", 0) >= 75)
    avg_sc   = int(sum(l.get("score",0) for l in new_leads) / total) if total else 0
    avg_prob = int(sum(l.get("impact_prob",0) for l in new_leads) / total) if total else 0
    run_dt   = datetime.now().strftime("%A %d %B %Y, %H:%M UTC")

    cards = "".join(_lead_card(l) for l in new_leads)

    no_leads = (
        '<div style="text-align:center;padding:40px;color:#6b7280;font-size:14px;">'
        'No new qualified leads found this week.</div>'
    ) if not new_leads else ""

    stat_items = [
        (str(total),       "New Leads",      "#1e40af"),
        (str(high),        "A Priority",     "#16a34a"),
        (str(avg_sc),      "Avg Score",      "#d97706"),
        (f"{avg_prob}%",   "Impact Prob",    "#dc2626"),
    ]
    stats_html = "".join(
        '<td style="width:25%;text-align:center;">'
        '<div style="background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:14px 8px;">'
        f'<div style="font-family:monospace;font-size:1.6rem;font-weight:700;color:{col};">{val}</div>'
        f'<div style="font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:.07em;margin-top:3px;">{lbl}</div>'
        '</div></td>'
        for val, lbl, col in stat_items
    )

    council_rows = "".join(
        '<tr style="border-bottom:1px solid #f3f4f6;">'
        f'<td style="padding:5px 0;font-size:12px;color:#374151;">{cn}</td>'
        '<td style="padding:5px 0;font-size:12px;color:#6b7280;text-align:right;">'
        f'{"&#127942; " + str(n) + " lead" + ("s" if n != 1 else "") if n else "&#8212;"}</td>'
        '</tr>'
        for cn, n in sorted(summary.items(), key=lambda x: -x[1])
    )
    failed_row = (
        '<tr><td colspan="2" style="padding:6px 0;font-size:12px;color:#dc2626;">'
        f'&#9888; Failed: {", ".join(failed)}</td></tr>'
    ) if failed else ""

    return (
        '<!DOCTYPE html><html><head><meta charset="UTF-8"/></head>'
        '<body style="margin:0;padding:0;background:#f3f4f6;font-family:Helvetica Neue,Arial,sans-serif;">'
        '<div style="max-width:680px;margin:0 auto;padding:24px 16px;">'

        # Header
        '<div style="background:#0f172a;border-radius:12px;padding:28px 32px;margin-bottom:16px;">'
        '<div style="font-size:22px;font-weight:700;color:#fff;margin-bottom:4px;">&#127959; MAPlanning</div>'
        '<div style="font-size:13px;color:#94a3b8;margin-bottom:12px;">Retail Lead Intelligence &middot; Weekly Digest</div>'
        f'<div style="font-size:12px;color:#64748b;">&#128197; Period: {date_from} &rarr; {date_to} &nbsp;&middot;&nbsp; Run: {run_dt}</div>'
        '</div>'

        # Stats
        f'<table style="width:100%;border-collapse:separate;border-spacing:8px;margin-bottom:16px;"><tr>{stats_html}</tr></table>'

        # Leads heading
        f'<div style="font-size:12px;font-weight:600;color:#374151;margin-bottom:12px;'
        f'letter-spacing:.06em;text-transform:uppercase;">{"New Qualified Leads" if new_leads else "No New Leads This Week"}</div>'

        f'{cards}{no_leads}'

        # Council summary
        '<div style="background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:16px 20px;margin-top:8px;">'
        '<div style="font-size:12px;font-weight:600;color:#374151;margin-bottom:10px;'
        'text-transform:uppercase;letter-spacing:.06em;">Councils Scraped</div>'
        f'<table style="width:100%;border-collapse:collapse;">{council_rows}{failed_row}</table>'
        '</div>'

        # Footer
        '<div style="text-align:center;padding:20px 0 8px;font-size:11px;color:#9ca3af;">'
        f'MAPlanning Retail Intelligence &middot; Automated weekly digest &middot; {datetime.now().year}'
        '</div>'

        '</div></body></html>'
    )


def send_digest(new_leads, summary, failed, date_from, date_to, log_fn=print):
    gmail_user = os.environ.get("GMAIL_FROM", "").strip()
    gmail_pass = os.environ.get("GMAIL_APP_PASSWORD", "").strip()
    to_raw     = os.environ.get("GMAIL_TO", "inger.balaj@gmail.com")
    recipients = [r.strip() for r in to_raw.split(",") if r.strip()]

    if not gmail_user or not gmail_pass:
        log_fn("  ⚠️  Email skipped — GMAIL_FROM or GMAIL_APP_PASSWORD secret not set")
        return

    n = len(new_leads)
    subject = (
        f"MAPlanning · {n} new retail lead{'s' if n != 1 else ''} ({date_from} – {date_to})"
        if n else
        f"MAPlanning · No new leads this week ({date_from} – {date_to})"
    )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"MAPlanning <{gmail_user}>"
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(build_html(new_leads, summary, failed, date_from, date_to), "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(gmail_user, gmail_pass)
            smtp.sendmail(gmail_user, recipients, msg.as_string())
        log_fn(f"  ✅ Email digest sent → {recipients}")
        log_fn(f"     Subject: {subject}")
    except Exception as e:
        log_fn(f"  ❌ Email failed: {e}")
