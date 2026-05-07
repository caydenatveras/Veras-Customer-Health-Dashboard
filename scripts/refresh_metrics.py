#!/usr/bin/env python3
"""
scripts/refresh_metrics.py
Daily auto-refresh of structured rawMetrics fields for the Veras Customer Health Dashboard.

Iterates all accounts in community_registry.json, calls HubSpot / Metabase / Slack /
Intercom APIs, patches rawMetrics in the latest data/{slug}.json entry, and writes
the file back to disk (committed to git by the GitHub Actions step).

LLM-only fields (tonePositiveCount, toneNegativeCount, hasEscalationLanguage,
metricDetails summaries, actions, notes) are NEVER overwritten.

Env vars:
  HUBSPOT_API_KEY   — HubSpot private app token
  METABASE_TOKEN    — Metabase API key (mb_...)
  SLACK_BOT_TOKEN   — Slack bot token (xoxb-...)
  INTERCOM_TOKEN    — Intercom access token
  DRY_RUN           — set to "1" to print changes without writing files
"""

import json
import os
import re
import sys
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

# ── Config ────────────────────────────────────────────────────────────────────
HUBSPOT_TOKEN  = os.environ.get('HUBSPOT_API_KEY', '')
METABASE_TOKEN = os.environ.get('METABASE_TOKEN', '')
SLACK_TOKEN    = os.environ.get('SLACK_BOT_TOKEN', '')
INTERCOM_TOKEN = os.environ.get('INTERCOM_TOKEN', '')
DRY_RUN        = os.environ.get('DRY_RUN', '') == '1'

METABASE_URL = 'https://metabase-prod.veras.com'
TODAY        = datetime.now(timezone.utc).date()
LOOKBACK     = TODAY - timedelta(days=90)
LOOKBACK_MS  = int(datetime(LOOKBACK.year, LOOKBACK.month, LOOKBACK.day, tzinfo=timezone.utc).timestamp() * 1000)

# HubSpot constants
TICKET_PIPELINE  = 't_4d47f840d1d899e8fa6ef5dda9bc5aa4'
STAGE_DONE       = '1177550115'
STAGE_REJECTED   = '1177550116'
STAGE_WITH_ENG   = '1288611854'
NPS_STAGE_ACTIVE = '1189366066'

STAGE_LABELS = {
    '1177550112': 'New',
    '1177550114': 'Backlog',
    '1288468571': 'Needs Info',
    '1193320038': 'Friday Fury',
    STAGE_WITH_ENG: 'With Eng',
}

EXEC_RE = re.compile(
    r'director|vp\b|vice\s+president|president|ceo|coo|cfo|cto|'
    r'executive|regional|owner|administrator|chief',
    re.IGNORECASE,
)

_PORTAL_ID: int | None = None


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def hs_post(path: str, body: dict) -> dict:
    r = requests.post(
        f'https://api.hubapi.com{path}',
        json=body,
        headers={'Authorization': f'Bearer {HUBSPOT_TOKEN}', 'Content-Type': 'application/json'},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()


def hs_get(path: str, params: dict | None = None) -> dict:
    r = requests.get(
        f'https://api.hubapi.com{path}',
        params=params,
        headers={'Authorization': f'Bearer {HUBSPOT_TOKEN}'},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()


def hs_search(obj_type: str, filters: list, properties: list, limit: int = 100) -> list:
    """Search HubSpot CRM objects, automatically paginating through all results."""
    results = []
    after = None
    while True:
        body: dict = {'filterGroups': [{'filters': filters}], 'properties': properties, 'limit': limit}
        if after:
            body['after'] = after
        data = hs_post(f'/crm/v3/objects/{obj_type}/search', body)
        results.extend(data.get('results', []))
        after = (data.get('paging') or {}).get('next', {}).get('after')
        if not after:
            break
    return results


def mb_query(sql: str) -> list[dict]:
    """Run a native SQL query in Metabase and return list of row dicts."""
    r = requests.post(
        f'{METABASE_URL}/api/dataset',
        json={'database': 2, 'type': 'native', 'native': {'query': sql}},
        headers={'x-api-key': METABASE_TOKEN, 'Content-Type': 'application/json'},
        timeout=45,
    )
    r.raise_for_status()
    data = r.json()
    if data.get('error') or data.get('status') == 'failed':
        raise RuntimeError(f"Metabase error: {data.get('error') or data.get('status')}")
    cols = [c['name'] for c in data['data']['cols']]
    return [dict(zip(cols, row)) for row in data['data']['rows']]


def get_portal_id() -> int:
    global _PORTAL_ID
    if _PORTAL_ID:
        return _PORTAL_ID
    info = hs_get('/account-info/v3/details')
    _PORTAL_ID = info['portalId']
    return _PORTAL_ID


def ticket_url(ticket_id: str) -> str:
    return f'https://app.hubspot.com/contacts/{get_portal_id()}/ticket/{ticket_id}'


# ── Date helpers ──────────────────────────────────────────────────────────────

def days_since(value) -> int | None:
    """Days between today and a date value (ISO string, ms timestamp, or None)."""
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            dt = datetime.fromtimestamp(value / 1000, tz=timezone.utc).date()
        elif isinstance(value, str):
            s = value.strip()
            # Handle ms timestamps stored as strings
            if s.isdigit() and len(s) > 10:
                dt = datetime.fromtimestamp(int(s) / 1000, tz=timezone.utc).date()
            else:
                dt = datetime.fromisoformat(s.replace('Z', '+00:00')).date()
        else:
            return None
        return max(0, (TODAY - dt).days)
    except (ValueError, TypeError, OverflowError):
        return None


def is_complete_month(month_str) -> bool:
    """True if month_str (ISO date or datetime, e.g. '2026-03-01T00:00:00Z') is before current month."""
    if not month_str:
        return False
    try:
        s = str(month_str)[:7]   # '2026-03'
        current = TODAY.strftime('%Y-%m')
        return s < current
    except Exception:
        return False


# ── HubSpot data fetchers ─────────────────────────────────────────────────────

def _props(item: dict, key: str):
    return (item.get('properties') or {}).get(key)


def fetch_bugs_and_features(company_id: str) -> tuple[list, list]:
    tickets = hs_search(
        'tickets',
        filters=[
            {'propertyName': 'associations.company', 'operator': 'EQ', 'value': company_id},
            {'propertyName': 'hs_pipeline', 'operator': 'EQ', 'value': TICKET_PIPELINE},
            {'propertyName': 'hs_pipeline_stage', 'operator': 'NOT_IN', 'values': [STAGE_DONE, STAGE_REJECTED]},
        ],
        properties=['subject', 'hs_ticket_priority', 'hs_pipeline_stage', 'createdate', 'ticket_type'],
    )
    bugs     = [t for t in tickets if (_props(t, 'ticket_type') or '').lower() == 'bug']
    features = [t for t in tickets if (_props(t, 'ticket_type') or '').lower() == 'feature']
    return bugs, features


def fetch_nps_tickets(company_id: str) -> list:
    return hs_search(
        'tickets',
        filters=[
            {'propertyName': 'associations.company', 'operator': 'EQ', 'value': company_id},
            {'propertyName': 'hs_pipeline_stage', 'operator': 'EQ', 'value': NPS_STAGE_ACTIVE},
        ],
        properties=['subject', 'response_score', 'createdate'],
        limit=100,
    )


def fetch_contacts(company_id: str) -> list:
    return hs_search(
        'contacts',
        filters=[
            {'propertyName': 'associations.company', 'operator': 'EQ', 'value': company_id},
        ],
        properties=['email', 'jobtitle', 'notes_last_contacted'],
        limit=200,
    )


def fetch_meetings(company_id: str) -> list:
    return hs_search(
        'meetings',
        filters=[
            {'propertyName': 'associations.company', 'operator': 'EQ', 'value': company_id},
            {'propertyName': 'hs_meeting_start_time', 'operator': 'GTE', 'value': LOOKBACK_MS},
        ],
        properties=['hs_meeting_title', 'hs_meeting_start_time'],
        limit=100,
    )


# ── HubSpot patch ─────────────────────────────────────────────────────────────

def patch_hubspot(rm: dict, entry: dict, company_id: str) -> None:
    # ── Bug tickets + feature requests ──
    try:
        bugs, features = fetch_bugs_and_features(company_id)

        high = [b for b in bugs if (_props(b, 'hs_ticket_priority') or '').upper() == 'HIGH']
        med  = [b for b in bugs if (_props(b, 'hs_ticket_priority') or '').upper() == 'MEDIUM']

        rm['openHighTickets'] = len(high)
        rm['openHighAges']    = [days_since(_props(b, 'createdate')) or 0 for b in high]
        rm['openHighWithEngStage'] = [
            _props(b, 'hs_pipeline_stage') == STAGE_WITH_ENG for b in high
        ]
        rm['openMediumGt30dCount'] = sum(
            1 for b in med if (days_since(_props(b, 'createdate')) or 0) > 30
        )

        entry['bugTicketUrls'] = [
            {
                'label':    _props(b, 'subject') or '',
                'priority': (_props(b, 'hs_ticket_priority') or '').upper(),
                'status':   STAGE_LABELS.get(_props(b, 'hs_pipeline_stage') or '', 'Open'),
                'url':      ticket_url(b['id']),
            }
            for b in bugs
        ]

        # Sync metricDetails.supportLoad display counts
        md_sl = (entry.get('metricDetails') or {}).get('supportLoad')
        if isinstance(md_sl, dict):
            md_sl['openHigh']   = len(high)
            md_sl['openMedium'] = len(med)

        entry['featureRequestUrls'] = [
            {
                'label':    _props(f, 'subject') or '',
                'priority': (_props(f, 'hs_ticket_priority') or '').upper(),
                'status':   STAGE_LABELS.get(_props(f, 'hs_pipeline_stage') or '', 'Open'),
                'url':      ticket_url(f['id']),
            }
            for f in features
        ]
    except Exception as e:
        print(f'    [WARN] tickets: {e}')

    # ── NPS tickets ──
    try:
        nps_tickets = fetch_nps_tickets(company_id)
        scores = []
        nps_urls = []
        for t in nps_tickets:
            raw = _props(t, 'response_score')
            score = None
            try:
                score = float(raw) if raw is not None else None
            except (ValueError, TypeError):
                pass
            subject = _props(t, 'subject') or ''
            url = ticket_url(t['id'])
            if score is not None:
                scores.append(score)
                nps_urls.append({'label': f'{subject} — {int(score)}/5', 'score': int(score), 'url': url})
            else:
                nps_urls.append({'label': subject, 'score': None, 'url': url})

        rm['npsResponseCount'] = len(nps_tickets)
        rm['npsAvg'] = round(sum(scores) / len(scores), 2) if scores else None
        entry['npsTicketUrls'] = nps_urls

        md_nps = (entry.get('metricDetails') or {}).get('npsSurvey')
        if isinstance(md_nps, dict):
            md_nps['avgScore']     = rm['npsAvg']
            md_nps['responseCount'] = rm['npsResponseCount']
    except Exception as e:
        print(f'    [WARN] NPS: {e}')

    # ── Contacts: multi-threading + exec engagement ──
    try:
        contacts = fetch_contacts(company_id)
        domains_all: set[str] = set()
        domains_recent: set[str] = set()
        exec_last_contacted: list[str] = []

        for c in contacts:
            email = _props(c, 'email') or ''
            domain = email.split('@')[-1].lower() if '@' in email else None
            if domain:
                domains_all.add(domain)
                last = _props(c, 'notes_last_contacted')
                d = days_since(last)
                if d is not None and d <= 90:
                    domains_recent.add(domain)

            jobtitle = _props(c, 'jobtitle') or ''
            if EXEC_RE.search(jobtitle):
                last = _props(c, 'notes_last_contacted')
                if last:
                    exec_last_contacted.append(last)

        rm['multiThreadingCoveragePct'] = (
            round(len(domains_recent) / len(domains_all) * 100, 1)
            if domains_all else None
        )
        rm['execContactsExist'] = len(exec_last_contacted) > 0 or any(
            EXEC_RE.search(_props(c, 'jobtitle') or '') for c in contacts
        )
        most_recent_exec = max(exec_last_contacted) if exec_last_contacted else None
        rm['execDaysSinceContact'] = days_since(most_recent_exec)
    except Exception as e:
        print(f'    [WARN] contacts: {e}')

    # ── Meetings: cadence + QBR classification ──
    try:
        meetings = fetch_meetings(company_id)
        thirty_ago = TODAY - timedelta(days=30)
        sixty_ago  = TODAY - timedelta(days=60)

        meeting_dates = []
        titles = []
        for m in meetings:
            ts = _props(m, 'hs_meeting_start_time')
            d  = days_since(ts)
            if d is not None:
                meeting_dates.append(TODAY - timedelta(days=d))
            title = (_props(m, 'hs_meeting_title') or '').lower()
            if title:
                titles.append(title)

        rm['meetingCount90d']      = len(meeting_dates)
        rm['daysSinceLastMeeting'] = min((TODAY - dt).days for dt in meeting_dates) if meeting_dates else None
        rm['meetingCountLast30d']  = sum(1 for d in meeting_dates if d >= thirty_ago)
        rm['meetingCountPrior60d'] = sum(1 for d in meeting_dates if sixty_ago <= d < thirty_ago)

        rm['hasQbrLast90d']      = any(kw in t for t in titles for kw in ['qbr', 'quarterly', 'business review'])
        rm['hasInPersonLast90d'] = any(kw in t for t in titles for kw in ['lunch', 'dinner', 'in-person', 'in person', 'site visit'])
        rm['hasTrainingLast90d'] = any(kw in t for t in titles for kw in ['training', 'onboarding', 'walkthrough'])
    except Exception as e:
        print(f'    [WARN] meetings: {e}')


# ── Metabase patch ────────────────────────────────────────────────────────────

def excl_sql(excl_ids: list[str], alias: str = 'f') -> str:
    """Returns SQL AND clause excluding facility IDs, or empty string."""
    if not excl_ids:
        return ''
    ids_list = ', '.join(f"'{i}'" for i in excl_ids)
    return f'AND {alias}.id NOT IN ({ids_list})'


def get_excluded_ids(account: dict) -> list[str]:
    excluded_names = set(account.get('excludedCommunities') or [])
    if not excluded_names:
        return []
    ids = []
    for comm in account.get('communities') or []:
        if comm.get('displayName') in excluded_names:
            ids.extend(comm.get('metabaseFacilityIds') or [])
    return ids


def patch_metabase(rm: dict, employer_id: str, account: dict) -> None:
    excl = get_excluded_ids(account)
    ef = excl_sql(excl)        # "AND f.id NOT IN (...)"
    ef_bare = excl_sql(excl, alias='id')  # for subqueries where alias is just 'id'

    # ── Query A — Publish rate + dark facilities ──────────────────────────────
    try:
        rows = mb_query(f"""
            SELECT f.id AS facility_id,
                   COUNT(*) FILTER (
                     WHERE s."startTime" >= DATE_TRUNC('month', NOW())
                       AND s."startTime" <  DATE_TRUNC('month', NOW()) + INTERVAL '1 month'
                   ) AS total,
                   SUM(CASE WHEN s."isDraft" = false
                     AND (s."isCancelled" = false OR s."isCancelled" IS NULL)
                     AND s."startTime" >= DATE_TRUNC('month', NOW())
                     AND s."startTime" <  DATE_TRUNC('month', NOW()) + INTERVAL '1 month'
                     THEN 1 ELSE 0 END) AS published
            FROM "Shift" s
            JOIN "Facility" f ON s."facilityId" = f.id
            WHERE f."employerId" = '{employer_id}'
              AND f."archivedAt" IS NULL
              AND s."startTime" >= DATE_TRUNC('month', NOW())
              AND s."startTime" <  DATE_TRUNC('month', NOW()) + INTERVAL '1 month'
              {ef}
            GROUP BY f.id
        """)
        if rows:
            tot_pub   = sum(r.get('published') or 0 for r in rows)
            tot_total = sum(r.get('total') or 0 for r in rows)
            rm['publishRate']    = round(tot_pub / tot_total, 4) if tot_total > 0 else None
            rm['darkFacilities'] = sum(1 for r in rows if (r.get('total') or 0) == 0)
        else:
            rm['publishRate']    = None
            rm['darkFacilities'] = 0
    except Exception as e:
        print(f'    [WARN] Metabase A (publish rate): {e}')

    # ── Query D — Fill rate ────────────────────────────────────────────────────
    try:
        rows = mb_query(f"""
            SELECT DATE_TRUNC('month', si."shiftStartTime") AS month,
                   ROUND(COUNT(*) FILTER (WHERE si."status" = 'ACCEPTED')::numeric
                         / NULLIF(COUNT(*), 0) * 100, 1) AS fill_rate_pct
            FROM "ShiftInvite" si
            JOIN "Facility" f ON si."facilityId" = f.id
            WHERE f."employerId" = '{employer_id}'
              AND si."shiftStartTime" >= NOW() - INTERVAL '3 months'
              {ef}
            GROUP BY DATE_TRUNC('month', si."shiftStartTime")
            ORDER BY month DESC
            LIMIT 3
        """)
        complete = [r for r in rows if is_complete_month(r.get('month'))]
        if complete:
            rm['fillRatePct'] = float(complete[0].get('fill_rate_pct') or 0)
            if len(complete) >= 2:
                rm['fillRateMomDeltaPct'] = round(
                    float(complete[0].get('fill_rate_pct') or 0)
                    - float(complete[1].get('fill_rate_pct') or 0), 1
                )
            else:
                rm['fillRateMomDeltaPct'] = None
        else:
            rm['fillRatePct'] = None
            rm['fillRateMomDeltaPct'] = None
    except Exception as e:
        print(f'    [WARN] Metabase D (fill rate): {e}')

    # ── Query E — Time savings ─────────────────────────────────────────────────
    try:
        rows = mb_query(f"""
            WITH employer_facilities AS (
              SELECT id FROM "Facility"
              WHERE "employerId" = '{employer_id}' AND "archivedAt" IS NULL
              {ef_bare}
            ),
            call_offs AS (
              SELECT COUNT(*) AS cnt FROM "ShiftCallOff" sco
              JOIN employer_facilities f ON sco."facilityId" = f.id
              WHERE sco."shiftStartTime" >= '2025-04-01' AND sco."shiftStartTime" < '2026-04-01'
            ),
            schedule_months AS (
              SELECT COALESCE(SUM(mc), 0) AS cnt FROM (
                SELECT COUNT(DISTINCT DATE_TRUNC('month', s."startTime")) AS mc
                FROM "Shift" s JOIN employer_facilities f ON s."facilityId" = f.id
                WHERE s."isDraft" = false AND s."startTime" >= '2025-04-01' AND s."startTime" < '2026-04-01'
                GROUP BY s."facilityId"
              ) sub
            ),
            daily_cards AS (
              SELECT COALESCE(SUM(dc), 0) AS cnt FROM (
                SELECT COUNT(DISTINCT DATE_TRUNC('day', fc."date")) AS dc
                FROM "FacilityCensus" fc JOIN employer_facilities f ON fc."facilityId" = f.id
                WHERE fc."date" >= '2025-04-01' AND fc."date" < '2026-04-01'
                GROUP BY fc."facilityId"
              ) sub
            )
            SELECT ROUND((co.cnt * 15.0 + sm.cnt * 600.0 + dc.cnt * 30.0) / 60, 1) AS hours_saved
            FROM call_offs co, schedule_months sm, daily_cards dc
        """)
        rm['hoursSaved'] = float(rows[0]['hours_saved']) if rows else None
    except Exception as e:
        print(f'    [WARN] Metabase E (time savings): {e}')

    # ── Query F — Nursing login + mobile ──────────────────────────────────────
    try:
        rows = mb_query(f"""
            WITH nursing_workers AS (
              SELECT DISTINCT si."userId"
              FROM "ShiftInvite" si
              WHERE si."employerId" = '{employer_id}'
                AND si."departmentSlug" ILIKE 'NURSING%'
                AND si."invitedAt" >= NOW() - INTERVAL '90 days'
                AND si."shiftIsTest" = false
            )
            SELECT
              COUNT(DISTINCT nw."userId") AS nursing_workers,
              ROUND(COUNT(DISTINCT u.id) FILTER (WHERE u."lastActivityAt" >= NOW() - INTERVAL '30 days')::numeric
                    / NULLIF(COUNT(DISTINCT nw."userId"), 0) * 100, 1) AS active_30d_pct,
              ROUND(COUNT(DISTINCT u.id) FILTER (WHERE u."hasMobileApp" = true)::numeric
                    / NULLIF(COUNT(DISTINCT nw."userId"), 0) * 100, 1) AS mobile_pct
            FROM nursing_workers nw
            JOIN "User" u ON nw."userId" = u.id
            WHERE u."isTest" = false
        """)
        if rows and (rows[0].get('nursing_workers') or 0) > 0:
            rm['nursingActive30dPct'] = float(rows[0].get('active_30d_pct') or 0)
            rm['mobileAdoptionPct']   = float(rows[0].get('mobile_pct') or 0)
        else:
            rm['nursingActive30dPct'] = None
            rm['mobileAdoptionPct']   = None
    except Exception as e:
        print(f'    [WARN] Metabase F (nursing login): {e}')

    # ── Query G — Scheduler WEB decay ─────────────────────────────────────────
    try:
        rows = mb_query(f"""
            SELECT DATE_TRUNC('month', uae.date) AS month,
                   COUNT(DISTINCT uae."userId") AS web_active_users
            FROM "UserActivityEvent" uae
            JOIN "User" u ON uae."userId" = u.id
            WHERE u."employerId" = '{employer_id}'
              AND uae.channel = 'WEB'
              AND uae.date >= DATE_TRUNC('month', NOW()) - INTERVAL '2 months'
              AND uae.date <  DATE_TRUNC('month', NOW())
              AND u."isTest" = false AND u."isSuperAdmin" = false
            GROUP BY DATE_TRUNC('month', uae.date)
            ORDER BY month
        """)
        if len(rows) >= 2:
            prev = float(rows[0].get('web_active_users') or 1)
            curr = float(rows[1].get('web_active_users') or 0)
            rm['schedulerMomChangePct'] = round((curr - prev) / prev * 100, 1) if prev else None
        else:
            rm['schedulerMomChangePct'] = None
    except Exception as e:
        print(f'    [WARN] Metabase G (scheduler decay): {e}')

    # ── Query H — Clock variance ───────────────────────────────────────────────
    try:
        rows = mb_query(f"""
            SELECT DATE_TRUNC('month', ts."time") AS month,
                   ROUND(COUNT(*) FILTER (WHERE ABS(ts."clockedInVarianceMinutes") > 15)::numeric
                         / NULLIF(COUNT(*), 0) * 100, 1) AS pct_variance_gt15
            FROM reporting."TimesheetShift" ts
            WHERE ts."employerId" = '{employer_id}'
              AND ts."departmentSlug" ILIKE 'NURSING%'
              AND ts."isStale" = false
              AND ts."time" >= DATE_TRUNC('month', NOW()) - INTERVAL '3 months'
              AND ts."time" <  DATE_TRUNC('month', NOW())
            GROUP BY DATE_TRUNC('month', ts."time")
            ORDER BY month DESC
        """)
        complete = [r for r in rows if is_complete_month(r.get('month'))]
        if complete:
            rm['clockVariancePctGt15'] = float(complete[0].get('pct_variance_gt15') or 0)
            if len(complete) >= 2:
                rm['clockVarianceMomChangePts'] = round(
                    float(complete[0].get('pct_variance_gt15') or 0)
                    - float(complete[1].get('pct_variance_gt15') or 0), 1
                )
            else:
                rm['clockVarianceMomChangePts'] = None
        else:
            rm['clockVariancePctGt15']    = None
            rm['clockVarianceMomChangePts'] = None
    except Exception as e:
        print(f'    [WARN] Metabase H (clock variance): {e}')

    # ── Query I — Meal break ───────────────────────────────────────────────────
    try:
        rows = mb_query(f"""
            SELECT DATE_TRUNC('month', mb."date") AS month,
                   ROUND(COUNT(*) FILTER (WHERE mb."tookBreak" = false)::numeric
                         / NULLIF(COUNT(*), 0) * 100, 1) AS missed_break_pct
            FROM "MealBreakRemediation" mb
            WHERE mb."employerId" = '{employer_id}'
              AND mb."isStale" = false
              AND mb."date" >= DATE_TRUNC('month', NOW()) - INTERVAL '3 months'
              AND mb."date" <  DATE_TRUNC('month', NOW())
            GROUP BY DATE_TRUNC('month', mb."date")
            ORDER BY month DESC
        """)
        complete = [r for r in rows if is_complete_month(r.get('month'))]
        if complete:
            rm['mealBreakMissedPct'] = float(complete[0].get('missed_break_pct') or 0)
            if len(complete) >= 2:
                rm['mealBreakMomChangePts'] = round(
                    float(complete[0].get('missed_break_pct') or 0)
                    - float(complete[1].get('missed_break_pct') or 0), 1
                )
            else:
                rm['mealBreakMomChangePts'] = None
        else:
            # T&A not enabled for this account — null is correct
            rm['mealBreakMissedPct']   = None
            rm['mealBreakMomChangePts'] = None
    except Exception as e:
        print(f'    [WARN] Metabase I (meal break): {e}')

    # ── Query J — OT trend ────────────────────────────────────────────────────
    try:
        rows = mb_query(f"""
            SELECT DATE_TRUNC('month', oa."time") AS month,
                   SUM(oa."totalOvertimeMinutes") AS total_ot_minutes
            FROM reporting."OvertimeActual" oa
            WHERE oa."employerId" = '{employer_id}'
              AND oa."isStale" = false
              AND oa."time" >= NOW() - INTERVAL '6 months'
            GROUP BY DATE_TRUNC('month', oa."time")
            ORDER BY month DESC
        """)
        complete = [r for r in rows if is_complete_month(r.get('month'))]
        if len(complete) >= 2:
            prev = float(complete[1].get('total_ot_minutes') or 1)
            curr = float(complete[0].get('total_ot_minutes') or 0)
            rm['otMomChangePct'] = round((curr - prev) / prev * 100, 1) if prev else None
        else:
            rm['otMomChangePct'] = None
    except Exception as e:
        print(f'    [WARN] Metabase J (OT trend): {e}')


# ── Slack patch ───────────────────────────────────────────────────────────────

def patch_slack(rm: dict, company_name: str) -> None:
    try:
        r = requests.get(
            'https://slack.com/api/search.messages',
            headers={'Authorization': f'Bearer {SLACK_TOKEN}'},
            params={'query': f'{company_name} in:#at-risk', 'count': 20},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        if data.get('ok'):
            rm['atRiskMentions'] = data.get('messages', {}).get('total', 0)
        else:
            print(f'    [WARN] Slack: {data.get("error")}')
    except Exception as e:
        print(f'    [WARN] Slack: {e}')


# ── Intercom patch ────────────────────────────────────────────────────────────

def patch_intercom(rm: dict, company_name: str) -> None:
    try:
        headers = {
            'Authorization': f'Bearer {INTERCOM_TOKEN}',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Intercom-Version': '2.10',
        }
        # Find company in Intercom
        r = requests.get(
            'https://api.intercom.io/companies',
            headers=headers,
            params={'name': company_name},
            timeout=15,
        )
        r.raise_for_status()
        companies = r.json().get('companies') or []
        if not companies:
            rm['intercomOpenConversations'] = None  # company not in Intercom
            return

        intercom_company_id = companies[0]['id']

        # Count open conversations for this company
        r2 = requests.post(
            'https://api.intercom.io/conversations/search',
            headers=headers,
            json={
                'query': {
                    'operator': 'AND',
                    'value': [
                        {'field': 'open', 'operator': '=', 'value': True},
                        {'field': 'company_id', 'operator': '=', 'value': intercom_company_id},
                    ],
                },
                'pagination': {'per_page': 1},
            },
            timeout=15,
        )
        r2.raise_for_status()
        rm['intercomOpenConversations'] = r2.json().get('total_count', 0)
    except Exception as e:
        print(f'    [WARN] Intercom: {e}')


# ── Account refresh ───────────────────────────────────────────────────────────

def refresh_account(name: str, account: dict) -> None:
    slug = name.lower().replace(' ', '-').replace('/', '-').replace(',', '')
    path = Path(f'data/{slug}.json')

    if not path.exists():
        print(f'  → data/{slug}.json not found, skipping')
        return

    with path.open() as f:
        data = json.load(f)

    entries = data.get('entries') or []
    if not entries:
        print(f'  → No entries in {slug}.json, skipping')
        return

    entry = entries[-1]
    if 'rawMetrics' not in entry:
        print(f'  → Latest entry has no rawMetrics (old schema), skipping')
        return

    rm = entry['rawMetrics']
    company_id  = account.get('hubspotCompanyId', '')
    employer_id = account.get('metabaseEmployerId', '')

    if HUBSPOT_TOKEN and company_id:
        print('  HubSpot...')
        patch_hubspot(rm, entry, company_id)
    else:
        print('  → Skipping HubSpot (no token or company ID)')

    if METABASE_TOKEN and employer_id:
        print('  Metabase...')
        patch_metabase(rm, employer_id, account)
    else:
        print('  → Skipping Metabase (no token or employer ID)')

    if SLACK_TOKEN:
        print('  Slack...')
        patch_slack(rm, name)
    else:
        print('  → Skipping Slack (no token)')

    if INTERCOM_TOKEN:
        print('  Intercom...')
        patch_intercom(rm, name)
    else:
        print('  → Skipping Intercom (no token)')

    entry['lastRefreshed'] = TODAY.isoformat()

    if DRY_RUN:
        print(f'  [DRY_RUN] Would write data/{slug}.json')
        print(f'    openHighTickets={rm.get("openHighTickets")}  npsAvg={rm.get("npsAvg")}  '
              f'publishRate={rm.get("publishRate")}  meetingCount90d={rm.get("meetingCount90d")}')
    else:
        with path.open('w') as f:
            json.dump(data, f, indent=2)
        print(f'  ✓ Wrote data/{slug}.json')


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    registry_path = Path('community_registry.json')
    if not registry_path.exists():
        print('ERROR: community_registry.json not found. Run from the repo root.')
        sys.exit(1)

    with registry_path.open() as f:
        registry = json.load(f)

    accounts = registry.get('accounts') or {}
    if not accounts:
        print('ERROR: No accounts found in registry.')
        sys.exit(1)

    print(f'Refreshing {len(accounts)} accounts  (DRY_RUN={DRY_RUN})')
    print(f'Today: {TODAY}  Lookback: {LOOKBACK}')
    if not HUBSPOT_TOKEN:  print('WARNING: HUBSPOT_API_KEY not set')
    if not METABASE_TOKEN: print('WARNING: METABASE_TOKEN not set')
    if not SLACK_TOKEN:    print('WARNING: SLACK_BOT_TOKEN not set')
    if not INTERCOM_TOKEN: print('WARNING: INTERCOM_TOKEN not set')

    for name, account in accounts.items():
        print(f'\n── {name} ──')
        try:
            refresh_account(name, account)
        except Exception:
            print(f'  [ERROR] Unhandled exception:')
            traceback.print_exc()

    print('\nRefresh complete.')


if __name__ == '__main__':
    main()
