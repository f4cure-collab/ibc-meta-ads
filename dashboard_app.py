"""
IBC Eventos - Dashboard de Performance Meta Ads
Servidor Flask na porta 5001 (separado do app.py principal).
"""

import os
import sys
import json
import math
import time
import uuid
import threading
import functools
import requests
from datetime import datetime, timedelta, timezone
from flask import Flask, render_template, request, jsonify, session, redirect, url_for, g
from werkzeug.middleware.proxy_fix import ProxyFix
from dotenv import load_dotenv
from cache_manager import get_cached, set_cached, clear_cache, cache_stats, start_scheduler, clear_expired, try_acquire_scheduler_lock, refresh_scheduler_lock, should_refresh, log_api_usage, clear_old_usage_logs, get_usage_stats, get_api_calls_for_user, pin_cache_key, set_atom, get_atom, get_atoms_for_range, list_atoms_metadata, count_atoms_by_scope
from event_grouper import group_campaigns_by_event, _parse_campaign_name as _parse_name

# Carrega .env sempre do diretorio do proprio arquivo (independente do cwd do gunicorn)
_ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(_ENV_PATH)

app = Flask(__name__, static_folder=os.path.dirname(__file__), static_url_path='/static')
# Usa FLASK_SECRET_KEY do .env (estavel entre restarts e workers do gunicorn).
# Sem isso, cada worker gera uma chave diferente e a sessao quebra a cada request.
app.secret_key = os.getenv("FLASK_SECRET_KEY") or os.urandom(24)
app.permanent_session_lifetime = timedelta(hours=12)

# Atras do NGINX em HTTPS: respeita X-Forwarded-Proto/For/Host
# para que cookies Secure e url_for funcionem com o esquema correto.
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

# Feature isolada: pagina /concorrentes que sincroniza com FB Ads Library
# via Apify. Toda a logica mora em competitors.py — pra remover, apague
# o arquivo e essas 2 linhas abaixo.
from competitors import competitors_bp
app.register_blueprint(competitors_bp)


# ── API usage logging (diagnostico) ────────────────────────────────────
# Registra cada request nos endpoints /api/dashboard/* pra saber quantas
# chamadas Meta cada um gerou. Dados em cache_manager api_usage_log, TTL 7 dias.
@app.before_request
def _usage_before():
    try:
        from flask import request as _req
        # So track /api/dashboard/*
        if not _req.path.startswith("/api/dashboard/"):
            return
        g._usage_start = time.time()
        g.meta_calls = 0
        g._usage_track = True
    except Exception:
        pass


@app.after_request
def _usage_after(resp):
    try:
        if not getattr(g, "_usage_track", False):
            return resp
        duration = int((time.time() - g._usage_start) * 1000)
        meta_calls = getattr(g, "meta_calls", 0) or 0
        cache_hit = (meta_calls == 0 and resp.status_code == 200)
        from flask import request as _req
        endpoint = _req.path
        camp_type = _req.args.get("camp_type") or getattr(g, "camp_type", None)
        # Identificacao do quem triggou:
        # - Header X-Internal-Scheduler: vem do scheduler/warmup (chamada automatica)
        # - Senao: usuario logado pela session
        if _req.headers.get("X-Internal-Scheduler"):
            user = "auto:" + _req.headers.get("X-Internal-Scheduler", "scheduler")
        else:
            try:
                user = session.get("username") or None
            except Exception:
                user = None
        # Pega pior pct atual (post-request) pra ver impacto
        try:
            pct, _ = _worst_usage_pct()
        except Exception:
            pct = None
        log_api_usage(
            endpoint=endpoint,
            camp_type=camp_type,
            meta_calls=meta_calls,
            cache_hit=cache_hit,
            duration_ms=duration,
            user=user,
            worst_buc_pct=pct,
        )
    except Exception as e:
        print(f"[USAGE LOG HOOK] {e}")
    return resp

API_VERSION = "v21.0"
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"
TOKEN = os.getenv("META_ACCESS_TOKEN", "")
ACCOUNT_ID = os.getenv("META_AD_ACCOUNT_ID", "")

# Contas extras (alem da principal do .env) sao gerenciadas via /admin e persistidas
# em ad_accounts.json local ao servidor — nao requer mexer no .env para adicionar.
# Formato: [{"id": "act_123", "label": "Conta Comercial", "camp_types": ["comercial"]}]
AD_ACCOUNTS_FILE = os.path.join(os.path.dirname(__file__), "ad_accounts.json")


CAMPAIGN_OVERRIDES_FILE = os.path.join(os.path.dirname(__file__), "campaign_overrides.json")


def _load_overrides():
    """Le overrides manuais: {campaign_id: {camp_type, event_name, ...}}"""
    try:
        if not os.path.exists(CAMPAIGN_OVERRIDES_FILE):
            return {}
        with open(CAMPAIGN_OVERRIDES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        print(f"[OVERRIDES] Erro lendo: {e}")
        return {}


def _save_overrides(overrides):
    try:
        with open(CAMPAIGN_OVERRIDES_FILE, "w", encoding="utf-8") as f:
            json.dump(overrides, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[OVERRIDES] Erro salvando: {e}")


def _load_ad_accounts():
    """Le contas extras do JSON local. Retorna [] se arquivo nao existe ou erro."""
    try:
        if not os.path.exists(AD_ACCOUNTS_FILE):
            return []
        with open(AD_ACCOUNTS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        return []
    except Exception as e:
        print(f"[AD_ACCOUNTS] Erro lendo {AD_ACCOUNTS_FILE}: {e}")
        return []


def _save_ad_accounts(accounts):
    try:
        with open(AD_ACCOUNTS_FILE, "w", encoding="utf-8") as f:
            json.dump(accounts, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[AD_ACCOUNTS] Erro salvando: {e}")


def _get_accounts_for_type(camp_type):
    """Retorna lista de contas Meta a consultar para o tipo de campanha.
    Conta principal (do .env) sempre vem primeiro. Contas extras do JSON sao
    incluidas quando tem o camp_type mapeado. Dedup preservando ordem."""
    seen = []
    if ACCOUNT_ID:
        seen.append(ACCOUNT_ID)
    for acc in _load_ad_accounts():
        acc_id = (acc.get("id") or "").strip()
        types = acc.get("camp_types") or []
        if acc_id and camp_type in types and acc_id not in seen:
            seen.append(acc_id)
    return seen


# Cache em memoria da lista de campanhas por conta. TTL curto (5min) evita
# que 3 endpoints chamados em sequencia (/campaigns, /daily-summary,
# /multi-insights) refacam a mesma busca de metadados na Meta. Dados de
# status/budget mudam lentamente, entao 5min eh seguro.
_campaigns_memcache = {}  # {(acc_id, effective_status, fields): (ts, [rows])}
_campaigns_memcache_lock = threading.Lock()
_CAMPAIGNS_MEMCACHE_TTL = 300  # 5 minutos


def _fetch_account_campaigns(acc_id, fields, effective_status):
    """Busca lista de campanhas de uma conta com cache em memoria de 5min.
    Dedupe chamadas de /campaigns quando varios endpoints precisam da mesma lista."""
    key = (acc_id, effective_status, fields)
    now = time.time()
    with _campaigns_memcache_lock:
        entry = _campaigns_memcache.get(key)
        if entry and (now - entry[0]) < _CAMPAIGNS_MEMCACHE_TTL:
            return [dict(r) for r in entry[1]]
    rows = meta_get_all_pages(
        f"{acc_id}/campaigns",
        {"fields": fields, "effective_status": effective_status}
    )
    for r in rows:
        r["_account_id"] = acc_id
    with _campaigns_memcache_lock:
        _campaigns_memcache[key] = (now, rows)
    return [dict(r) for r in rows]


def _fetch_type_campaigns(camp_type, fields, effective_status):
    """Busca campanhas de TODAS as contas configuradas para o camp_type,
    aplica o filtro por tipo, e retorna a lista com _account_id tagueado.
    Usa _fetch_account_campaigns com cache em memoria (5min) pra dedupe."""
    all_camps = []
    for acc in _get_accounts_for_type(camp_type):
        try:
            rows = _fetch_account_campaigns(acc, fields, effective_status)
            all_camps.extend(rows)
        except Exception as e:
            print(f"[MULTI-ACCT] Falha campanhas {acc}: {e}")
    return _filter_campaigns_by_type(all_camps, camp_type)


def _fetch_account_raw_v1(acc_id, camp_status, date_from, date_to):
    """Cache BRUTO por conta — lista de campanhas + insights SEM filtro por tipo.
    Key: campaigns_raw_v1_{acc}_{status}_{range}.

    Motivo da separacao: mudancas na regra de classificacao por nome nao
    devem invalidar Meta caches. O cache filtrado por tipo (campaigns_v*_{ct})
    eh derivado deste em runtime. Bump de v3->v4 classificacao = re-filtra
    o bruto, zero chamada Meta.

    Retorna: {'campaigns': [lista com id/name/status/objective/etc],
              'insights_by_id': {cid: {raw insight dict}}}"""
    cache_key = f"campaigns_raw_v1_{acc_id}_{camp_status}_{date_from}_{date_to}"
    cached = get_cached(cache_key)
    if cached is not None:
        return cached

    # Lista de campanhas (meta_get_all_pages — cacheada internamente 5min)
    try:
        camps = _fetch_account_campaigns(
            acc_id,
            "id,name,status,objective,daily_budget,lifetime_budget,start_time,created_time",
            _camp_status_filter(camp_status),
        )
    except Exception as e:
        print(f"[RAW] Falha campanhas {acc_id}: {e}")
        camps = []

    # Insights level=campaign pra TODAS as campanhas da conta no periodo.
    # Filter impressions > 0 evita retornar campanhas sem atividade.
    insights_by_id = {}
    try:
        rows = meta_get_all_pages(f"{acc_id}/insights", {
            "fields": INSIGHT_FIELDS_CAMPAIGN,
            "time_range": json.dumps({"since": date_from, "until": date_to}),
            "level": "campaign",
            "filtering": json.dumps([{"field": "impressions", "operator": "GREATER_THAN", "value": 0}]),
            "limit": 500,
        })
        for r in rows:
            cid = r.get("campaign_id", "")
            if cid:
                insights_by_id[cid] = r
    except Exception as e:
        print(f"[RAW] Falha insights {acc_id}: {e}")

    payload = {"campaigns": camps, "insights_by_id": insights_by_id}
    set_cached(cache_key, payload, ttl_hours=_cache_ttl_for_range(date_from, date_to))
    # Se o range eh inteiramente mes completo, pina 180d
    _segs = _split_range_by_month_segments(date_from, date_to)
    if _segs and all(_is_completed_month(sf, st) for sf, st in _segs):
        pin_cache_key(cache_key, ttl_hours=_MONTHLY_TTL_HOURS)
    return payload


# ── ATOM SYSTEM (cache diario imutavel) ────────────────────────────────
# Atom = unidade indivisivel: 1 dia, 1 conta. Range = soma de atoms.
# Atoms maduros (>D+8) sao imutaveis e nunca refetcheados.
# Sistema com dual-read: USE_ATOMS=True usa atoms+aggregar, fallback no
# range cache antigo se atoms ainda nao cobrem o range completo.

# Flag global de feature. Persistida em arquivo pra sobreviver reinicio
# de container / deploy. Inicia em False na primeira vez (sem arquivo);
# depois disso, le do arquivo. Painel /admin tem botao pra ativar.
_USE_ATOMS_FILE = os.path.join(os.path.dirname(__file__), "use_atoms_flag.json")


def _load_use_atoms_from_file():
    """Le o estado persistido. Retorna False se arquivo nao existe ou
    falha de leitura — fail-closed (atoms so ativam se explicit ON)."""
    try:
        if not os.path.exists(_USE_ATOMS_FILE):
            return False
        with open(_USE_ATOMS_FILE, "r", encoding="utf-8") as f:
            return bool((json.load(f) or {}).get("use_atoms", False))
    except Exception:
        return False


def _save_use_atoms_to_file(value):
    try:
        with open(_USE_ATOMS_FILE, "w", encoding="utf-8") as f:
            json.dump({"use_atoms": bool(value), "saved_at": _now_br().isoformat(timespec="seconds")}, f)
    except Exception as e:
        print(f"[ATOMS] Erro salvar flag: {e}")


USE_ATOMS = _load_use_atoms_from_file()

# Lock pra mudancas concorrentes na flag
_use_atoms_lock = threading.Lock()

# Reversao automatica: se N divergencias graves em janela curta, desativa
_atom_recent_divergences = []  # lista de timestamps
_DIVERGENCE_WINDOW_SECONDS = 3600  # 1h
_DIVERGENCE_THRESHOLD = 3  # 3 divergencias em 1h = desativa


def _set_use_atoms(value, reason=""):
    """Atomically set USE_ATOMS flag. Loga a mudanca e persiste em arquivo
    pra sobreviver reinicio de container."""
    global USE_ATOMS
    with _use_atoms_lock:
        old = USE_ATOMS
        USE_ATOMS = bool(value)
        if old != USE_ATOMS:
            print(f"[ATOMS] USE_ATOMS {old} -> {USE_ATOMS} ({reason})")
            _log_atom_event("flag_change", {"from": old, "to": USE_ATOMS, "reason": reason})
        _save_use_atoms_to_file(USE_ATOMS)


def _sync_use_atoms_from_file():
    """Sincroniza USE_ATOMS in-memory com o arquivo. Chamado nos endpoints
    pra que workers gunicorn paralelos vejam mudancas feitas em outro
    worker (memoria nao e compartilhada entre workers)."""
    global USE_ATOMS
    try:
        new_value = _load_use_atoms_from_file()
        if new_value != USE_ATOMS:
            USE_ATOMS = new_value
    except Exception:
        pass


def _log_atom_event(event_type, data):
    """Adiciona evento ao log de migracao (mostrado no painel /admin).
    event_type: 'fetch_ok' | 'fetch_err' | 'validate_ok' | 'validate_diverge' |
                'flag_change' | 'auto_revert' | 'backfill_progress'
    data: dict com info especifica do evento."""
    try:
        log_path = os.path.join(os.path.dirname(__file__), "atom_migration_log.json")
        log = []
        if os.path.exists(log_path):
            try:
                with open(log_path, "r", encoding="utf-8") as f:
                    log = json.load(f)
            except Exception:
                log = []
        entry = {
            "ts": _now_br().isoformat(timespec="seconds"),
            "event": event_type,
            "data": data,
        }
        log.append(entry)
        # Mantem apenas os ultimos 500 eventos
        if len(log) > 500:
            log = log[-500:]
        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(log, f, ensure_ascii=False)
    except Exception as e:
        print(f"[ATOMS] Log erro: {e}")


def _fetch_atom_acc_for_day(acc_id, date_str, force=False):
    """Fetcha 1 atom: dados de UMA conta em UM dia. 1 chamada Meta.
    Retorna {campaigns, insights_by_id, fetched_at}.
    Cacheado via set_atom (TTL adaptativo).

    force=True bypassa o cache e refaz fetch (usado pra revalidacao D+2/D+8).
    Sem force, retorna cache se existir."""
    if not force:
        cached = get_atom('acc', acc_id, date_str)
        if cached is not None:
            return cached

    t0 = time.time()
    try:
        camps_list = _fetch_account_campaigns(
            acc_id,
            "id,name,status,objective,daily_budget,lifetime_budget,start_time,created_time",
            '["ACTIVE","PAUSED","ARCHIVED"]',
        )
    except Exception as e:
        print(f"[ATOM] Falha campanhas {acc_id} {date_str}: {e}")
        camps_list = []

    insights_by_id = {}
    try:
        rows = meta_get_all_pages(f"{acc_id}/insights", {
            "fields": INSIGHT_FIELDS_CAMPAIGN,
            "time_range": json.dumps({"since": date_str, "until": date_str}),
            "level": "campaign",
            "filtering": json.dumps([{"field": "impressions", "operator": "GREATER_THAN", "value": 0}]),
            "limit": 500,
        })
        for r in rows:
            cid = r.get("campaign_id", "")
            if cid:
                insights_by_id[cid] = r
    except Exception as e:
        print(f"[ATOM] Falha insights {acc_id} {date_str}: {e}")
        _log_atom_event("fetch_err", {"acc": acc_id, "date": date_str, "error": str(e)[:200]})
        return None

    payload = {
        "campaigns": camps_list,
        "insights_by_id": insights_by_id,
        "fetched_at": _now_br().isoformat(timespec="seconds"),
    }
    set_atom('acc', acc_id, date_str, payload)
    duration_ms = int((time.time() - t0) * 1000)
    _log_atom_event("fetch_ok", {
        "acc": acc_id, "date": date_str,
        "campaigns": len(camps_list),
        "insights": len(insights_by_id),
        "ms": duration_ms,
    })
    return payload


def _accumulate_insight_row(combined, new_row):
    """Soma campos numericos + merge de listas (actions, action_values, etc)
    de UM insight row novo no acumulador combined.
    combined eh modificado in-place. new_row eh o dict raw da Meta."""
    if not new_row:
        return
    # Campos numericos diretos
    for k in ("spend", "impressions", "clicks", "reach", "inline_link_clicks",
              "frequency", "cpc", "cpm", "ctr"):
        v = new_row.get(k)
        if v is None:
            continue
        try:
            v = float(v)
        except Exception:
            continue
        if k in ("frequency", "cpc", "cpm", "ctr"):
            # Esses sao DERIVADOS — guardamos a ultima vista pra recalcular depois
            combined.setdefault("_derived_seen", {})[k] = v
        else:
            combined[k] = combined.get(k, 0) + v

    # Listas de acoes — merge por action_type
    for list_field in ("actions", "action_values", "cost_per_action_type",
                        "purchase_roas", "video_thruplay_watched_actions",
                        "video_play_actions", "video_p25_watched_actions",
                        "video_p50_watched_actions", "video_p75_watched_actions",
                        "video_p95_watched_actions", "video_p100_watched_actions"):
        new_list = new_row.get(list_field) or []
        if not new_list:
            continue
        bucket = combined.setdefault("_list_" + list_field, {})  # action_type -> sum
        for item in new_list:
            at = item.get("action_type")
            if not at:
                continue
            try:
                v = float(item.get("value", 0) or 0)
            except Exception:
                v = 0
            if list_field in ("cost_per_action_type", "purchase_roas"):
                # Derivados — recalculam depois, marcamos com flag pra nao usar a soma
                continue
            bucket[at] = bucket.get(at, 0) + v


def _finalize_combined_insight(combined):
    """Converte o acumulador (`_list_*` dicts) de volta pra estrutura Meta-like
    com campos `actions`, `action_values`, etc. Recalcula derivados como CPA
    a partir dos somatorios."""
    out = {}
    for k, v in combined.items():
        if k.startswith("_list_") or k.startswith("_derived"):
            continue
        out[k] = v
    # Reconstroi listas
    for prefix in ("actions", "action_values", "video_thruplay_watched_actions",
                    "video_play_actions", "video_p25_watched_actions",
                    "video_p50_watched_actions", "video_p75_watched_actions",
                    "video_p95_watched_actions", "video_p100_watched_actions"):
        bucket = combined.get("_list_" + prefix) or {}
        if bucket:
            out[prefix] = [{"action_type": at, "value": str(v)} for at, v in bucket.items()]
    # Recalcula cost_per_action_type (CPA por tipo) a partir do acumulado
    spend = float(out.get("spend", 0) or 0)
    actions_bucket = combined.get("_list_actions") or {}
    if spend > 0 and actions_bucket:
        cpa_list = []
        for at, count in actions_bucket.items():
            if count > 0:
                cpa_list.append({"action_type": at, "value": str(round(spend / count, 4))})
        if cpa_list:
            out["cost_per_action_type"] = cpa_list
    # Recalcula purchase_roas: action_values purchase / spend
    av_bucket = combined.get("_list_action_values") or {}
    if spend > 0 and av_bucket:
        roas_list = []
        for at, value in av_bucket.items():
            roas_list.append({"action_type": at, "value": str(round(value / spend, 4))})
        if roas_list:
            out["purchase_roas"] = roas_list
    return out


def _atoms_can_serve_range(acc_ids, date_from, date_to):
    """Verifica se TODOS os atoms necessarios pra esse range estao disponiveis
    (todas as contas × todos os dias). Usado pelo dual-read pra decidir se
    pode usar atoms ou cai no fallback."""
    for acc in acc_ids:
        atoms, missing = get_atoms_for_range('acc', acc, date_from, date_to)
        if missing:
            return False
    return True


def _fetch_atoms_for_range(acc_ids, date_from, date_to):
    """Le atoms de varias contas pra um range. Retorna {acc_id: [atoms_list]}.
    Atoms faltantes sao IGNORADOS (chamador valida via _atoms_can_serve_range
    antes de usar)."""
    result = {}
    for acc in acc_ids:
        atoms, missing = get_atoms_for_range('acc', acc, date_from, date_to)
        result[acc] = atoms
    return result


def _atom_status_allowed(camp_status):
    """Retorna o set de status permitidos pro filtro pedido pelo usuario.
    Mesma logica de _camp_status_filter mas pra filtragem in-memory dos
    atoms (nao da pra passar effective_status pra atom ja cacheado)."""
    if camp_status == "paused":
        return {"PAUSED", "ARCHIVED"}
    if camp_status == "all":
        return {"ACTIVE", "PAUSED", "ARCHIVED"}
    return {"ACTIVE"}  # default


def _build_pseudo_raw_per_account_from_atoms(acc_id, date_from, date_to, camp_type=None, camp_status="all"):
    """Constroi a estrutura {campaigns, atom_parsed_metrics_by_id} a partir
    dos atoms. ABORDAGEM CORRETA: parse_insights de CADA DIA separado, depois
    soma os valores parsed (nao reconstroi a lista de actions).

    Isso garante que sum-of-days == range-query, sem perder dados em conflitos
    de priorizacao de action_types entre dias.

    camp_type: passa pra parse_insights aplicar a logica especifica do tipo
    (VENDAS prioriza purchases, METEORICOS prioriza leads, etc).
    camp_status: filtra campanhas pelo status atual ('active', 'paused', 'all').
    Status e tirado do atom mais recente (D-1 normalmente) pra refletir o
    estado atual da campanha.

    Retorna None se atoms incompletos pra esse range."""
    atoms_list, missing = get_atoms_for_range('acc', acc_id, date_from, date_to)
    if missing:
        return None

    all_campaigns = {}
    accumulated_per_cid = {}  # cid -> {metric_name: sum_value}

    # Atom mais recente vence pra metadata (status, name) — refletir estado
    # atual da campanha. Atoms vem ordenados por data ascendente em
    # get_atoms_for_range, entao processamos do mais novo pro mais velho.
    for atom in reversed(atoms_list):
        payload = atom['payload']
        for c in payload.get('campaigns') or []:
            cid = c.get('id')
            if cid and cid not in all_campaigns:
                all_campaigns[cid] = dict(c)
        for cid, raw_ins in (payload.get('insights_by_id') or {}).items():
            day_metrics = parse_insights(raw_ins, camp_type=camp_type)
            if cid not in accumulated_per_cid:
                accumulated_per_cid[cid] = {}
            for k, v in day_metrics.items():
                if isinstance(v, (int, float)):
                    accumulated_per_cid[cid][k] = accumulated_per_cid[cid].get(k, 0) + v

    # Filtra por status atual da campanha (post-aggregation)
    allowed_status = _atom_status_allowed(camp_status)
    filtered_campaigns = {}
    for cid, c in all_campaigns.items():
        if c.get("status", "").upper() in allowed_status:
            filtered_campaigns[cid] = c
    all_campaigns = filtered_campaigns
    # Remove insights de campanhas que nao passaram no filtro
    accumulated_per_cid = {cid: m for cid, m in accumulated_per_cid.items() if cid in all_campaigns}

    # Recalcula derivadas a partir dos somatorios das bases (nao some derivadas!)
    for cid, m in accumulated_per_cid.items():
        spend = float(m.get("spend", 0) or 0)
        purch = float(m.get("purchases", 0) or 0)
        rev = float(m.get("revenue", 0) or 0)
        impr = float(m.get("impressions", 0) or 0)
        clk = float(m.get("clicks", 0) or 0)
        m["roas"] = round(rev / spend, 2) if spend > 0 else 0
        m["cpa"] = round(spend / purch, 2) if purch > 0 else 0
        m["cpm"] = round(spend / impr * 1000, 2) if impr > 0 else 0
        m["ctr"] = round(clk / impr * 100, 2) if impr > 0 else 0
        m["cpc"] = round(spend / clk, 2) if clk > 0 else 0

    return {
        "campaigns": list(all_campaigns.values()),
        "atom_parsed_metrics_by_id": accumulated_per_cid,  # marker do atom path
    }


def _build_pseudo_daily_rows_from_atoms(camp_type, date_from, date_to, camp_status="all"):
    """Constroi (sales_campaigns, daily_rows) no formato de _get_shared_daily_insights
    a partir dos atoms. Cada atom vira N rows (um por campanha-dia) com date_start.
    Retorna None se atoms incompletos.

    camp_status filtra campanhas pelo status atual ('active', 'paused', 'all').
    Status do atom mais recente vence (refletindo estado atual da campanha)."""
    accounts = [a for a in _get_accounts_for_type(camp_type) if a]
    if not accounts:
        return None
    if not _atoms_can_serve_range(accounts, date_from, date_to):
        return None

    all_campaigns = {}
    daily_rows = []
    for acc in accounts:
        atoms_list, _ = get_atoms_for_range('acc', acc, date_from, date_to)
        # Atom mais recente vence pra metadata (status). Reverso = mais novo primeiro.
        for atom in reversed(atoms_list):
            atom_date = atom['date']
            payload = atom['payload']
            for c in payload.get('campaigns') or []:
                cid = c.get('id')
                if cid and cid not in all_campaigns:
                    cc = dict(c)
                    cc['_account_id'] = acc
                    all_campaigns[cid] = cc
            for cid, ins in (payload.get('insights_by_id') or {}).items():
                row = dict(ins)
                row['date_start'] = atom_date
                row['date_stop'] = atom_date
                row['campaign_id'] = cid
                daily_rows.append(row)

    sales_campaigns = _filter_campaigns_by_type(list(all_campaigns.values()), camp_type)
    # Filtra por status atual
    allowed_status = _atom_status_allowed(camp_status)
    sales_campaigns = [c for c in sales_campaigns if (c.get("status") or "").upper() in allowed_status]
    sales_ids = {c['id'] for c in sales_campaigns}
    daily_rows = [r for r in daily_rows if r.get('campaign_id') in sales_ids]
    return sales_campaigns, daily_rows


# ── BACKFILL ENGINE ──────────────────────────────────────────────────
# Fila persistente em JSON. Worker daemon thread processa 1 atom por vez
# com pacing adaptativo (10/h normal, 5/h se BUC>50%, 3/h se BUC>70%).

_BACKFILL_QUEUE_FILE = os.path.join(os.path.dirname(__file__), "atom_backfill_queue.json")
_BACKFILL_STATE_FILE = os.path.join(os.path.dirname(__file__), "atom_backfill_state.json")
_backfill_lock = threading.Lock()
_backfill_thread_started = False
_backfill_paused = False  # toggle via admin button
_backfill_state = {"last_fetch_at": None, "current_pacing_h": 10, "target_days": 30}
# Boost mode: pacing acelerado (90/h = 40s) por janela limitada de tempo.
# Ativado via /api/admin/atom-boost. Respeita BUC critico (>=85% pausa,
# >=70% volta pra 3/h, >=50% volta pra 5/h). Auto-desliga ao expirar.
_backfill_boost = {"active": False, "until_ts": 0, "seconds_between": 40, "pacing_h": 90}


def _save_backfill_persistent_state():
    """Persiste boost+target+pacing em arquivo pra que workers gunicorn
    paralelos vejam o mesmo estado (memoria nao e compartilhada entre eles)."""
    try:
        with open(_BACKFILL_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "boost": dict(_backfill_boost),
                "target_days": _backfill_state.get("target_days", 30),
                "last_fetch_at": _backfill_state.get("last_fetch_at"),
                "current_pacing_h": _backfill_state.get("current_pacing_h", 10),
            }, f)
    except Exception as e:
        print(f"[BACKFILL] Erro salvar state: {e}")


def _load_backfill_persistent_state():
    """Carrega state do arquivo (se existir) — chamado nos endpoints/worker
    pra ter sempre a versao atual em qualquer worker."""
    try:
        if not os.path.exists(_BACKFILL_STATE_FILE):
            return
        with open(_BACKFILL_STATE_FILE, "r", encoding="utf-8") as f:
            d = json.load(f) or {}
        boost = d.get("boost") or {}
        for k in ("active", "until_ts", "seconds_between", "pacing_h"):
            if k in boost:
                _backfill_boost[k] = boost[k]
        if "target_days" in d:
            _backfill_state["target_days"] = d["target_days"]
        if d.get("last_fetch_at"):
            _backfill_state["last_fetch_at"] = d["last_fetch_at"]
        if "current_pacing_h" in d:
            _backfill_state["current_pacing_h"] = d["current_pacing_h"]
    except Exception:
        pass


def _load_backfill_queue():
    try:
        if not os.path.exists(_BACKFILL_QUEUE_FILE):
            return []
        with open(_BACKFILL_QUEUE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def _save_backfill_queue(queue):
    try:
        with open(_BACKFILL_QUEUE_FILE, "w", encoding="utf-8") as f:
            json.dump(queue, f, ensure_ascii=False)
    except Exception as e:
        print(f"[BACKFILL] Erro salvar fila: {e}")


def _populate_backfill_queue(days_back=30):
    """Adiciona atoms faltantes a fila. Idempotente — nao duplica entradas
    nem refaz atoms ja cacheados. Retorna # de atoms adicionados."""
    _load_backfill_persistent_state()
    if days_back > _backfill_state.get("target_days", 30):
        _backfill_state["target_days"] = days_back
        _save_backfill_persistent_state()
    accounts = set()
    for ct in VALID_CAMP_TYPES:
        for acc in _get_accounts_for_type(ct):
            if acc:
                accounts.add(acc)
    if ACCOUNT_ID:
        accounts.add(ACCOUNT_ID)

    with _backfill_lock:
        queue = _load_backfill_queue()
        existing = set((q.get('acc'), q.get('date')) for q in queue)

        today = _now_br().replace(hour=0, minute=0, second=0, microsecond=0)
        added = 0
        for d_offset in range(1, days_back + 1):
            target_date = (today - timedelta(days=d_offset)).strftime("%Y-%m-%d")
            for acc in sorted(accounts):
                if (acc, target_date) in existing:
                    continue
                # Skip se atom ja existe (talvez populado por outro fluxo)
                if get_atom('acc', acc, target_date) is not None:
                    continue
                queue.append({
                    "acc": acc,
                    "date": target_date,
                    "added_at": _now_br().isoformat(timespec="seconds"),
                    "attempts": 0,
                })
                added += 1
        _save_backfill_queue(queue)
    if added:
        _log_atom_event("backfill_queue_populated", {"added": added, "days_back": days_back})
    return added


def _backfill_get_pacing_seconds():
    """Determina segundos entre fetches.

    Meta hard-block real: BUC ~95-100%. Throttle agressivo, mantendo
    producao alta ate beira do bloqueio.

        BUC >= 92%: 1200s (3/h, beira do bloqueio real)
        BUC >= 82%: 60s   (60/h, ainda muito produtivo)
        BUC < 82% normal: 360s (10/h padrao)
        BUC < 82% + BOOST: configurado (ex 24s = 150/h)"""
    _load_backfill_persistent_state()
    try:
        worst_pct, _ = _worst_usage_pct()
    except Exception:
        worst_pct = 0
    if worst_pct >= 92:
        _backfill_state["current_pacing_h"] = 3
        _save_backfill_persistent_state()
        return 1200
    if worst_pct >= 82:
        _backfill_state["current_pacing_h"] = 60
        _save_backfill_persistent_state()
        return 60
    # BUC saudavel (<82%) — checa boost
    if _backfill_boost.get("active"):
        if time.time() < _backfill_boost.get("until_ts", 0):
            _backfill_state["current_pacing_h"] = _backfill_boost.get("pacing_h", 90)
            _save_backfill_persistent_state()
            return _backfill_boost.get("seconds_between", 40)
        # Boost expirou — auto-desliga
        _backfill_boost["active"] = False
        _log_atom_event("boost_ended", {"reason": "expired"})
    _backfill_state["current_pacing_h"] = 10
    _save_backfill_persistent_state()
    return 360


def _backfill_worker():
    """Loop principal do backfill. Daemon thread.
    Tambem roda revalidacao automatica D-1 a D-7 a cada 6h."""
    print(f"[BACKFILL] Worker iniciado PID {os.getpid()}")
    time.sleep(60)
    last_revalidate_ts = 0

    while True:
        try:
            if _backfill_paused:
                time.sleep(60)
                continue

            # Revalidacao 1x/dia: APENAS atoms D+2 e D+8 (~6 calls/dia)
            now_ts = time.time()
            if now_ts - last_revalidate_ts > 24 * 3600:
                try:
                    if not _buc_is_critical(threshold=70):
                        print("[BACKFILL] Revalidando atoms D+2 e D+8")
                        r = _revalidate_atoms_due_today()
                        last_revalidate_ts = now_ts
                        print(f"[BACKFILL] Revalidacao auto: {r}")
                except Exception as e:
                    print(f"[BACKFILL] Erro revalidacao auto: {e}")

            with _backfill_lock:
                queue = _load_backfill_queue()
            if not queue:
                try:
                    _populate_backfill_queue(days_back=30)
                except Exception as e:
                    print(f"[BACKFILL] Erro re-popular: {e}")
                time.sleep(600)
                continue

            # Pega o proximo item
            with _backfill_lock:
                queue = _load_backfill_queue()
                if not queue:
                    time.sleep(60)
                    continue
                item = queue[0]
                queue = queue[1:]
                _save_backfill_queue(queue)

            acc = item.get('acc')
            date = item.get('date')
            if not acc or not date:
                continue

            # Skip se atom ja existe
            if get_atom('acc', acc, date) is not None:
                continue

            # Circuit breaker: BUC beira do hard-block (>=92%) = pula esse
            # atom, espera 2min. Threshold subiu pra 92% pra nao desperdicar
            # tempo travando em BUC moderado — pacing function ja desacelera
            # gradualmente (60/h em 82%+).
            if _buc_is_critical(threshold=92):
                print(f"[BACKFILL] BUC critico — re-enfileirando {acc}/{date} e pausando 2min")
                with _backfill_lock:
                    q = _load_backfill_queue()
                    q.append(item)
                    _save_backfill_queue(q)
                time.sleep(120)
                continue

            # Fetch atom
            try:
                payload = _fetch_atom_acc_for_day(acc, date)
                if payload is None:
                    item['attempts'] = item.get('attempts', 0) + 1
                    if item['attempts'] < 3:
                        with _backfill_lock:
                            q = _load_backfill_queue()
                            q.append(item)
                            _save_backfill_queue(q)
                _backfill_state["last_fetch_at"] = _now_br().isoformat(timespec="seconds")
                _save_backfill_persistent_state()
            except Exception as e:
                print(f"[BACKFILL] Erro fetch {acc} {date}: {e}")
                item['attempts'] = item.get('attempts', 0) + 1
                if item['attempts'] < 3:
                    with _backfill_lock:
                        q = _load_backfill_queue()
                        q.append(item)
                        _save_backfill_queue(q)

            # Pacing entre fetches — dorme em CHUNKS de 60s pra reavaliar
            # BUC frequentemente. Se BUC cair, sai do sleep early e volta
            # pra pacing mais rapido na proxima iteracao. Antes ficava
            # travado por 1200s (20min) mesmo quando BUC ja tinha decaido.
            sleep_secs = _backfill_get_pacing_seconds()
            slept = 0
            while slept < sleep_secs:
                chunk = min(60, sleep_secs - slept)
                time.sleep(chunk)
                slept += chunk
                # Se BUC caiu drasticamente, encurta o sleep
                if slept >= 60:
                    new_secs = _backfill_get_pacing_seconds()
                    if new_secs < sleep_secs:
                        # Pacing acelerou — sai do sleep
                        break
        except Exception as e:
            print(f"[BACKFILL] Erro loop: {e}")
            time.sleep(60)


def _start_backfill_worker():
    """Inicia worker daemon (so 1 vez por processo)."""
    global _backfill_thread_started
    with _backfill_lock:
        if _backfill_thread_started:
            return
        _backfill_thread_started = True
    threading.Thread(target=_backfill_worker, daemon=True, name="backfill-worker").start()


def _revalidate_atoms_due_today():
    """Garante que existam atoms pra D-1 (ontem) e revalida atoms na janela
    D+2 e D+8 (atribuicao tardia da Meta).

    D-1: atom de ontem precisa existir todo dia — apos boost historico, o
    backfill nao cria automaticamente o D-1 do dia seguinte. Sem isso,
    primeiro request da manha cai no legado porque atoms nao cobrem o
    range padrao (date_to=yesterday).

    D+2 e D+8: refetch pra capturar atribuicao tardia (compras que entram
    em campanhas dias depois do gasto).

    Atoms com idade 9+ dias sao IMUTAVEIS — nao toca.
    Custo: 3 ages × N contas = ~9 calls/dia pra 3 contas."""
    accounts = set()
    for ct in VALID_CAMP_TYPES:
        for acc in _get_accounts_for_type(ct):
            if acc:
                accounts.add(acc)
    if ACCOUNT_ID:
        accounts.add(ACCOUNT_ID)

    today = _now_br().replace(hour=0, minute=0, second=0, microsecond=0)
    refetched = 0
    for age_days in (1, 2, 8):
        target_date = (today - timedelta(days=age_days)).strftime("%Y-%m-%d")
        for acc in sorted(accounts):
            try:
                payload = _fetch_atom_acc_for_day(acc, target_date, force=True)
                if payload is not None:
                    refetched += 1
                time.sleep(2)
            except Exception as e:
                print(f"[REVALIDATE-DUE] Erro {acc} {target_date}: {e}")
    _log_atom_event("revalidate_due_done", {"refetched": refetched, "ages": [1, 2, 8]})
    return {"refetched": refetched, "ages": [1, 2, 8]}


def _revalidate_recent_atoms(days_back=7, force_all=False):
    """Refetcha os atoms recentes (D-1 a D-N) de todas as contas.
    Usado pra capturar atribuicao tardia da Meta.
    force_all=True ignora cache e refetcha tudo. Senao so refetcha atoms
    com mais de 6h de idade."""
    accounts = set()
    for ct in VALID_CAMP_TYPES:
        for acc in _get_accounts_for_type(ct):
            if acc:
                accounts.add(acc)
    if ACCOUNT_ID:
        accounts.add(ACCOUNT_ID)

    today = _now_br().replace(hour=0, minute=0, second=0, microsecond=0)
    refetched = 0
    skipped = 0
    failed = 0
    for d_offset in range(1, days_back + 1):
        target_date = (today - timedelta(days=d_offset)).strftime("%Y-%m-%d")
        for acc in accounts:
            try:
                # Se nao force, checa idade
                if not force_all:
                    cache_key = f"atom_acc_v1_{acc}_{target_date}"
                    existing = get_cached(cache_key)
                    # Verifica idade do atom existente via list_atoms_metadata
                    # (mais simples: pega o cache_key e ve created_at no SQLite)
                    pass  # vamos sempre refetchar nos days_back ate D-7

                payload = _fetch_atom_acc_for_day(acc, target_date, force=True)
                if payload is None:
                    failed += 1
                else:
                    refetched += 1
                # Pequeno gap pra nao estourar BUC
                time.sleep(2)
            except Exception as e:
                failed += 1
                print(f"[REVALIDATE] Erro {acc} {target_date}: {e}")
    _log_atom_event("revalidate_done", {
        "days_back": days_back,
        "refetched": refetched,
        "failed": failed,
        "force_all": force_all,
    })
    return {"refetched": refetched, "skipped": skipped, "failed": failed}


def _backfill_force_n(n=5):
    """Forca processamento de N atoms da fila SINCRONO (ignora pacing).
    Usado pelo botao /admin 'Acelerar'. Retorna lista de resultados."""
    results = []
    for _ in range(n):
        with _backfill_lock:
            queue = _load_backfill_queue()
            if not queue:
                break
            item = queue[0]
            queue = queue[1:]
            _save_backfill_queue(queue)
        acc = item.get('acc')
        date = item.get('date')
        if not acc or not date:
            continue
        if get_atom('acc', acc, date) is not None:
            results.append({"acc": acc, "date": date, "status": "skipped_exists"})
            continue
        try:
            payload = _fetch_atom_acc_for_day(acc, date)
            results.append({
                "acc": acc, "date": date,
                "status": "ok" if payload else "failed",
            })
        except Exception as e:
            results.append({"acc": acc, "date": date, "status": "error", "error": str(e)[:200]})
        time.sleep(2)  # gap minimo entre forcados
    return results


def _validate_atom_vs_legacy(label, atom_value, legacy_value, tolerance=0.0001):
    """Compara valor calculado via atoms vs valor de cache antigo / Meta direto.
    Loga divergencias. Retorna True se OK (diff < tolerance)."""
    try:
        a = float(atom_value or 0)
        l = float(legacy_value or 0)
        if l == 0 and a == 0:
            return True
        diff_pct = abs(a - l) / max(abs(l), 1) * 100
        ok = diff_pct < (tolerance * 100)
        _log_atom_event("validate_ok" if ok else "validate_diverge", {
            "label": label,
            "atom": round(a, 4),
            "legacy": round(l, 4),
            "diff_pct": round(diff_pct, 6),
        })
        if not ok:
            now_ts = time.time()
            _atom_recent_divergences.append(now_ts)
            cutoff = now_ts - _DIVERGENCE_WINDOW_SECONDS
            _atom_recent_divergences[:] = [t for t in _atom_recent_divergences if t >= cutoff]
            if len(_atom_recent_divergences) >= _DIVERGENCE_THRESHOLD:
                _set_use_atoms(False, f"auto-revert: {len(_atom_recent_divergences)} divergencias em 1h")
                _log_atom_event("auto_revert", {"divergences": len(_atom_recent_divergences)})
        return ok
    except Exception as e:
        print(f"[ATOMS] Erro validacao {label}: {e}")
        return True  # erro silencioso nao bloqueia atoms


def _fetch_insights_for_tagged_campaigns(campaigns, base_params, extra_filters=None):
    """Busca insights para campanhas taggueadas com _account_id, agrupando as chamadas
    por conta. base_params deve ter fields, time_range, level, limit, etc. mas NAO
    filtering por campaign.id (sera injetado automaticamente).

    Chunking adaptativo: se a lista de IDs for muito grande (muitas archived),
    a Meta rejeita com 'Please reduce the amount of data'. Divide em batches
    de 30 e auto-split em caso de erro de volume. Sem isso, o Resumo com
    camp_status='all' falhava silencioso pra Vendas (retornando listas vazias)."""
    by_acc = {}
    for c in campaigns:
        acc = c.get("_account_id") or ACCOUNT_ID
        by_acc.setdefault(acc, []).append(c["id"])

    INITIAL_BATCH = 30

    def _try_fetch(acc, ids, depth=0, label="0"):
        filters = [{"field": "campaign.id", "operator": "IN", "value": ids}]
        if extra_filters:
            filters.extend(extra_filters)
        params = dict(base_params)
        params["filtering"] = json.dumps(filters)
        try:
            return meta_get_all_pages(f"{acc}/insights", params)
        except Exception as e:
            msg = str(e)
            is_vol = "reduce the amount of data" in msg or "(#100)" in msg or "(#613)" in msg
            if is_vol and len(ids) > 1 and depth < 5:
                mid = len(ids) // 2
                print(f"[INSIGHTS-SPLIT] {acc} batch {label} de {len(ids)} falhou por volume, divide em {mid} + {len(ids)-mid}")
                return _try_fetch(acc, ids[:mid], depth + 1, label + "L") + _try_fetch(acc, ids[mid:], depth + 1, label + "R")
            print(f"[MULTI-ACCT] Falha insights {acc} batch {label}: {e}")
            return []

    def _chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    all_rows = []
    for acc, ids in by_acc.items():
        if not acc or not ids:
            continue
        for batch_idx, id_batch in enumerate(_chunks(ids, INITIAL_BATCH)):
            all_rows.extend(_try_fetch(acc, id_batch, 0, str(batch_idx)))
    return all_rows

SUPER_ADMIN_EMAIL = "f4cure@gmail.com"  # Admin principal — invisível e intocável
ADMIN_DEFAULT_PASS = os.getenv("ADMIN_PASSWORD", "ibc!facure@1010")
USERS_FILE = os.path.join(os.path.dirname(__file__), "users.json")
ACTIVITY_LOG_FILE = os.path.join(os.path.dirname(__file__), "activity_log.json")
ACTIVITY_LOG_MAX = 5000  # Mantem so os 5000 eventos mais recentes
_activity_lock = threading.Lock()


def _client_ip():
    """Retorna IP real do cliente (ProxyFix ja aplica X-Forwarded-For)."""
    return request.headers.get("X-Real-IP") or request.remote_addr or ""


def log_activity(email, event, session_id="", extra=None):
    """Grava um evento no activity_log.json. Append com limite para evitar arquivo infinito."""
    try:
        entry = {
            "email": email,
            "event": event,  # login | logout | heartbeat
            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "ip": _client_ip(),
            "session_id": session_id or "",
        }
        if extra:
            entry.update(extra)
        with _activity_lock:
            log = []
            if os.path.exists(ACTIVITY_LOG_FILE):
                try:
                    with open(ACTIVITY_LOG_FILE, "r") as f:
                        log = json.load(f)
                except Exception:
                    log = []
            log.append(entry)
            if len(log) > ACTIVITY_LOG_MAX:
                log = log[-ACTIVITY_LOG_MAX:]
            with open(ACTIVITY_LOG_FILE, "w") as f:
                json.dump(log, f)
    except Exception as e:
        print(f"[LOG] Falha ao gravar atividade: {e}")


def _load_users():
    """Carrega usuarios do arquivo JSON."""
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE, "r") as f:
            users = json.load(f)
    else:
        users = {}
    # Super admin: senha/role/must_reset sao forcados, mas preserva last_login
    # (e outros campos dinamicos que ja tenham sido gravados no arquivo).
    existing = users.get(SUPER_ADMIN_EMAIL, {}) or {}
    users[SUPER_ADMIN_EMAIL] = {
        **existing,
        "password": ADMIN_DEFAULT_PASS,
        "role": "super_admin",
        "must_reset": False,
    }
    return users


def _save_users(users):
    """Salva usuarios no arquivo JSON."""
    with open(USERS_FILE, "w") as f:
        json.dump(users, f, indent=2)


def _get_users():
    return _load_users()


def _check_login(username, password):
    users = _get_users()
    user = users.get(username)
    if not user:
        return None
    if user["password"] == password:
        return user
    return None


def _is_admin(username):
    users = _get_users()
    user = users.get(username)
    return user and user.get("role") in ("admin", "super_admin")


def _is_super_admin(username):
    return username == SUPER_ADMIN_EMAIL


# Compatibilidade: USERS dict para login existente
@property
def USERS():
    return {k: v["password"] for k, v in _get_users().items()}

PURCHASE_TYPES = [
    "purchase",
    "offsite_conversion.fb_pixel_purchase",
    "onsite_conversion.purchase",
]

LEAD_TYPES = [
    "lead",
    "onsite_conversion.lead_grouped",
    "offsite_conversion.fb_pixel_lead",
]

# Action_types que representam "seguir" no Instagram/Facebook.
# IMPORTANTE: incluir APENAS types que SAO seguidores de verdade. Nao incluir
# post_save, page_engagement, etc — esses inflam a contagem com outras acoes.
# Se o action_type real das campanhas nao estiver aqui, a contagem vai 0 e
# aparece o diagnostico /api/admin/crescimento-preview pra descobrir qual usar.
FOLLOW_TYPES = [
    "onsite_conversion.follow",
    "follow",
    "onsite_conversion.ig_following",
    "instagram_follow",
    "ig_account_follow",
]

# Action_types de "Visitas ao perfil do Instagram" — passo do funil DISTINTO
# de link_click. link_click e clique em qualquer link do ad (landing page, etc);
# profile_visit e especificamente quando o usuario abre o perfil do IG.
# O funil correto e: impressoes -> cliques -> VISITAS NO PERFIL -> seguidores.
PROFILE_VISIT_TYPES = [
    "onsite_conversion.ig_profile_visit",
    "ig_profile_visit",
    "onsite_conversion.instagram_profile_visit",
    "instagram_profile_visit",
    "profile_visit",
]

CAMP_TYPE_VENDAS = "vendas"
CAMP_TYPE_METEORICOS = "meteoricos"
CAMP_TYPE_COMERCIAL = "comercial"
CAMP_TYPE_CRESCIMENTO = "crescimento"
CAMP_TYPE_NUTRICAO = "nutricao"
VALID_CAMP_TYPES = (CAMP_TYPE_VENDAS, CAMP_TYPE_METEORICOS, CAMP_TYPE_COMERCIAL, CAMP_TYPE_CRESCIMENTO, CAMP_TYPE_NUTRICAO)

# Produtos comerciais (highticket). Chave = token no nome da campanha.
# CSI/PNL estao pausados atualmente mas aparecem quando o filtro de status inclui pausadas.
COMERCIAL_PRODUCTS = {
    "MTR": "Master Trainer",
    "PSC": "Professional & Self Coaching",
    "OHIO": "Ohio",
    "CSI": "Constelacao Sistemica",
    "PNL": "PNL",
}


def _normalize_camp_type(ct):
    """Normaliza e valida o parametro camp_type vindo da request."""
    ct = (ct or "").strip().lower()
    if ct not in VALID_CAMP_TYPES:
        return CAMP_TYPE_VENDAS
    return ct


def _get_conversion_types(camp_type):
    """Retorna a lista de action_types da conversao primaria do tipo.
    Vendas = purchase. Meteoricos/Comercial = lead. Crescimento = follow (Instagram)."""
    if camp_type == CAMP_TYPE_CRESCIMENTO:
        return FOLLOW_TYPES
    if camp_type in (CAMP_TYPE_METEORICOS, CAMP_TYPE_COMERCIAL):
        return LEAD_TYPES
    return PURCHASE_TYPES


def _name_tokens(name):
    """Tokeniza nome de campanha normalizando acentos, pontos, hifens, brackets e espacos."""
    if not name:
        return set()
    u = name.upper()
    # Trata como separadores: hifen, ponto, espaco, brackets, parenteses, barras,
    # virgula, dois-pontos, ponto-e-virgula, exclamacao, interrogacao, pipe.
    # :/;/!/? cobrem nomes auto-gerados tipo "Post do Instagram: [caption]".
    for sep in ["-", ".", " ", "[", "]", "(", ")", "/", "\\", ",", ":", ";", "!", "?", "|"]:
        u = u.replace(sep, "_")
    u = (u.replace("Ç", "C").replace("Ã", "A").replace("Á", "A")
         .replace("É", "E").replace("Í", "I").replace("Ó", "O").replace("Ú", "U"))
    return set(t for t in u.split("_") if t)


def _is_meteoricos_campaign(name):
    """True se o nome contem token METEORICO/METEORICOS em qualquer posicao."""
    tokens = _name_tokens(name)
    return "METEORICO" in tokens or "METEORICOS" in tokens


def _name_tokens_ordered(name):
    """Tokeniza preservando ordem (lista, nao set). Usado pra logica de
    'primeira classificacao escrita ganha'."""
    if not name:
        return []
    u = name.upper()
    for sep in ["-", ".", " ", "[", "]", "(", ")", "/", "\\", ",", ":", ";", "!", "?", "|"]:
        u = u.replace(sep, "_")
    u = (u.replace("Ç", "C").replace("Ã", "A").replace("Á", "A")
         .replace("É", "E").replace("Í", "I").replace("Ó", "O").replace("Ú", "U"))
    return [t for t in u.split("_") if t]


def _primary_type_from_name(name):
    """Retorna o tipo cujo keyword aparece PRIMEIRO no nome da campanha.
    'Primeira classificacao escrita ganha' — evita double-classify.

    Ex: 'VENDAS_DSP_GOIANIA_ENGAJAMENTO' -> CAMP_TYPE_VENDAS (VENDAS vem antes)
        'NUTRICAO_MTR_DSP' -> CAMP_TYPE_NUTRICAO (NUTRICAO vem antes de MTR)
        'POST_Instagram: caption' -> CAMP_TYPE_NUTRICAO (Post+Instagram sem
            outro keyword antes = Nutricao)

    Retorna None se nao achar nenhum keyword de classificacao."""
    tokens = _name_tokens_ordered(name)
    if not tokens:
        return None
    token_set = set(tokens)
    # RMKT bloqueia comercial (remarketing nao eh lead-gen)
    has_rmkt = bool(token_set & {"RMKT", "REMARKETING", "RETARGETING", "NURTURE"})
    for tok in tokens:
        if tok == "VENDAS":
            return CAMP_TYPE_VENDAS
        if tok in ("METEORICO", "METEORICOS"):
            return CAMP_TYPE_METEORICOS
        if tok in ("CRESCIMENTO", "CRESC"):
            return CAMP_TYPE_CRESCIMENTO
        if tok in ("NUTRICAO", "ENGAJAMENTO", "RECONHECIMENTO"):
            return CAMP_TYPE_NUTRICAO
        if tok in COMERCIAL_PRODUCTS and not has_rmkt:
            return CAMP_TYPE_COMERCIAL
    # Special: POST + INSTAGRAM em qualquer posicao (sem outro keyword antes)
    if "POST" in token_set and "INSTAGRAM" in token_set:
        return CAMP_TYPE_NUTRICAO
    return None


def _is_crescimento_campaign(name):
    return _primary_type_from_name(name) == CAMP_TYPE_CRESCIMENTO


def _is_post_instagram_campaign(name):
    """Nome tem POST+INSTAGRAM sem outro keyword antes."""
    tokens = _name_tokens(name)
    return (_primary_type_from_name(name) == CAMP_TYPE_NUTRICAO
            and "POST" in tokens and "INSTAGRAM" in tokens)


def _is_reconhecimento_campaign(name):
    tokens = _name_tokens(name)
    return (_primary_type_from_name(name) == CAMP_TYPE_NUTRICAO
            and "RECONHECIMENTO" in tokens)


def _is_nutricao_campaign(name):
    return _primary_type_from_name(name) == CAMP_TYPE_NUTRICAO


def _is_comercial_campaign(name):
    return _primary_type_from_name(name) == CAMP_TYPE_COMERCIAL


def _is_vendas_campaign_by_name(name):
    """Fallback de Vendas por NOME — True se a PRIMEIRA classificacao escrita
    no nome eh VENDAS. Garante exclusividade (1 campanha -> 1 tipo)."""
    return _primary_type_from_name(name) == CAMP_TYPE_VENDAS


def _filter_campaigns_by_type(campaigns, camp_type):
    """Filtra campanhas conforme o tipo selecionado no dashboard.
    Overrides manuais (campaign_overrides.json) tem precedencia sobre o
    auto-detect — admin pode reclassificar ou incluir campanhas manualmente.
    - vendas: objective == OUTCOME_SALES
    - meteoricos: nome contem token METEORICO em qualquer posicao
    - comercial: nome contem token de produto (MTR, PSC, OHIO, CSI, PNL)"""
    overrides = _load_overrides()
    result = []
    for c in campaigns:
        cid = c.get("id", "")
        ov = overrides.get(cid)
        if ov is not None:
            # Override sempre vence auto-detect
            if ov.get("camp_type") == camp_type:
                # injeta dados do override pro grupper usar
                c["_override"] = ov
                result.append(c)
            continue
        # Auto-detect
        name = c.get("name", "")
        if camp_type == CAMP_TYPE_METEORICOS:
            if _is_meteoricos_campaign(name):
                result.append(c)
        elif camp_type == CAMP_TYPE_COMERCIAL:
            if _is_comercial_campaign(name):
                result.append(c)
        elif camp_type == CAMP_TYPE_CRESCIMENTO:
            if _is_crescimento_campaign(name):
                result.append(c)
        elif camp_type == CAMP_TYPE_NUTRICAO:
            if _is_nutricao_campaign(name):
                result.append(c)
        else:
            # Vendas: primeira classificacao no nome eh VENDAS, OU nome sem
            # keyword de tipo algum + objective=OUTCOME_SALES (legacy).
            # Nao adiciona se outra classificacao aparece antes no nome
            # (ex: 'NUTRICAO_DSP_VENDAS' eh Nutricao, nao Vendas).
            primary = _primary_type_from_name(name)
            if primary == CAMP_TYPE_VENDAS:
                result.append(c)
            elif primary is None and c.get("objective") == "OUTCOME_SALES":
                result.append(c)
    return result


def _camp_type_from_request():
    """Helper que le camp_type da request e normaliza. Default vendas."""
    return _normalize_camp_type(request.args.get("camp_type", CAMP_TYPE_VENDAS))


# Funil: Landing Page View, View Content, Add to Cart, Initiate Checkout
LPV_TYPES = ["landing_page_view"]
VIEW_CONTENT_TYPES = [
    "view_content",
    "offsite_conversion.fb_pixel_view_content",
]
ATC_TYPES = [
    "add_to_cart",
    "offsite_conversion.fb_pixel_add_to_cart",
]
IC_TYPES = [
    "initiate_checkout",
    "offsite_conversion.fb_pixel_initiate_checkout",
]

# ── Nutricao: metricas de video ────────────────────────────────────────
# ThruPlay = visualizacao ate 15s OU video inteiro (se < 15s). Metrica
# principal das campanhas NUTRICAO que otimizam para video view.
# Funil: Impressoes -> Plays -> 25% -> 50% -> 75% -> 95% (proxy de 90%+)
# -> 100% -> ThruPlay. Meta API expoe campos dedicados para cada marco.
VIDEO_METRIC_FIELDS = (
    "video_play_actions,"
    "video_p25_watched_actions,"
    "video_p50_watched_actions,"
    "video_p75_watched_actions,"
    "video_p95_watched_actions,"
    "video_p100_watched_actions,"
    "video_thruplay_watched_actions,"
    "video_avg_time_watched_actions"
)


def _extract_video_metric(row, field):
    """Extrai valor numerico de um campo de video actions (video_p25_watched_actions, etc).
    Esses campos vem como [{action_type, value}] e so tem uma entrada para video_view.
    Retorna soma total."""
    actions = row.get(field) or []
    total = 0
    for a in actions:
        try:
            total += int(float(a.get("value", 0) or 0))
        except Exception:
            pass
    return total


# ── Auth ───────────────────────────────────────────────────────────────

def login_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            if request.path.startswith("/api/"):
                return jsonify({"ok": False, "error": "Não autenticado"}), 401
            return redirect(url_for("login_page"))
        return f(*args, **kwargs)
    return decorated


def not_viewer_required(f):
    """Exige login e role != viewer. Viewer2 acessa (campanhas/projecao/breakdowns),
    so o viewer simples e bloqueado."""
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            if request.path.startswith("/api/"):
                return jsonify({"ok": False, "error": "Não autenticado"}), 401
            return redirect(url_for("login_page"))
        if session.get("role") == "viewer":
            return jsonify({"ok": False, "error": "Acesso restrito a administradores"}), 403
        return f(*args, **kwargs)
    return decorated


def _cache_ttl_for_range(date_from, date_to):
    """Define TTL do cache baseado no tamanho do range.
    Dados recentes (1-7 dias) mudam mais: a Meta atualiza atribuicoes
    retroativamente ate ~48h. Cache mais curto pra essas faixas evita
    inconsistencia entre "1d" e o ultimo ponto do grafico "30d".
    Retorna TTL em horas (pode ser fracionado)."""
    try:
        d_from = datetime.strptime(date_from, "%Y-%m-%d")
        d_to = datetime.strptime(date_to, "%Y-%m-%d")
        diff = (d_to - d_from).days + 1
    except Exception:
        return 20
    if diff <= 1:
        return 0.6  # 1d: 36min — ciclo de refresh e 30min, margem de 6min evita gap
    if diff <= 7:
        return 3    # 7d: 3h
    if diff <= 14:
        return 8    # 14d: 8h
    return 20       # 30d+: 20h


def _enforce_range_for_role(date_from, date_to):
    """Bloqueia ranges pesados para perfis restritos.
    - viewer: delegado a logica especifica de cache em /all-creatives
    - viewer2: limite de 60 dias (nao pode puxar 90d ou ranges customizados longos)
    Retorna None se OK, ou um tuple (response, status) pra retornar direto do endpoint.
    """
    role = session.get("role")
    if role != "viewer2":
        return None
    try:
        d_from = datetime.strptime(date_from, "%Y-%m-%d")
        d_to = datetime.strptime(date_to, "%Y-%m-%d")
        diff_days = (d_to - d_from).days + 1
    except Exception:
        return None
    if diff_days > 60:
        return jsonify({
            "ok": False,
            "error": "Periodo maximo para o perfil Analisador e 60 dias.",
            "role_limited": True,
        }), 403
    return None


@app.route("/login", methods=["GET", "POST"])
def login_page():
    if request.method == "GET":
        if session.get("logged_in"):
            return redirect(url_for("dashboard"))
        return render_template("dashboard_login.html")

    data = request.get_json() if request.is_json else request.form
    username = data.get("username", "")
    password = data.get("password", "")

    user = _check_login(username, password)
    if user:
        session.permanent = True
        session["logged_in"] = True
        session["username"] = username
        session["role"] = user.get("role", "viewer")
        session["real_role"] = user.get("role", "viewer")  # imutavel durante preview
        session["must_reset"] = user.get("must_reset", False)
        session["session_id"] = str(uuid.uuid4())
        # Registra evento de login no log de atividade
        log_activity(username, "login", session_id=session["session_id"])
        # Registra timestamp de ultimo acesso no users.json.
        # Para o super admin cria a entrada se ela ainda nao existir no arquivo —
        # _load_users re-injeta senha/role/must_reset, entao nao ha risco de conflito.
        try:
            persisted = {}
            if os.path.exists(USERS_FILE):
                with open(USERS_FILE, "r") as f:
                    persisted = json.load(f)
            if username not in persisted:
                persisted[username] = {}
            # Timezone explicito (UTC) para o frontend conseguir converter corretamente
            persisted[username]["last_login"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            _save_users(persisted)
        except Exception as e:
            print(f"[AUTH] Erro ao registrar last_login: {e}")

        if request.is_json:
            return jsonify({"ok": True, "must_reset": user.get("must_reset", False)})
        if user.get("must_reset"):
            return redirect("/admin/reset-password")
        return redirect(url_for("dashboard"))

    if request.is_json:
        return jsonify({"ok": False, "error": "Credenciais inválidas"}), 401
    return render_template("dashboard_login.html", error="Credenciais inválidas")


@app.route("/logout")
def logout():
    if session.get("logged_in"):
        log_activity(session.get("username", ""), "logout", session_id=session.get("session_id", ""))
    session.clear()
    return redirect(url_for("login_page"))


# ── Pages ──────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return redirect(url_for("login_page"))


@app.route("/dashboard")
@login_required
def dashboard():
    real_role = session.get("real_role") or session.get("role", "viewer")
    current_role = session.get("role", real_role)
    preview_mode = current_role != real_role
    return render_template(
        "dashboard.html",
        username=session.get("username"),
        role=current_role,
        real_role=real_role,
        preview_mode=preview_mode,
    )


# ── Meta API helpers ───────────────────────────────────────────────────

# ── Rate Limit Protection ──────────────────────────────────────────────
# Meta retorna 3 metricas independentes via x-business-use-case-usage:
#   - call_count (% do numero de chamadas/hora)
#   - total_cputime (% de CPU consumido em queries pesadas tipo insights)
#   - total_time (% de tempo de processamento)
# O limite real eh o MAIOR dos 3. Armazenamos por conta porque a Meta
# aplica o limite por business/account separadamente.
_rate_per_account = {}  # {acc_id: {call, cpu, time, regain_seconds, last_check}}
_MIN_DELAY = 1
_last_call_time = 0


def _decay_usage(u):
    """Estima uso atual aplicando decay agressivo desde o ultimo check.

    Como so READ-only e Meta raramente bloqueia, priorizamos nao assustar
    o usuario com banner falso. Se Meta nao sinalizou bloqueio (regain=0),
    decaimos em 2min — o valor armazenado ficou velho, nao reflete o real.
    Se regain > 0, Meta esta de fato bloqueando; preserva o valor por mais
    tempo (5min) ate expirar a janela de bloqueio."""
    last = u.get("last_check", 0)
    if not last:
        return 0, 0, 0
    elapsed = time.time() - last
    has_block = (u.get("regain_seconds", 0) or 0) > 0
    DECAY_WINDOW = 300 if has_block else 120  # 5min com bloqueio, 2min sem
    if elapsed > DECAY_WINDOW:
        return 0, 0, 0
    factor = max(0.0, 1.0 - (elapsed / DECAY_WINDOW))
    return (
        int(u.get("call", 0) * factor),
        int(u.get("cpu", 0) * factor),
        int(u.get("time", 0) * factor),
    )


def _worst_usage_pct():
    """Retorna (pct, acc_id) do pior uso entre todas as contas monitoradas.
    Aplica decay temporal pra nao travar em 71% quando Meta ja resetou."""
    worst = 0
    worst_acc = None
    for acc_id, u in _rate_per_account.items():
        call, cpu, tim = _decay_usage(u)
        m = max(call, cpu, tim)
        if m > worst:
            worst = m
            worst_acc = acc_id
    return worst, worst_acc


def _enforce_rate_limit():
    """Preemptive throttle baseado no BUC + respeita regain_seconds explicito.

    Comportamento:
      regain > 0 (Meta bloqueou)    -> pausa ate 30s
      BUC >= 98%                    -> pausa 60s (beira do hard-block)
      BUC >= 90%                    -> pausa 15s (zona vermelha)
      BUC >= 80%                    -> pausa 5s  (zona amarela)
      BUC <  80%                    -> so o _MIN_DELAY normal

    Protege scheduler/warmups/bg threads contra stampede: se um bg pegar BUC
    em 85%, os proximos calls automaticamente desaceleram. Nao bloqueia o
    request do usuario — so desacelera cada call individual."""
    global _last_call_time
    now = time.time()
    elapsed = now - _last_call_time
    if elapsed < _MIN_DELAY:
        time.sleep(_MIN_DELAY - elapsed)

    # 1) Regain ativo (Meta bloqueando explicitamente)
    regain = 0
    acc_regain = None
    for acc_id, u in _rate_per_account.items():
        r = u.get("regain_seconds", 0) or 0
        age = time.time() - u.get("last_check", 0)
        if r > 0 and age < 900:
            if r > regain:
                regain = r
                acc_regain = acc_id
    if regain > 0:
        wait = min(regain, 30)
        print(f"[RATE LIMIT] {acc_regain}: Meta bloqueando por {regain}s — pausando {wait}s")
        time.sleep(wait)
        _last_call_time = time.time()
        return

    # 2) Preemptive throttle pelo BUC (evita stampede)
    worst_pct, worst_acc = _worst_usage_pct()
    if worst_pct >= 98:
        wait = 60
    elif worst_pct >= 90:
        wait = 15
    elif worst_pct >= 80:
        wait = 5
    else:
        wait = 0
    if wait > 0:
        print(f"[RATE LIMIT] BUC {worst_acc}={worst_pct}% — pausando {wait}s")
        time.sleep(wait)

    _last_call_time = time.time()


def _update_rate_from_headers(resp):
    """Atualiza uso por conta a partir de x-business-use-case-usage.
    Esse header vem como {acc_id: [{call_count, total_cputime, total_time, estimated_time_to_regain_access}]}"""
    import json as _json
    usage = resp.headers.get("x-business-use-case-usage", "")
    if not usage:
        return
    try:
        data = _json.loads(usage)
        now_ts = time.time()
        for acct_id, usages in data.items():
            if not usages:
                continue
            # Meta retorna lista — pega o pior (maior uso) dentre os entries
            worst_call = worst_cpu = worst_time = worst_regain = 0
            for u in usages:
                worst_call = max(worst_call, u.get("call_count", 0))
                worst_cpu = max(worst_cpu, u.get("total_cputime", 0))
                worst_time = max(worst_time, u.get("total_time", 0))
                worst_regain = max(worst_regain, u.get("estimated_time_to_regain_access", 0))
            _rate_per_account[acct_id] = {
                "call": worst_call,
                "cpu": worst_cpu,
                "time": worst_time,
                "regain_seconds": worst_regain,
                "last_check": now_ts,
            }
    except Exception:
        pass


def _buc_is_critical(threshold=95):
    """True se BUC >= threshold% OU Meta sinalizou regain_seconds ativo.
    Usado pra circuit-breaker: scheduler/warmups param quando BUC critico
    pra nao agravar o problema. Reles meta_get tem preemptive throttle
    proprio, mas pra jobs em background e melhor abortar cedo."""
    worst_pct, _ = _worst_usage_pct()
    if worst_pct >= threshold:
        return True
    # Regain ativo
    for acc_id, u in _rate_per_account.items():
        r = u.get("regain_seconds", 0) or 0
        age = time.time() - u.get("last_check", 0)
        if r > 0 and age < 900:
            return True
    return False


def _wait_for_buc_ok(max_wait_seconds=600, check_interval=30):
    """Espera ate BUC ficar OK (< 85%) ou timeout. Usado em jobs longos
    (warmups, scheduler) pra retomar quando o BUC drenar. Max default 10min."""
    waited = 0
    while waited < max_wait_seconds:
        worst_pct, worst_acc = _worst_usage_pct()
        if worst_pct < 85 and not _buc_is_critical(threshold=85):
            return True
        print(f"[RATE LIMIT] Aguardando BUC drenar: {worst_acc}={worst_pct}% (aguardou {waited}s)")
        time.sleep(check_interval)
        waited += check_interval
    return False


def get_dashboard_rate_info():
    """Retorna info de rate limit (com decay temporal aplicado)."""
    worst_pct, worst_acc = _worst_usage_pct()
    accounts = {}
    now = time.time()
    for acc_id, u in _rate_per_account.items():
        call, cpu, tim = _decay_usage(u)
        age = int(now - u.get("last_check", now))
        accounts[acc_id] = {
            "call_count": call,
            "total_cputime": cpu,
            "total_time": tim,
            "max": max(call, cpu, tim),
            "regain_seconds": u.get("regain_seconds", 0),
            "age_seconds": age,
            "stale": age > 300,  # >5min = dado velho
        }
    return {
        "pct": worst_pct,
        "worst_account": worst_acc,
        "accounts": accounts,
    }


def _inc_meta_call_counter():
    """Incrementa contador de chamadas Meta no contexto da request atual.
    Usado pelo middleware de logging pra saber quantas chamadas o request
    gerou. Usa flask.g (thread-safe por request)."""
    try:
        from flask import has_request_context
        if has_request_context():
            g.meta_calls = (getattr(g, "meta_calls", 0) or 0) + 1
    except Exception:
        pass


def meta_get(endpoint, params=None):
    _enforce_rate_limit()
    p = {"access_token": TOKEN}
    if params:
        p.update(params)
    resp = requests.get(f"{BASE_URL}/{endpoint}", params=p, timeout=60)
    _inc_meta_call_counter()
    _update_rate_from_headers(resp)
    data = resp.json()
    if "error" in data:
        raise Exception(data["error"].get("message", str(data["error"])))
    return data


# ── Instagram Graph API: ganho de seguidores ──────────────────────────
# Perfil principal IG associado as campanhas CRESCIMENTO.
# @joserobertomarques — todas campanhas CRESCIMENTO promovem este perfil.
IG_PROFILE_ID_JRM = "17841400833978215"


def fetch_ig_follower_gain_by_day(ig_user_id, date_from, date_to):
    """Consulta Instagram Graph API dia a dia e retorna ganho liquido por dia.

    Retorna dict {'YYYY-MM-DD': net_gain_int}.

    Nota: metric_type=time_series NAO funciona para follows_and_unfollows com
    breakdown=follow_type (Meta aceita so total_value). Entao fazemos 1 query
    por dia e agregamos. 30 chamadas cobre mes inteiro. Cache 6h."""
    cache_key = f"ig_follower_gain_{ig_user_id}_{date_from}_{date_to}"
    cached = get_cached(cache_key)
    if cached is not None:
        return cached

    result = {}
    from datetime import datetime as _dt
    try:
        d_from = _dt.strptime(date_from, "%Y-%m-%d")
        d_to = _dt.strptime(date_to, "%Y-%m-%d")
    except Exception:
        return {}

    current = d_from
    while current <= d_to:
        day_str = current.strftime("%Y-%m-%d")
        next_day = (current + timedelta(days=1)).strftime("%Y-%m-%d")
        try:
            resp = meta_get(f"{ig_user_id}/insights", {
                "metric": "follows_and_unfollows",
                "breakdown": "follow_type",
                "period": "day",
                "metric_type": "total_value",
                "since": day_str,
                "until": next_day,
            })
            follower = non_follower = 0
            for entry in resp.get("data", []):
                tv = entry.get("total_value") or {}
                for bd in tv.get("breakdowns", []):
                    for r in bd.get("results", []):
                        dims = r.get("dimension_values") or []
                        v = int(r.get("value", 0) or 0)
                        if "FOLLOWER" in dims:
                            follower = v
                        elif "NON_FOLLOWER" in dims:
                            non_follower = v
            result[day_str] = follower - non_follower
        except Exception as e:
            print(f"[IG] Erro dia {day_str}: {e}")
        current += timedelta(days=1)

    # Cache 6h — IG API tem delay de 24-48h no backfill dos valores
    set_cached(cache_key, result, ttl_hours=6)
    return result


def _extract_link_clicks_from_row(row):
    """Extrai link_clicks de um row de insights (usa inline_link_clicks se presente,
    senao busca em actions[link_click])."""
    lc = row.get("inline_link_clicks")
    try:
        lc = int(float(lc)) if lc else 0
    except Exception:
        lc = 0
    if lc > 0:
        return lc
    for a in (row.get("actions") or []):
        if a.get("action_type") == "link_click":
            try:
                return int(float(a.get("value", 0) or 0))
            except Exception:
                return 0
    return 0


def _extract_profile_visits_from_row(row):
    """Extrai VISITAS AO PERFIL do Instagram de uma linha de insights.

    Fontes (ordem de prioridade):
      1) results[] — coluna 'Resultados' do Gerenciador. PRIMARIO. A API pode
         retornar varios formatos; este parser cobre todos que ja vimos.
      2) actions[] com action_type em PROFILE_VISIT_TYPES — fallback quando
         'results' nao vem populado (algumas edge_table nao expoem o campo).
      3) conversions[] e unique_actions[] com profile_visit action_types.

    NAO cai em link_click. Click no anuncio != visita ao perfil."""
    # Fonte 1: results[] — formato pode variar:
    #   A) {"value": "123"} (flat)
    #   B) {"value": 123} (flat numerico)
    #   C) {"indicator": "...", "values": [{"value": "123"}]} (nested)
    #   D) {"values": ["123"]} (lista de strings)
    total_results = 0
    for r in (row.get("results") or []):
        if not isinstance(r, dict):
            continue
        v = r.get("value")
        if v is not None and not isinstance(v, (list, dict)):
            try:
                total_results += int(float(v))
                continue
            except Exception:
                pass
        values = r.get("values")
        if isinstance(values, list):
            for vv in values:
                if isinstance(vv, dict):
                    inner = vv.get("value", 0)
                    try:
                        total_results += int(float(inner or 0))
                    except Exception:
                        pass
                elif isinstance(vv, (int, float, str)):
                    try:
                        total_results += int(float(vv))
                    except Exception:
                        pass
    if total_results > 0:
        return total_results

    # Fonte 2: actions[] / conversions[] / unique_actions[] com action_types de profile_visit
    total_actions = 0
    for field in ("actions", "conversions", "unique_actions"):
        for a in (row.get(field) or []):
            if a.get("action_type") in PROFILE_VISIT_TYPES:
                try:
                    total_actions = max(total_actions, int(float(a.get("value", 0) or 0)))
                except Exception:
                    pass
    return total_actions


# Peso relativo de campanhas NAO-Crescimento na atribuicao de seguidores.
# Default 0.025 = R$1 em outra campanha gera 0.025 seguidor vs 1.0 em Crescimento.
# Calibrado pelos dados reais: 109.258 atribuido Meta / 125.381 NET IG = 87%.
# Valor menor dessa tabela significa que campanhas que nao otimizam seguidor
# contribuem pouco pro ganho do perfil.
CRESCIMENTO_NON_CRESCIMENTO_WEIGHT = 0.025


def _fetch_account_total_spend(acc_id, date_from, date_to):
    """Retorna gasto total de UMA conta no periodo (todas campanhas, todos tipos).
    Cache interno via cache_manager — TTL 20min.

    DUAL-READ: se USE_ATOMS ativo e atoms cobrem o range, soma de atoms
    (zero call Meta). Senao fallback pra Meta level=account."""
    cache_key = f"account_total_spend_{acc_id}_{date_from}_{date_to}"
    cached = get_cached(cache_key)
    if cached is not None:
        return cached

    # Tenta atoms primeiro
    _sync_use_atoms_from_file()
    if USE_ATOMS:
        atoms_list, missing = get_atoms_for_range('acc', acc_id, date_from, date_to)
        if not missing:
            total = 0.0
            for atom in atoms_list:
                payload = atom.get('payload') or {}
                for cid, ins in (payload.get('insights_by_id') or {}).items():
                    total += float(ins.get('spend', 0) or 0)
            set_cached(cache_key, total, ttl_hours=0.33)
            return total

    try:
        rows = meta_get_all_pages(f"{acc_id}/insights", {
            "fields": "spend",
            "time_range": json.dumps({"since": date_from, "until": date_to}),
            "level": "account",
        })
        total = sum(float(r.get("spend", 0) or 0) for r in rows)
    except Exception as e:
        print(f"[ACCOUNT-SPEND] Erro {acc_id}: {e}")
        total = 0
    set_cached(cache_key, total, ttl_hours=0.33)
    return total


def _get_crescimento_context(date_from, date_to, crescimento_spend=None):
    """Retorna contexto de atribuicao de Crescimento pra um periodo.
    Cacheado por 30min — mesma janela TTL do gasto por conta.

    Evita repetir o calculo em cada endpoint (campaigns, multi_insights,
    daily_summary, breakdowns). Antes, cada um desses chamava fetch_ig_
    follower_gain_total + fetch_total_spend_all_accounts + calc share =
    3-4 operacoes pesadas, multiplicadas por chamada.

    Args:
        date_from / date_to: periodo a analisar
        crescimento_spend: gasto da Crescimento no periodo (se None, busca)

    Returns dict com:
        ig_net_total: ganho liquido de seguidores do perfil IG
        total_acc_spend: gasto total (todas as contas que tem Crescimento)
        crescimento_spend: gasto de Crescimento especificamente
        other_spend: gasto das outras campanhas (total - crescimento)
        cresc_share: fracao do IG net atribuivel a Crescimento (0-1)
        total_seguidores_attributed: ig_net × share (seguidores 'reais' atribuidos)
    """
    # Parte 1: gasto total de todas as contas de Crescimento (cached 20min em _fetch_account_total_spend)
    accounts = _get_accounts_for_type(CAMP_TYPE_CRESCIMENTO)
    total_acc_spend = _fetch_total_spend_all_accounts(accounts, date_from, date_to)

    # Parte 2: IG follower gain (cached 6h em fetch_ig_follower_gain_total)
    try:
        ig_follower, ig_non = fetch_ig_follower_gain_total(IG_PROFILE_ID_JRM, date_from, date_to)
        ig_net_total = max(0, ig_follower - ig_non)
    except Exception as e:
        print(f"[CRESCIMENTO CTX] IG API falhou: {e}")
        ig_net_total = 0

    # Parte 3: share calculation — depende do gasto de Crescimento
    cresc_spend = crescimento_spend if crescimento_spend is not None else 0
    other_spend = max(0, total_acc_spend - cresc_spend)
    cresc_share = compute_crescimento_share(cresc_spend, other_spend) if cresc_spend > 0 else 1.0
    total_seguidores = int(round(ig_net_total * cresc_share)) if ig_net_total > 0 else 0

    return {
        "ig_net_total": ig_net_total,
        "total_acc_spend": total_acc_spend,
        "crescimento_spend": cresc_spend,
        "other_spend": other_spend,
        "cresc_share": cresc_share,
        "total_seguidores_attributed": total_seguidores,
    }


def _fetch_total_spend_all_accounts(account_ids, date_from, date_to):
    """Soma gasto total de multiplas contas."""
    total = 0
    for acc in account_ids:
        total += _fetch_account_total_spend(acc, date_from, date_to)
    return total


def compute_crescimento_share(crescimento_spend, other_spend,
                              non_cresc_weight=CRESCIMENTO_NON_CRESCIMENTO_WEIGHT):
    """Fracao dos seguidores totais que sao atribuiveis a campanhas de Crescimento.

    crescimento_spend conta 1.0; outras campanhas contam non_cresc_weight (default 0.1).
    Retorna valor entre 0 e 1."""
    cresc_eff = crescimento_spend * 1.0
    other_eff = other_spend * non_cresc_weight
    total_eff = cresc_eff + other_eff
    if total_eff <= 0:
        return 1.0
    return cresc_eff / total_eff


def compute_crescimento_follower_attribution(daily_rows, ig_net_total, crescimento_share=1.0):
    """Atribuicao de seguidores proporcional a VISITAS AO PERFIL de cada campanha.

    Funil correto: impressoes -> cliques -> visitas no perfil -> seguidores.
    Visitas ao perfil (profile_visit) e um passo DISTINTO de link_click e
    muito mais correlacionado com novos seguidores.

    ig_net_total: ganho liquido TOTAL do periodo (FOLLOWER - NON_FOLLOWER) do perfil IG
    crescimento_share: fracao do NET atribuivel a Crescimento (0-1)

    Para cada campanha:
      seguidores = ig_net_total * crescimento_share * (visitas_camp / total_visitas_cresc)

    Returns: dict {(campaign_id, date): seguidores_atribuidos_float}
    """
    visits_by_cid = {}
    days_by_cid = {}
    for row in daily_rows:
        cid = row.get("campaign_id", "")
        d = row.get("date_start", "")
        if not cid or not d:
            continue
        v = _extract_profile_visits_from_row(row)
        visits_by_cid[cid] = visits_by_cid.get(cid, 0) + v
        days_by_cid.setdefault(cid, []).append((d, v))

    total_visits = sum(visits_by_cid.values())
    if total_visits <= 0 or ig_net_total <= 0:
        return {}

    gain_for_cresc = ig_net_total * crescimento_share
    attribution = {}

    for cid, total_cid_visits in visits_by_cid.items():
        if total_cid_visits <= 0:
            continue
        cid_total_follows = gain_for_cresc * (total_cid_visits / total_visits)
        for (date, visits_day) in days_by_cid[cid]:
            if visits_day > 0:
                attribution[(cid, date)] = cid_total_follows * (visits_day / total_cid_visits)
    return attribution


def fetch_ig_follower_gain_total(ig_user_id, date_from, date_to):
    """Fallback/diagnostico: retorna (follower_total, non_follower_total) agregado."""
    try:
        resp = meta_get(f"{ig_user_id}/insights", {
            "metric": "follows_and_unfollows",
            "breakdown": "follow_type",
            "period": "day",
            "metric_type": "total_value",
            "since": date_from,
            "until": date_to,
        })
        follower = non_follower = 0
        for entry in resp.get("data", []):
            tv = entry.get("total_value") or {}
            for bd in tv.get("breakdowns", []):
                for r in bd.get("results", []):
                    dims = r.get("dimension_values") or []
                    v = int(r.get("value", 0) or 0)
                    if "FOLLOWER" in dims:
                        follower = v
                    elif "NON_FOLLOWER" in dims:
                        non_follower = v
        return follower, non_follower
    except Exception as e:
        print(f"[IG] Erro total seguidores {ig_user_id}: {e}")
        return 0, 0


class RateLimitError(Exception):
    """Erro de rate limit da Meta API."""
    pass


def _is_rate_limit_error(err_obj):
    """Detecta se um erro da Meta API é de rate limit."""
    if not isinstance(err_obj, dict):
        return False
    code = err_obj.get("code")
    subcode = err_obj.get("error_subcode")
    msg = (err_obj.get("message") or "").lower()
    # Códigos conhecidos de rate limit da Meta Marketing API
    if code in (4, 17, 32, 613, 80000, 80001, 80002, 80003, 80004, 80005, 80006, 80008, 80014):
        return True
    if subcode in (2446079, 1487742):
        return True
    return "rate" in msg and "limit" in msg


def meta_get_all_pages(endpoint, params=None, max_retries=3):
    """Busca todos os resultados paginados, com retry em rate limit.

    - Para chamadas de /insights, adiciona use_unified_attribution_setting=true
      automaticamente (mesma janela do Gerenciador de Anúncios).
    - Em caso de rate limit, tenta novamente com backoff exponencial.
    - Se ainda falhar após max_retries, levanta RateLimitError.
    """
    p = {"access_token": TOKEN, "limit": 500}
    if params:
        p.update(params)
    # Se for endpoint de insights, usar attribution unified (bate com Gerenciador)
    if "/insights" in endpoint or endpoint.endswith("insights"):
        p.setdefault("use_unified_attribution_setting", "true")
    url = f"{BASE_URL}/{endpoint}"
    all_data = []
    attempt = 0
    while url:
        _enforce_rate_limit()
        try:
            resp = requests.get(url, params=p, timeout=60)
            _inc_meta_call_counter()
            _update_rate_from_headers(resp)
            data = resp.json()
        except Exception as e:
            attempt += 1
            if attempt > max_retries:
                raise Exception(f"Falha de rede após {max_retries} tentativas: {e}")
            time.sleep(2 ** attempt)
            continue

        if "error" in data:
            err = data["error"]
            if _is_rate_limit_error(err) and attempt < max_retries:
                attempt += 1
                # Backoff exponencial: 10, 20, 40 segundos (seguro)
                wait = 10 * (2 ** (attempt - 1))
                print(f"[RATE LIMIT] {endpoint} — aguardando {wait}s (tentativa {attempt}/{max_retries})")
                time.sleep(wait)
                continue
            if _is_rate_limit_error(err):
                raise RateLimitError(err.get("message", "Rate limit"))
            raise Exception(err.get("message", str(err)))

        all_data.extend(data.get("data", []))
        url = data.get("paging", {}).get("next")
        p = None  # next URL already has params
        attempt = 0  # reset contador após sucesso
    return all_data


def extract_purchase_value(actions_or_values, field="value", types=None):
    """Extrai valor da conversao primaria evitando duplicacao.

    Estratégia: pega o PRIMEIRO match da ordem `types` que tiver valor > 0.
    Se nenhum tiver valor > 0, retorna 0.
    """
    if not actions_or_values:
        return 0.0
    if types is None:
        types = PURCHASE_TYPES
    # Procurar na ordem de prioridade dos tipos
    for ptype in types:
        for item in actions_or_values:
            if item.get("action_type") == ptype:
                v = 0.0
                try:
                    v = float(item.get(field, 0) or 0)
                except Exception:
                    pass
                if v > 0:
                    return v
    # Nenhum match com valor > 0: retornar o primeiro match qualquer
    for item in actions_or_values:
        if item.get("action_type") in types:
            try:
                return float(item.get(field, 0) or 0)
            except Exception:
                return 0.0
    return 0.0


def extract_purchase_count(actions, types=None):
    """Extrai contagem da conversao primaria evitando duplicacao."""
    if not actions:
        return 0
    if types is None:
        types = PURCHASE_TYPES
    for ptype in types:
        for item in actions:
            if item.get("action_type") == ptype:
                try:
                    v = int(float(item.get("value", 0) or 0))
                except Exception:
                    v = 0
                if v > 0:
                    return v
    for item in actions:
        if item.get("action_type") in types:
            try:
                return int(float(item.get("value", 0) or 0))
            except Exception:
                return 0
    return 0


def extract_action_count(actions, types):
    """Extrai contagem de uma lista de action_types (pega primeiro match para evitar duplicação)."""
    if not actions:
        return 0
    for t in types:
        for item in actions:
            if item.get("action_type") == t:
                try:
                    v = int(float(item.get("value", 0) or 0))
                except Exception:
                    v = 0
                if v > 0:
                    return v
    for item in actions:
        if item.get("action_type") in types:
            try:
                return int(float(item.get("value", 0) or 0))
            except Exception:
                return 0
    return 0


def _safe_int(v):
    try:
        return int(float(v or 0))
    except Exception:
        return 0


def _safe_float(v):
    try:
        return float(v or 0)
    except Exception:
        return 0.0


def _current_camp_type():
    """Le camp_type do contexto Flask g (setado pelo endpoint). Default vendas."""
    try:
        return getattr(g, "camp_type", CAMP_TYPE_VENDAS) or CAMP_TYPE_VENDAS
    except Exception:
        return CAMP_TYPE_VENDAS


def parse_insights(row, camp_type=None):
    """Transforma uma linha de insights da API em dict limpo.

    camp_type:
    - 'vendas': conversao = purchase, com revenue/ROAS
    - 'meteoricos': conversao = lead, revenue/ROAS = 0 (nao se aplica)
    - None: le do Flask g (contexto da request). Evita threading manual em helpers.

    Campo `purchases` no retorno continua carregando a contagem da conversao primaria
    (compra ou lead) para nao quebrar call sites. Frontend adapta label por camp_type.
    """
    if camp_type is None:
        camp_type = _current_camp_type()
    spend = _safe_float(row.get("spend"))
    impressions = _safe_int(row.get("impressions"))
    clicks = _safe_int(row.get("clicks"))
    reach = _safe_int(row.get("reach"))

    actions = row.get("actions", []) or []
    action_values = row.get("action_values", []) or []
    purchase_roas_list = row.get("purchase_roas", []) or []

    conv_types = _get_conversion_types(camp_type)
    purchases = extract_purchase_count(actions, types=conv_types)

    if camp_type in (CAMP_TYPE_METEORICOS, CAMP_TYPE_COMERCIAL, CAMP_TYPE_CRESCIMENTO):
        # Tipos de captacao (leads/seguidores): sem revenue/ROAS
        revenue = 0.0
        roas = 0.0
    else:
        revenue = extract_purchase_value(action_values, types=conv_types)
        # ROAS: usar purchase_roas oficial como fonte primária
        roas = 0
        if purchase_roas_list:
            for pr in purchase_roas_list:
                if pr.get("action_type") in conv_types:
                    roas = _safe_float(pr.get("value"))
                    break
            if roas == 0 and purchase_roas_list:
                roas = _safe_float(purchase_roas_list[0].get("value"))
        if roas == 0:
            roas = revenue / spend if spend > 0 else 0

    cpa = spend / purchases if purchases > 0 else 0
    ctr = _safe_float(row.get("ctr"))
    cpm = _safe_float(row.get("cpm"))
    cpp = _safe_float(row.get("cpp"))

    # Link clicks: usar inline_link_clicks (campo oficial). Fallback para actions[link_click].
    link_clicks = _safe_int(row.get("inline_link_clicks"))
    if link_clicks == 0:
        for a in actions:
            if a.get("action_type") == "link_click":
                link_clicks = _safe_int(a.get("value"))
                break

    cost_per_link_click = spend / link_clicks if link_clicks > 0 else 0

    # Funil: LPV, view_content, ATC, initiate_checkout
    lpv = extract_action_count(actions, LPV_TYPES)
    view_content = extract_action_count(actions, VIEW_CONTENT_TYPES)
    add_to_cart = extract_action_count(actions, ATC_TYPES)
    initiate_checkout = extract_action_count(actions, IC_TYPES)

    # Visitas ao perfil do Instagram — passo do funil em Crescimento.
    # Distinto de link_click (cliques em qualquer link do ad) e de LPV
    # (view de landing page). NAO deve ser mapeado em nenhum deles.
    profile_visits = _extract_profile_visits_from_row(row)

    # ── Metricas de video (Nutricao) ──
    # Funil: Plays -> 25% -> 50% -> 75% -> 95% (proxy de 90%+) -> 100% -> ThruPlay
    video_plays = _extract_video_metric(row, "video_play_actions")
    video_p25 = _extract_video_metric(row, "video_p25_watched_actions")
    video_p50 = _extract_video_metric(row, "video_p50_watched_actions")
    video_p75 = _extract_video_metric(row, "video_p75_watched_actions")
    video_p95 = _extract_video_metric(row, "video_p95_watched_actions")
    video_p100 = _extract_video_metric(row, "video_p100_watched_actions")
    video_thruplay = _extract_video_metric(row, "video_thruplay_watched_actions")
    # Custos e taxas de video
    cost_per_thruplay = spend / video_thruplay if video_thruplay > 0 else 0
    cost_per_video_play = spend / video_plays if video_plays > 0 else 0
    rate_play_p25 = (video_p25 / video_plays * 100) if video_plays > 0 else 0
    rate_p25_p50 = (video_p50 / video_p25 * 100) if video_p25 > 0 else 0
    rate_p50_p75 = (video_p75 / video_p50 * 100) if video_p50 > 0 else 0
    rate_p75_p95 = (video_p95 / video_p75 * 100) if video_p75 > 0 else 0
    rate_p95_p100 = (video_p100 / video_p95 * 100) if video_p95 > 0 else 0
    rate_play_thruplay = (video_thruplay / video_plays * 100) if video_plays > 0 else 0

    # Taxas de conversão do funil
    rate_click_lpv = (lpv / link_clicks * 100) if link_clicks > 0 else 0
    rate_lpv_ic = (initiate_checkout / lpv * 100) if lpv > 0 else 0
    rate_ic_purchase = (purchases / initiate_checkout * 100) if initiate_checkout > 0 else 0
    rate_click_purchase = (purchases / link_clicks * 100) if link_clicks > 0 else 0
    cost_per_ic = spend / initiate_checkout if initiate_checkout > 0 else 0
    cost_per_lpv = spend / lpv if lpv > 0 else 0

    # Frequência: usar campo oficial da Meta API (calcula com reach único real)
    frequency = _safe_float(row.get("frequency"))

    return {
        "spend": round(spend, 2),
        "impressions": impressions,
        "clicks": clicks,
        "reach": reach,
        "frequency": round(frequency, 2),
        "purchases": purchases,
        "revenue": round(revenue, 2),
        "roas": round(roas, 2),
        "cpa": round(cpa, 2),
        "ctr": round(ctr, 2),
        "cpm": round(cpm, 2),
        "cpp": round(cpp, 2),
        "link_clicks": link_clicks,
        "cost_per_link_click": round(cost_per_link_click, 2),
        "profile_visits": profile_visits,
        "lpv": lpv,
        "view_content": view_content,
        "add_to_cart": add_to_cart,
        "initiate_checkout": initiate_checkout,
        "rate_click_lpv": round(rate_click_lpv, 2),
        "rate_lpv_ic": round(rate_lpv_ic, 2),
        "rate_ic_purchase": round(rate_ic_purchase, 2),
        "rate_click_purchase": round(rate_click_purchase, 2),
        "cost_per_ic": round(cost_per_ic, 2),
        "cost_per_lpv": round(cost_per_lpv, 2),
        # Video (Nutricao)
        "video_plays": video_plays,
        "video_p25": video_p25,
        "video_p50": video_p50,
        "video_p75": video_p75,
        "video_p95": video_p95,
        "video_p100": video_p100,
        "video_thruplay": video_thruplay,
        "cost_per_thruplay": round(cost_per_thruplay, 2),
        "cost_per_video_play": round(cost_per_video_play, 2),
        "rate_play_p25": round(rate_play_p25, 2),
        "rate_p25_p50": round(rate_p25_p50, 2),
        "rate_p50_p75": round(rate_p50_p75, 2),
        "rate_p75_p95": round(rate_p75_p95, 2),
        "rate_p95_p100": round(rate_p95_p100, 2),
        "rate_play_thruplay": round(rate_play_thruplay, 2),
    }


def _camp_status_filter(camp_status="active"):
    """Retorna o filtro effective_status para a Meta API.
    'all' inclui ARCHIVED para cobrir campanhas de eventos ja encerrados
    (muito comum em meteoricos, cujos eventos sao de datas passadas)."""
    if camp_status == "paused":
        return '["PAUSED","ARCHIVED"]'
    elif camp_status == "all":
        return '["ACTIVE","PAUSED","ARCHIVED"]'
    return '["ACTIVE"]'


try:
    from zoneinfo import ZoneInfo
    _BR_TZ = ZoneInfo("America/Sao_Paulo")
except Exception:
    _BR_TZ = timezone(timedelta(hours=-3))  # fallback sem tzdata


def _now_br():
    """Datetime corrente no fuso de Sao Paulo.
    Independe do fuso do servidor (producao pode estar em US/UTC).
    Essencial pra que "ontem" e o range default batam com o que o
    usuario brasileiro ve — e pra evitar inconsistencia entre caches
    populados em momentos com "data" diferente no servidor."""
    return datetime.now(_BR_TZ)


def _yesterday():
    """Retorna data de ontem no fuso BR — nunca usar dados do dia atual (incompletos)."""
    return (_now_br() - timedelta(days=1)).strftime("%Y-%m-%d")


def _default_date_from():
    return (_now_br() - timedelta(days=31)).strftime("%Y-%m-%d")


# Fields padrão para chamadas de /insights — inclui inline_link_clicks e cpc
INSIGHT_FIELDS_CAMPAIGN = (
    "campaign_id,campaign_name,spend,impressions,clicks,inline_link_clicks,"
    "reach,frequency,ctr,cpm,cpp,cpc,actions,action_values,purchase_roas,results,"
    + VIDEO_METRIC_FIELDS
)
INSIGHT_FIELDS_AD = (
    "ad_id,ad_name,spend,impressions,clicks,inline_link_clicks,"
    "reach,frequency,ctr,cpm,cpc,actions,action_values,purchase_roas,results,date_start,"
    + VIDEO_METRIC_FIELDS
)
INSIGHT_FIELDS_DAILY = (
    "spend,impressions,clicks,inline_link_clicks,reach,frequency,"
    "actions,action_values,purchase_roas,results,date_start,"
    + VIDEO_METRIC_FIELDS
)
INSIGHT_FIELDS_DAILY_CAMP = (
    "campaign_id,campaign_name,spend,impressions,clicks,inline_link_clicks,reach,frequency,"
    "actions,action_values,purchase_roas,results,date_start,"
    + VIDEO_METRIC_FIELDS
)


def _get_shared_daily_insights(camp_type, date_from, date_to, camp_status="all"):
    """Daily insights compartilhado entre api_daily_summary e api_multi_insights.

    Antes: cada endpoint fazia seu proprio fetch da Meta com fields parecidos.
    Em Crescimento 30d, 2 chamadas pesadas duplicadas por usuario que abre a aba.

    Agora: 1 fetch por (camp_type, camp_status, periodo). Cacheado 4h em SQLite.
    Ambos endpoints leem deste cache, reduzindo ~50% das calls Meta na primeira
    abertura do dia.

    Retorna (sales_campaigns, daily_rows).
    """
    cache_key = f"shared_daily_v4_{camp_type}_{camp_status}_{date_from}_{date_to}"
    cached = get_cached(cache_key)
    if cached is not None:
        return cached.get("campaigns", []), cached.get("rows", [])

    # DUAL-READ: se USE_ATOMS ativo, tenta atoms primeiro
    _sync_use_atoms_from_file()
    if USE_ATOMS:
        atom_result = _build_pseudo_daily_rows_from_atoms(camp_type, date_from, date_to, camp_status)
        if atom_result is not None:
            sales_campaigns, rows = atom_result
            set_cached(cache_key, {"campaigns": sales_campaigns, "rows": rows}, ttl_hours=4)
            return sales_campaigns, rows

    sales_campaigns = _fetch_type_campaigns(
        camp_type,
        "id,name,objective,daily_budget,lifetime_budget,start_time,created_time,status",
        _camp_status_filter(camp_status)
    )
    if not sales_campaigns:
        set_cached(cache_key, {"campaigns": [], "rows": []}, ttl_hours=4)
        return [], []

    rows = _fetch_insights_for_tagged_campaigns(
        sales_campaigns,
        base_params={
            "fields": INSIGHT_FIELDS_DAILY_CAMP,
            "time_range": json.dumps({"since": date_from, "until": date_to}),
            "time_increment": 1,
            "level": "campaign",
            "limit": 500,
        },
        extra_filters=[{"field": "impressions", "operator": "GREATER_THAN", "value": 0}]
    )

    set_cached(cache_key, {"campaigns": sales_campaigns, "rows": rows}, ttl_hours=4)
    return sales_campaigns, rows


# ── API Endpoints ──────────────────────────────────────────────────────

@app.route("/api/dashboard/campaigns")
@not_viewer_required
def api_campaigns():
    """Lista campanhas (vendas ou meteoricos) com métricas agregadas."""
    try:
        date_from = request.args.get("date_from", _default_date_from())
        date_to = request.args.get("date_to", _yesterday())
        camp_status = request.args.get("camp_status", "active")
        force = request.args.get("force", "false") == "true"
        camp_type = _camp_type_from_request()
        g.camp_type = camp_type

        blocked = _enforce_range_for_role(date_from, date_to)
        if blocked:
            return blocked

        # v3: attribution baseada em profile_visits (campo results). Bumpado pra
        # invalidar cache antigo que ainda usava link_click como proxy.
        cache_key = f"campaigns_v8_{camp_type}_{camp_status}_{date_from}_{date_to}"
        if not force:
            cached = get_cached(cache_key)
            if cached:
                return jsonify(cached)

        # 1. Buscar RAW (campanhas + insights) por conta.
        # DUAL-READ: se USE_ATOMS ativo, tenta atoms primeiro com parsing
        # per-day (sum-of-days); senao cai no campaigns_raw_v1.
        _sync_use_atoms_from_file()
        all_campaigns = []
        insights_map = {}
        for acc in _get_accounts_for_type(camp_type):
            if not acc:
                continue
            raw = None
            if USE_ATOMS:
                raw = _build_pseudo_raw_per_account_from_atoms(acc, date_from, date_to, camp_type=camp_type, camp_status=camp_status)
            if raw is not None and "atom_parsed_metrics_by_id" in raw:
                # ATOM PATH: metricas ja parsed e somadas per-day
                for c in (raw.get("campaigns") or []):
                    cc = dict(c); cc["_account_id"] = acc
                    all_campaigns.append(cc)
                for cid, parsed in raw.get("atom_parsed_metrics_by_id", {}).items():
                    if cid not in insights_map:
                        insights_map[cid] = parsed
            else:
                # LEGACY PATH: raw insights da Meta (range query)
                if raw is None:
                    raw = _fetch_account_raw_v1(acc, camp_status, date_from, date_to)
                for c in (raw.get("campaigns") or []):
                    cc = dict(c); cc["_account_id"] = acc
                    all_campaigns.append(cc)
                for cid, ins in (raw.get("insights_by_id") or {}).items():
                    if cid not in insights_map:
                        insights_map[cid] = parse_insights(ins, camp_type=camp_type)

        # 2. Filtrar por tipo (aplica classificacao ATUAL em cima do bruto)
        sales_campaigns = _filter_campaigns_by_type(all_campaigns, camp_type)

        if not sales_campaigns:
            return jsonify({"ok": True, "data": [], "summary": {}})

        # 2b. Crescimento: sobrescreve 'purchases' com seguidores atribuidos do IG.
        # Meta Marketing API nao expoe follow por campanha — usamos IG Graph API
        # e distribuimos o NET proporcional a VISITAS AO PERFIL (profile_visit)
        # de cada campanha, ponderando pelo gasto total vs outros tipos de campanha.
        if camp_type == CAMP_TYPE_CRESCIMENTO:
            try:
                # Usa TOTAL do periodo (daily breakdown nao funciona na IG API)
                ig_follower, ig_non = fetch_ig_follower_gain_total(IG_PROFILE_ID_JRM, date_from, date_to)
                ig_net_total = ig_follower - ig_non
                if ig_net_total > 0:
                    daily_rows = _fetch_insights_for_tagged_campaigns(
                        sales_campaigns,
                        base_params={
                            # actions + results: ambos sao fontes de visitas ao perfil
                            "fields": "campaign_id,date_start,actions,spend,results",
                            "time_range": json.dumps({"since": date_from, "until": date_to}),
                            "time_increment": 1,
                            "level": "campaign",
                            "limit": 500,
                        }
                    )
                    # Ponderacao: calcula share de Crescimento vs total da conta
                    crescimento_spend = sum(float(r.get("spend", 0) or 0) for r in daily_rows)
                    accounts = _get_accounts_for_type(camp_type)
                    total_account_spend = _fetch_total_spend_all_accounts(accounts, date_from, date_to)
                    other_spend = max(0, total_account_spend - crescimento_spend)
                    cresc_share = compute_crescimento_share(crescimento_spend, other_spend)
                    attribution = compute_crescimento_follower_attribution(
                        daily_rows, ig_net_total, crescimento_share=cresc_share
                    )
                    # Agrega por campanha e injeta em insights_map
                    total_by_camp = {}
                    for (cid, _d), v in attribution.items():
                        total_by_camp[cid] = total_by_camp.get(cid, 0) + v
                    for cid, total in total_by_camp.items():
                        if cid in insights_map:
                            insights_map[cid]["purchases"] = int(round(total))
                            # Recalcula CPS (usa campo cpa)
                            sp = insights_map[cid].get("spend", 0)
                            insights_map[cid]["cpa"] = round(sp / total, 2) if total > 0 else 0
            except Exception as e:
                print(f"[CRESCIMENTO] Falha atribuicao de seguidores: {e}")

        # 3. Montar resposta
        result = []
        total_spend = 0
        total_revenue = 0
        total_purchases = 0
        total_impressions = 0
        total_clicks = 0
        total_profile_visits = 0
        total_thruplay = 0
        total_video_plays = 0
        total_video_p25 = 0
        total_video_p50 = 0
        total_video_p75 = 0
        total_video_p95 = 0
        total_video_p100 = 0

        # Calcular dias ativos dentro do per&iacute;odo: max(start_time, date_from) at&eacute; date_to
        try:
            dt_from = datetime.strptime(date_from, "%Y-%m-%d")
            dt_to_obj = datetime.strptime(date_to, "%Y-%m-%d")
            period_total_days = max((dt_to_obj - dt_from).days + 1, 1)
        except Exception:
            dt_from = None
            dt_to_obj = None
            period_total_days = 1

        for c in sales_campaigns:
            metrics = insights_map.get(c["id"], {})
            if not metrics or metrics.get("impressions", 0) == 0:
                continue
            start_time_str = c.get("start_time", "") or c.get("created_time", "")
            active_days = period_total_days
            if start_time_str and dt_from and dt_to_obj:
                try:
                    camp_start = datetime.fromisoformat(start_time_str[:10])
                    effective_start = max(camp_start, dt_from)
                    if effective_start <= dt_to_obj:
                        active_days = (dt_to_obj - effective_start).days + 1
                    else:
                        active_days = 0
                except Exception:
                    pass

            entry = {
                "id": c["id"],
                "name": c.get("name", ""),
                "status": c.get("status", ""),
                "objective": c.get("objective", ""),
                "daily_budget": float(c.get("daily_budget", 0)) / 100 if c.get("daily_budget") else None,
                "lifetime_budget": float(c.get("lifetime_budget", 0)) / 100 if c.get("lifetime_budget") else None,
                "created_time": c.get("created_time", ""),
                "start_time": start_time_str,
                "active_days": active_days,
                "period_days": period_total_days,
                **metrics,
            }
            result.append(entry)
            total_spend += metrics.get("spend", 0)
            total_revenue += metrics.get("revenue", 0)
            total_purchases += metrics.get("purchases", 0)
            total_impressions += metrics.get("impressions", 0)
            total_clicks += metrics.get("clicks", 0)
            total_profile_visits += metrics.get("profile_visits", 0)
            total_thruplay += metrics.get("video_thruplay", 0)
            total_video_plays += metrics.get("video_plays", 0)
            total_video_p25 += metrics.get("video_p25", 0)
            total_video_p50 += metrics.get("video_p50", 0)
            total_video_p75 += metrics.get("video_p75", 0)
            total_video_p95 += metrics.get("video_p95", 0)
            total_video_p100 += metrics.get("video_p100", 0)

        # Ordenar por gasto (maior primeiro)
        result.sort(key=lambda x: x.get("spend", 0), reverse=True)

        summary = {
            # Conta apenas campanhas com IMPRESSAO no periodo (result),
            # nao TODAS as campanhas do tipo (sales_campaigns inclui paused/archived
            # sem atividade no periodo). Isso reflete o que Meta UI mostra.
            "total_campaigns": len(result),
            "total_spend": round(total_spend, 2),
            "total_revenue": round(total_revenue, 2),
            "total_purchases": total_purchases,
            "avg_roas": round(total_revenue / total_spend, 2) if total_spend > 0 else 0,
            "avg_cpa": round(total_spend / total_purchases, 2) if total_purchases > 0 else 0,
            "avg_cpm": round((total_spend / total_impressions) * 1000, 2) if total_impressions > 0 else 0,
            "avg_ctr": round((total_clicks / total_impressions) * 100, 2) if total_impressions > 0 else 0,
            "total_impressions": total_impressions,
            "total_clicks": total_clicks,
            "total_profile_visits": total_profile_visits,
            "total_thruplay": total_thruplay,
            "total_video_plays": total_video_plays,
            "total_video_p25": total_video_p25,
            "total_video_p50": total_video_p50,
            "total_video_p75": total_video_p75,
            "total_video_p95": total_video_p95,
            "total_video_p100": total_video_p100,
        }

        # Agrupar por evento
        events = group_campaigns_by_event(sales_campaigns)
        # Mapear campaign_id -> event_id/event_name
        camp_event_map = {}
        for ev in events:
            for cid in ev["campaign_ids"]:
                camp_event_map[cid] = {"event_id": ev["event_id"], "event_name": ev["event_name"]}
        # Adicionar evento em cada campanha do resultado
        for c in result:
            ev_info = camp_event_map.get(c["id"], {})
            c["event_id"] = ev_info.get("event_id", "OUTROS")
            c["event_name"] = ev_info.get("event_name", "Outros")

        # Mapear status das campanhas no resultado (com insights)
        camp_status_map = {c["id"]: c.get("status", "PAUSED") for c in result}

        # Limpar events para o retorno (sem objetos campanha completos)
        events_summary = []
        for ev in events:
            statuses = [camp_status_map.get(cid, "PAUSED") for cid in ev["campaign_ids"]]
            ev_status = "ACTIVE" if any(s == "ACTIVE" for s in statuses) else "PAUSED"
            events_summary.append({
                "event_id": ev["event_id"],
                "event_name": ev["event_name"],
                "event_type": ev["event_type"],
                "event_type_name": ev["event_type_name"],
                "city": ev["city"],
                "campaign_ids": ev["campaign_ids"],
                "campaign_count": ev["campaign_count"],
                "date_range": ev["date_range"],
                "status": ev_status,
            })

        response = {"ok": True, "data": result, "summary": summary, "events": events_summary}
        set_cached(cache_key, response, ttl_hours=_cache_ttl_for_range(date_from, date_to))
        return jsonify(response)

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/dashboard/campaigns/<campaign_id>/insights")
@not_viewer_required
def api_campaign_insights(campaign_id):
    """Insights diários de uma campanha para gráficos temporais."""
    try:
        date_from = request.args.get("date_from", _default_date_from())
        date_to = request.args.get("date_to", _yesterday())
        g.camp_type = _camp_type_from_request()

        blocked = _enforce_range_for_role(date_from, date_to)
        if blocked:
            return blocked

        rows = meta_get_all_pages(
            f"{campaign_id}/insights",
            {
                "fields": INSIGHT_FIELDS_DAILY,
                "time_range": json.dumps({"since": date_from, "until": date_to}),
                "time_increment": 1,
                "limit": 500,
            }
        )

        daily = []
        for row in rows:
            parsed = parse_insights(row)
            parsed["date"] = row.get("date_start", "")
            daily.append(parsed)

        daily.sort(key=lambda x: x["date"])
        return jsonify({"ok": True, "data": daily})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/dashboard/campaigns/multi-insights")
@not_viewer_required
def api_campaigns_multi_insights():
    """Insights diários por campanha para várias campanhas. ?ids=id1,id2,id3 ou ?ids=all."""
    try:
        date_from = request.args.get("date_from", _default_date_from())
        date_to = request.args.get("date_to", _yesterday())
        camp_type = _camp_type_from_request()
        g.camp_type = camp_type

        blocked = _enforce_range_for_role(date_from, date_to)
        if blocked:
            return blocked
        ids_param = request.args.get("ids", "all")
        camp_status = request.args.get("camp_status", "all")
        force = request.args.get("force", "false") == "true"

        # v5: filtra campanhas sem dados quando ids=all (nao polui o seletor
        # de Projecao com campanhas antigas arquivadas sem impressoes).
        cache_key = f"multi_insights_v9_{camp_type}_{ids_param}_{camp_status}_{date_from}_{date_to}"
        if not force:
            cached = get_cached(cache_key)
            if cached:
                return jsonify(cached)

        # Usa daily insights compartilhado — mesma fonte que api_daily_summary,
        # evitando duplicar fetch pesado com time_increment=1 quando o mesmo
        # usuario abre a aba Campanhas (ambos sao chamados no load).
        shared_camps, shared_rows = _get_shared_daily_insights(camp_type, date_from, date_to, camp_status)
        sales_map = {c["id"]: c for c in shared_camps}

        if ids_param == "all":
            target_ids = list(sales_map.keys())
            rows = shared_rows
        else:
            # IDs especificos: filtra rows do shared pra apenas esses ids
            target_ids = [i.strip() for i in ids_param.split(",") if i.strip()]
            # Completa sales_map com IDs que nao estejam no cache de tipo
            # (campanhas que nao sao do tipo atual mas foram solicitadas)
            missing = [cid for cid in target_ids if cid not in sales_map]
            if missing:
                for acc in _get_accounts_for_type(camp_type):
                    try:
                        extra = meta_get_all_pages(
                            f"{acc}/campaigns",
                            {"fields": "id,name,objective", "effective_status": '["ACTIVE","PAUSED"]'}
                        )
                        for c in extra:
                            if c["id"] in missing and c["id"] not in sales_map:
                                c["_account_id"] = acc
                                sales_map[c["id"]] = c
                    except Exception as e:
                        print(f"[MULTI-ACCT] Falha multi-insights {acc}: {e}")
            rows = [r for r in shared_rows if r.get("campaign_id") in set(target_ids)]

        if not target_ids:
            return jsonify({"ok": True, "campaigns": []})

        # Monta lista de campanhas taggueadas (ainda util pra query auxiliar de video em Nutricao)
        tagged_camps = []
        for cid in target_ids:
            c = sales_map.get(cid, {"id": cid})
            if not c.get("_account_id"):
                c = dict(c)
                c["_account_id"] = ACCOUNT_ID
            c.setdefault("id", cid)
            tagged_camps.append(c)

        # Video metrics (Nutricao): Meta NAO retorna video_* com time_increment=1
        # em level=campaign (campos ficam vazios nas rows diarias). Pra detail
        # cards/funil de Nutricao funcionar, fazemos uma query separada agregada
        # (sem time_increment) e injetamos nos dailies como se fosse do ultimo dia.
        # O frontend soma por campanha e o total fica correto.
        if camp_type == CAMP_TYPE_NUTRICAO and rows:
            try:
                video_rows = _fetch_insights_for_tagged_campaigns(
                    tagged_camps,
                    base_params={
                        "fields": "campaign_id," + VIDEO_METRIC_FIELDS,
                        "time_range": json.dumps({"since": date_from, "until": date_to}),
                        "level": "campaign",
                        "limit": 500,
                    },
                    extra_filters=[{"field": "impressions", "operator": "GREATER_THAN", "value": 0}]
                )
                video_by_cid = {}
                for vr in video_rows:
                    cid = vr.get("campaign_id")
                    if cid:
                        video_by_cid[cid] = {
                            "video_plays": _extract_video_metric(vr, "video_play_actions"),
                            "video_p25": _extract_video_metric(vr, "video_p25_watched_actions"),
                            "video_p50": _extract_video_metric(vr, "video_p50_watched_actions"),
                            "video_p75": _extract_video_metric(vr, "video_p75_watched_actions"),
                            "video_p95": _extract_video_metric(vr, "video_p95_watched_actions"),
                            "video_p100": _extract_video_metric(vr, "video_p100_watched_actions"),
                            "video_thruplay": _extract_video_metric(vr, "video_thruplay_watched_actions"),
                        }
                # Pega o maior date_start por campanha pra injetar os totais naquele dia.
                # (Frontend soma todos os dias, entao totais vao pra apenas 1 dia evita
                # dobrar valores se Meta em algum momento comecar a devolver por dia.)
                last_date_by_cid = {}
                for r in rows:
                    cid = r.get("campaign_id")
                    d = r.get("date_start", "")
                    if cid and d and d > last_date_by_cid.get(cid, ""):
                        last_date_by_cid[cid] = d
                for r in rows:
                    cid = r.get("campaign_id")
                    d = r.get("date_start", "")
                    if cid in video_by_cid and d == last_date_by_cid.get(cid):
                        vdata = video_by_cid[cid]
                        # Injeta como lista de actions (formato esperado por parse_insights)
                        def _wrap(n):
                            return [{"action_type": "video_view", "value": str(n)}] if n else []
                        r["video_play_actions"] = _wrap(vdata["video_plays"])
                        r["video_p25_watched_actions"] = _wrap(vdata["video_p25"])
                        r["video_p50_watched_actions"] = _wrap(vdata["video_p50"])
                        r["video_p75_watched_actions"] = _wrap(vdata["video_p75"])
                        r["video_p95_watched_actions"] = _wrap(vdata["video_p95"])
                        r["video_p100_watched_actions"] = _wrap(vdata["video_p100"])
                        r["video_thruplay_watched_actions"] = _wrap(vdata["video_thruplay"])
            except Exception as e:
                print(f"[NUTRICAO multi-insights video] Falha: {e}")

        # Crescimento: calcula atribuicao de seguidores (IG API + link_click + share)
        crescimento_attr = {}
        if camp_type == CAMP_TYPE_CRESCIMENTO:
            try:
                ig_follower, ig_non = fetch_ig_follower_gain_total(IG_PROFILE_ID_JRM, date_from, date_to)
                ig_net_total = ig_follower - ig_non
                if ig_net_total > 0:
                    cresc_spend = sum(float(r.get("spend", 0) or 0) for r in rows)
                    accounts = _get_accounts_for_type(camp_type)
                    total_spend = _fetch_total_spend_all_accounts(accounts, date_from, date_to)
                    other_spend = max(0, total_spend - cresc_spend)
                    cresc_share = compute_crescimento_share(cresc_spend, other_spend)
                    crescimento_attr = compute_crescimento_follower_attribution(
                        rows, ig_net_total, crescimento_share=cresc_share
                    )
            except Exception as e:
                print(f"[CRESCIMENTO multi-insights] Falha: {e}")

        # Agrupar por campaign_id
        by_camp = {}
        for row in rows:
            cid = row.get("campaign_id")
            if not cid:
                continue
            if cid not in by_camp:
                by_camp[cid] = {
                    "id": cid,
                    "name": row.get("campaign_name") or sales_map.get(cid, {}).get("name", cid),
                    "daily": [],
                }
            parsed = parse_insights(row)
            parsed["date"] = row.get("date_start", "")
            # Sobrescreve purchases com seguidores atribuidos do dia (crescimento).
            # Mantem como FLOAT (arredondado so na exibicao) — arredondar por dia
            # a valores pequenos (<1) zera e soma total fica menor que o real.
            if crescimento_attr:
                attr_val = crescimento_attr.get((cid, parsed["date"]), 0)
                parsed["purchases"] = round(attr_val, 2)
                if parsed.get("spend", 0) > 0 and parsed["purchases"] > 0:
                    parsed["cpa"] = round(parsed["spend"] / parsed["purchases"], 2)
                else:
                    parsed["cpa"] = 0
            by_camp[cid]["daily"].append(parsed)

        # Ordenar daily por data
        result = []
        # Quando ids=all (aba Projecao/lista completa): so inclui campanhas com dados.
        # Quando ids sao especificos (usuario clicou campanhas na tabela): mantem todas
        # pra mostrar mesmo que zeradas — o usuario escolheu explicitamente.
        include_empty = ids_param != "all"
        for cid in target_ids:
            if cid in by_camp:
                entry = by_camp[cid]
            elif include_empty:
                entry = {"id": cid, "name": sales_map.get(cid, {}).get("name", cid), "daily": []}
            else:
                continue
            entry["daily"].sort(key=lambda x: x["date"])
            result.append(entry)

        # Reach/frequency agregados: precisam ser por conta (Meta API nao agrega entre contas).
        # Se ha campanhas em multiplas contas, pega a conta com mais campanhas (aproximacao razoavel).
        agg_totals = {"reach": 0, "frequency": 0}
        try:
            by_acc_ids = {}
            for c in tagged_camps:
                by_acc_ids.setdefault(c.get("_account_id") or ACCOUNT_ID, []).append(c["id"])
            main_acc = max(by_acc_ids, key=lambda k: len(by_acc_ids[k]))
            agg_rows = meta_get_all_pages(
                f"{main_acc}/insights",
                {
                    "fields": "reach,frequency,impressions",
                    "time_range": json.dumps({"since": date_from, "until": date_to}),
                    "level": "account",
                    "filtering": json.dumps([{"field": "campaign.id", "operator": "IN", "value": by_acc_ids[main_acc]}]),
                }
            )
            if agg_rows:
                agg_totals["reach"] = int(agg_rows[0].get("reach", 0))
                agg_totals["frequency"] = round(float(agg_rows[0].get("frequency", 0)), 2)
        except Exception as e:
            print(f"[WARN] reach/frequency agregados falhou: {e}")

        response = {"ok": True, "campaigns": result, "aggregated": agg_totals}
        set_cached(cache_key, response, ttl_hours=_cache_ttl_for_range(date_from, date_to))
        return jsonify(response)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


def _aggregate_daily_to_window(daily_rows, last_n_days, date_to):
    """Agrega rows diários nos últimos N dias antes de date_to (inclusive)."""
    dt_to = datetime.strptime(date_to, "%Y-%m-%d")
    cutoff = (dt_to - timedelta(days=last_n_days - 1)).strftime("%Y-%m-%d")

    spend = 0.0
    revenue = 0.0
    purchases = 0
    for row in daily_rows:
        d = row.get("date_start", "")
        if d < cutoff or d > date_to:
            continue
        m = parse_insights(row)
        spend += m.get("spend", 0)
        revenue += m.get("revenue", 0)
        purchases += m.get("purchases", 0)

    roas = revenue / spend if spend > 0 else 0
    return {"spend": round(spend, 2), "revenue": round(revenue, 2), "purchases": purchases, "roas": round(roas, 2)}


def _daily_spend_map(daily_rows):
    """Transforma rows diários em dict {date: spend}."""
    m = {}
    for row in daily_rows:
        d = row.get("date_start", "")
        if d:
            m[d] = m.get(d, 0) + float(row.get("spend", 0))
    return m


def _compute_advanced_metrics(ads_by_campaign, daily_by_ad, date_from, date_to):
    """Calcula velocity, trend, confidence, score + dados 3d e 7d a partir de dados diários.

    Share (participação) é calculado corretamente dia-a-dia:
      share = SUM(ad_spend[d]) / SUM(camp_spend[d])  onde d ∈ dias em que o ad teve gasto
    Isso garante que share ∈ [0%, 100%] sempre, nunca ultrapassa.
    """
    dt_to = datetime.strptime(date_to, "%Y-%m-%d")
    total_period_days = max((dt_to - datetime.strptime(date_from, "%Y-%m-%d")).days + 1, 1)

    for camp_id, camp_data in ads_by_campaign.items():
        ads = camp_data["ads"]

        # Mapas spend por ad e por dia {date: spend}
        ad_spend_by_day = {}  # ad_id -> {date: spend}
        for ad in ads:
            ad_id = ad.get("ad_id", "")
            ad_spend_by_day[ad_id] = _daily_spend_map(daily_by_ad.get(ad_id, []))

        # Gasto total da campanha por dia (soma de todos os ads)
        camp_spend_by_day = {}
        for ad_id, m in ad_spend_by_day.items():
            for d, v in m.items():
                camp_spend_by_day[d] = camp_spend_by_day.get(d, 0) + v

        # Calcular 3d e 7d (agregados da janela)
        map_3d = {}
        map_7d = {}
        for ad in ads:
            ad_id = ad.get("ad_id", "")
            daily = daily_by_ad.get(ad_id, [])
            map_3d[ad_id] = _aggregate_daily_to_window(daily, 3, date_to)
            map_7d[ad_id] = _aggregate_daily_to_window(daily, 7, date_to)

        # Data cutoffs
        dt_end = datetime.strptime(date_to, "%Y-%m-%d")
        cutoff_3d = (dt_end - timedelta(days=2)).strftime("%Y-%m-%d")
        cutoff_7d = (dt_end - timedelta(days=6)).strftime("%Y-%m-%d")

        for a in ads:
            days = a.get("days_active", 0)
            ad_id = a.get("ad_id", "")
            r3 = map_3d.get(ad_id, {})
            r7 = map_7d.get(ad_id, {})
            ad_spend_map = ad_spend_by_day.get(ad_id, {})

            # 3d / 7d data
            a["spend_3d"] = round(r3.get("spend", 0), 2)
            a["roas_3d"] = round(r3.get("roas", 0), 2)
            a["purchases_3d"] = r3.get("purchases", 0)
            a["spend_7d"] = round(r7.get("spend", 0), 2)
            a["roas_7d"] = round(r7.get("roas", 0), 2)
            a["purchases_7d"] = r7.get("purchases", 0)
            # Video (Nutricao): ThruPlays por janela
            a["video_thruplay_3d"] = r3.get("video_thruplay", 0)
            a["video_thruplay_7d"] = r7.get("video_thruplay", 0)
            a["video_plays_3d"] = r3.get("video_plays", 0)
            a["video_plays_7d"] = r7.get("video_plays", 0)

            # ── SHARE TOTAL (dia a dia, só nos dias em que o ad teve gasto) ──
            # ∑ ad_spend[d] / ∑ camp_spend[d] onde d = dias com ad_spend > 0
            active_days = [d for d, v in ad_spend_map.items() if v > 0]
            ad_total_on_active = sum(ad_spend_map.get(d, 0) for d in active_days)
            camp_total_on_active = sum(camp_spend_by_day.get(d, 0) for d in active_days)
            share_total = (ad_total_on_active / camp_total_on_active * 100) if camp_total_on_active > 0 else 0

            # ── SHARE 3D ──
            days_3d = [d for d in active_days if d >= cutoff_3d]
            ad_3d = sum(ad_spend_map.get(d, 0) for d in days_3d)
            camp_3d = sum(camp_spend_by_day.get(d, 0) for d in days_3d)
            share_3d = (ad_3d / camp_3d * 100) if camp_3d > 0 else 0

            # ── SHARE 7D ──
            days_7d = [d for d in active_days if d >= cutoff_7d]
            ad_7d = sum(ad_spend_map.get(d, 0) for d in days_7d)
            camp_7d = sum(camp_spend_by_day.get(d, 0) for d in days_7d)
            share_7d = (ad_7d / camp_7d * 100) if camp_7d > 0 else 0

            # Clamp entre 0 e 100 (seguran&ccedil;a extra)
            share_total = max(0, min(100, share_total))
            share_3d = max(0, min(100, share_3d))
            share_7d = max(0, min(100, share_7d))

            a["share_total"] = round(share_total, 1)
            a["share_3d"] = round(share_3d, 1)
            a["share_7d"] = round(share_7d, 1)

            # Velocity
            if days <= 2:
                velocity = 0
                a["velocity_note"] = "novo"
            elif days <= 9:
                if share_total > 0:
                    velocity = round(((share_3d - share_total) / share_total) * 100)
                    velocity = max(min(velocity, 100), -100)
                else:
                    velocity = 0
                a["velocity_note"] = "recente"
            else:
                velocity = round(((share_3d - share_total) / share_total) * 100) if share_total > 0 else 0
                a["velocity_note"] = "maduro"
            a["velocity"] = velocity

            # Trend
            if days <= 3:
                a["trend"] = "estavel"
            elif velocity >= 30:
                a["trend"] = "escalando"
            elif velocity <= -30:
                a["trend"] = "caindo"
            else:
                a["trend"] = "estavel"

            # ── DESEMPENHO (relativo à campanha) ──
            # Placeholder — será calculado no segundo passo abaixo
            a["confidence"] = "baixo"

        # Segundo passo: calcular desempenho com contexto da campanha inteira
        camp_total_spend = sum(ad.get("spend", 0) for ad in ads)
        camp_total_rev = sum(ad.get("revenue", 0) for ad in ads)
        camp_roas = camp_total_rev / camp_total_spend if camp_total_spend > 0 else 0

        # Top 1 por share_total e top 3 por share_7d
        sorted_by_share_total = sorted(ads, key=lambda x: x.get("share_total", 0), reverse=True)
        top1_total_id = sorted_by_share_total[0].get("ad_id", "") if sorted_by_share_total else ""
        sorted_by_share7d = sorted(ads, key=lambda x: x.get("share_7d", 0), reverse=True)
        top3_7d_ids = set(x.get("ad_id", "") for x in sorted_by_share7d[:3])
        top1_7d_id = sorted_by_share7d[0].get("ad_id", "") if sorted_by_share7d else ""

        # Em Nutricao a 'conversao' principal e ThruPlay (nao compra).
        # Usamos video_thruplay como volume pra score/confidence.
        is_nutricao_camp = (g.get("camp_type") == CAMP_TYPE_NUTRICAO) if hasattr(g, "get") else False
        try:
            from flask import has_request_context as _hrc
            if _hrc():
                is_nutricao_camp = getattr(g, "camp_type", None) == CAMP_TYPE_NUTRICAO
        except Exception:
            pass

        for a in ads:
            conv = (a.get("video_thruplay", 0) if is_nutricao_camp else a.get("purchases", 0))
            roas = a.get("roas", 0)
            spend = a.get("spend", 0)
            days = a.get("days_active", 0)
            velocity = a.get("velocity", 0)
            roas_ratio = roas / camp_roas if camp_roas > 0 else 0
            ad_id = a.get("ad_id", "")
            is_top3_7d = ad_id in top3_7d_ids
            is_top1_total = ad_id == top1_total_id and ad_id == top1_7d_id

            if conv == 0:
                if days <= 7:
                    a["confidence"] = "baixo"
                else:
                    a["confidence"] = "ruim"

            elif is_top1_total and conv >= 1:
                a["confidence"] = "alto"

            elif is_top3_7d and roas_ratio >= 0.9:
                a["confidence"] = "alto"

            elif roas_ratio >= 0.7 and conv >= 3:
                a["confidence"] = "medio"

            elif roas_ratio < 0.5 and days > 7 and spend >= 200:
                a["confidence"] = "ruim"

            elif velocity < -30 and days > 7:
                a["confidence"] = "declinando"

            elif conv >= 1:
                a["confidence"] = "tendencia"

            else:
                a["confidence"] = "baixo"

            # ── CONFIANÇA DOS DADOS ──
            # Para resultado POSITIVO (tem conversões): precisa de volume alto
            # Para resultado NEGATIVO (sem conversões): tempo ativo já basta
            #
            # Alta positiva: muito gasto + muitas impressões + tempo
            # Alta negativa: muitos dias ativos (mesmo com pouco gasto, se não vendeu em 15+ dias é confiável que é ruim)
            # Média: dados moderados
            # Baixa: criativo novo com poucos dados
            impr = a.get("impressions", 0)
            has_sales = conv > 0

            if days >= 14 or spend >= 2000:
                a["data_confidence"] = "alta"
            elif days >= 7 or spend >= 500:
                a["data_confidence"] = "media"
            else:
                a["data_confidence"] = "baixa"

            # Score composto — agora dá peso direto à participação no gasto da campanha.
            # Lógica: a Meta otimiza investimento para criativos com mais potencial.
            # Se share_total é alto, é sinal forte de que a Meta confia neste criativo.
            #
            # Componentes:
            #   volume          → número de compras (mais é melhor)
            #   roas_factor     → max(roas, 0.5) → não zera tudo se ROAS for ruim
            #   share_factor    → 1 + (share_total/100)*3 → 0% = 1x, 33% = 2x, 100% = 4x
            #   spend_factor    → log(spend) → reliability (mais gasto = mais confiança nos números)
            #   trend_boost     → 1.3 escalando, 0.7 caindo, 1.0 estável
            volume = conv
            roas_factor = max(roas, 0.5)
            share_factor = 1 + (a.get("share_total", 0) / 100.0) * 3
            spend_factor = math.log(max(spend, 1) + 1)
            trend_boost = 1.3 if a["trend"] == "escalando" else (0.7 if a["trend"] == "caindo" else 1.0)
            a["score"] = round(volume * roas_factor * share_factor * spend_factor * trend_boost, 2)

            # Frequency: usar valor j&aacute; presente (vem da API via parse_insights)
            if not a.get("frequency"):
                impressions = a.get("impressions", 0)
                reach = a.get("reach", 0)
                a["frequency"] = round(impressions / reach, 2) if reach > 0 else 0


def _aggregate_daily_total(daily_rows):
    """Soma todos os dados diários em métricas totais."""
    spend = 0.0
    revenue = 0.0
    purchases = 0
    impressions = 0
    clicks = 0
    max_reach = 0  # Reach: usar o maior diário (lower bound, não somar)
    freq_weighted_sum = 0.0  # Frequency: média ponderada por impressões
    link_clicks = 0
    lpv = 0
    profile_visits = 0
    view_content = 0
    add_to_cart = 0
    initiate_checkout = 0
    video_plays = 0
    video_p25 = 0
    video_p50 = 0
    video_p75 = 0
    video_p95 = 0
    video_p100 = 0
    video_thruplay = 0
    for row in daily_rows:
        m = parse_insights(row)
        spend += m.get("spend", 0)
        revenue += m.get("revenue", 0)
        purchases += m.get("purchases", 0)
        day_impr = m.get("impressions", 0)
        impressions += day_impr
        clicks += m.get("clicks", 0)
        day_reach = m.get("reach", 0)
        if day_reach > max_reach:
            max_reach = day_reach
        day_freq = m.get("frequency", 0)
        if day_freq > 0 and day_impr > 0:
            freq_weighted_sum += day_freq * day_impr
        link_clicks += m.get("link_clicks", 0)
        lpv += m.get("lpv", 0)
        profile_visits += m.get("profile_visits", 0)
        view_content += m.get("view_content", 0)
        add_to_cart += m.get("add_to_cart", 0)
        initiate_checkout += m.get("initiate_checkout", 0)
        video_plays += m.get("video_plays", 0)
        video_p25 += m.get("video_p25", 0)
        video_p50 += m.get("video_p50", 0)
        video_p75 += m.get("video_p75", 0)
        video_p95 += m.get("video_p95", 0)
        video_p100 += m.get("video_p100", 0)
        video_thruplay += m.get("video_thruplay", 0)

    roas = revenue / spend if spend > 0 else 0
    cpa = spend / purchases if purchases > 0 else 0
    ctr = (clicks / impressions * 100) if impressions > 0 else 0
    cpm = (spend / impressions * 1000) if impressions > 0 else 0
    frequency = freq_weighted_sum / impressions if impressions > 0 else 0
    cost_per_link_click = spend / link_clicks if link_clicks > 0 else 0
    rate_click_lpv = (lpv / link_clicks * 100) if link_clicks > 0 else 0
    rate_lpv_ic = (initiate_checkout / lpv * 100) if lpv > 0 else 0
    rate_ic_purchase = (purchases / initiate_checkout * 100) if initiate_checkout > 0 else 0
    rate_click_purchase = (purchases / link_clicks * 100) if link_clicks > 0 else 0
    cost_per_ic = spend / initiate_checkout if initiate_checkout > 0 else 0
    cost_per_lpv = spend / lpv if lpv > 0 else 0
    cost_per_profile_visit = spend / profile_visits if profile_visits > 0 else 0
    cost_per_thruplay = spend / video_thruplay if video_thruplay > 0 else 0
    cost_per_video_play = spend / video_plays if video_plays > 0 else 0
    rate_play_thruplay = (video_thruplay / video_plays * 100) if video_plays > 0 else 0
    rate_play_p25 = (video_p25 / video_plays * 100) if video_plays > 0 else 0
    rate_play_p100 = (video_p100 / video_plays * 100) if video_plays > 0 else 0

    return {
        "spend": round(spend, 2), "impressions": impressions, "clicks": clicks,
        "reach": max_reach, "frequency": round(frequency, 2),
        "purchases": purchases, "revenue": round(revenue, 2), "roas": round(roas, 2),
        "cpa": round(cpa, 2), "ctr": round(ctr, 2), "cpm": round(cpm, 2),
        "link_clicks": link_clicks, "cost_per_link_click": round(cost_per_link_click, 2),
        "lpv": lpv, "view_content": view_content, "add_to_cart": add_to_cart,
        "initiate_checkout": initiate_checkout,
        "profile_visits": profile_visits,
        "cost_per_profile_visit": round(cost_per_profile_visit, 2),
        "rate_click_lpv": round(rate_click_lpv, 2),
        "rate_lpv_ic": round(rate_lpv_ic, 2),
        "rate_ic_purchase": round(rate_ic_purchase, 2),
        "rate_click_purchase": round(rate_click_purchase, 2),
        "cost_per_ic": round(cost_per_ic, 2),
        "cost_per_lpv": round(cost_per_lpv, 2),
        # Video (Nutricao)
        "video_plays": video_plays,
        "video_p25": video_p25,
        "video_p50": video_p50,
        "video_p75": video_p75,
        "video_p95": video_p95,
        "video_p100": video_p100,
        "video_thruplay": video_thruplay,
        "cost_per_thruplay": round(cost_per_thruplay, 2),
        "cost_per_video_play": round(cost_per_video_play, 2),
        "rate_play_thruplay": round(rate_play_thruplay, 2),
        "rate_play_p25": round(rate_play_p25, 2),
        "rate_play_p100": round(rate_play_p100, 2),
    }


def _compute_advanced_metrics_from_aggregates(ads_by_campaign, totals_by_ad, totals_7d_by_ad, totals_3d_by_ad, date_from, date_to):
    """Calcula share, velocity, trend, confidence e score a partir de metricas
    agregadas por janela (total, 7d, 3d) — sem precisar de daily por ad.

    Share (total): spend_ad / spend_campanha_total
    Share (7d): spend_ad_7d / spend_campanha_7d
    Share (3d): spend_ad_3d / spend_campanha_3d
    Velocity: (share_3d - share_total) / share_total
    Trend: escalando / caindo / estavel (baseado em velocity)
    Score: volume * roas_factor * share_factor * spend_factor * trend_boost
    """
    is_nutricao_camp = False
    try:
        from flask import has_request_context as _hrc
        if _hrc():
            is_nutricao_camp = getattr(g, "camp_type", None) == CAMP_TYPE_NUTRICAO
    except Exception:
        pass

    for camp_id, camp_data in ads_by_campaign.items():
        ads = camp_data["ads"]
        # Totais de campanha (soma dos ads)
        camp_total_spend = sum(totals_by_ad.get(a["ad_id"], {}).get("spend", 0) for a in ads)
        camp_7d_spend = sum(totals_7d_by_ad.get(a["ad_id"], {}).get("spend", 0) for a in ads)
        camp_3d_spend = sum(totals_3d_by_ad.get(a["ad_id"], {}).get("spend", 0) for a in ads)
        camp_total_rev = sum(totals_by_ad.get(a["ad_id"], {}).get("revenue", 0) for a in ads)
        camp_roas = camp_total_rev / camp_total_spend if camp_total_spend > 0 else 0

        # Top por share pra confidence
        share_total_by_ad = {}
        share_7d_by_ad = {}
        for ad in ads:
            aid = ad["ad_id"]
            ad_spend = totals_by_ad.get(aid, {}).get("spend", 0)
            ad_7d_spend = totals_7d_by_ad.get(aid, {}).get("spend", 0)
            share_total_by_ad[aid] = (ad_spend / camp_total_spend * 100) if camp_total_spend > 0 else 0
            share_7d_by_ad[aid] = (ad_7d_spend / camp_7d_spend * 100) if camp_7d_spend > 0 else 0
        top1_total_id = max(share_total_by_ad, key=share_total_by_ad.get, default="")
        top3_7d_ids = set(sorted(share_7d_by_ad, key=share_7d_by_ad.get, reverse=True)[:3])
        top1_7d_id = max(share_7d_by_ad, key=share_7d_by_ad.get, default="")

        for a in ads:
            aid = a["ad_id"]
            r_total = totals_by_ad.get(aid, {})
            r_7d = totals_7d_by_ad.get(aid, {})
            r_3d = totals_3d_by_ad.get(aid, {})
            days = a.get("days_active", 0)

            # 3d / 7d data
            a["spend_3d"] = round(r_3d.get("spend", 0), 2)
            a["roas_3d"] = round(r_3d.get("roas", 0), 2)
            a["purchases_3d"] = r_3d.get("purchases", 0)
            a["spend_7d"] = round(r_7d.get("spend", 0), 2)
            a["roas_7d"] = round(r_7d.get("roas", 0), 2)
            a["purchases_7d"] = r_7d.get("purchases", 0)
            a["video_thruplay_3d"] = r_3d.get("video_thruplay", 0)
            a["video_thruplay_7d"] = r_7d.get("video_thruplay", 0)
            a["video_plays_3d"] = r_3d.get("video_plays", 0)
            a["video_plays_7d"] = r_7d.get("video_plays", 0)
            # Crescimento: Visitas ao Perfil por janela
            a["profile_visits_3d"] = r_3d.get("profile_visits", 0)
            a["profile_visits_7d"] = r_7d.get("profile_visits", 0)

            # Share (percentual do gasto da campanha)
            share_total = share_total_by_ad.get(aid, 0)
            ad_3d_spend = r_3d.get("spend", 0)
            share_3d = (ad_3d_spend / camp_3d_spend * 100) if camp_3d_spend > 0 else 0
            share_7d = share_7d_by_ad.get(aid, 0)
            a["share_total"] = round(share_total, 1)
            a["share_7d"] = round(share_7d, 1)
            a["share_3d"] = round(share_3d, 1)

            # Velocity: variacao % entre share_3d e share_total
            if share_total > 0:
                velocity = round(((share_3d - share_total) / share_total) * 100)
            else:
                velocity = 0
            a["velocity"] = velocity
            a["velocity_note"] = "novo" if days <= 3 else "maduro"

            # Trend
            if days <= 3:
                a["trend"] = "estavel"
            elif velocity >= 30:
                a["trend"] = "escalando"
            elif velocity <= -30:
                a["trend"] = "caindo"
            else:
                a["trend"] = "estavel"

            # Confidence (desempenho relativo a campanha).
            # - Nutricao: ThruPlays como volume
            # - Crescimento: Visitas ao Perfil (nao seguidores — esses sao atribuicao)
            # - Outros: purchases (compras/leads nativos)
            is_crescimento_camp = False
            try:
                from flask import has_request_context as _hrc
                if _hrc():
                    is_crescimento_camp = getattr(g, "camp_type", None) == CAMP_TYPE_CRESCIMENTO
            except Exception:
                pass
            if is_nutricao_camp:
                conv = a.get("video_thruplay", 0)
            elif is_crescimento_camp:
                conv = a.get("profile_visits", 0)
            else:
                conv = a.get("purchases", 0)
            roas = a.get("roas", 0)
            spend = a.get("spend", 0)
            roas_ratio = roas / camp_roas if camp_roas > 0 else 0
            is_top3_7d = aid in top3_7d_ids
            is_top1_total = aid == top1_total_id and aid == top1_7d_id
            if conv == 0:
                a["confidence"] = "baixo" if days <= 7 else "ruim"
            elif is_top1_total and conv >= 1:
                a["confidence"] = "alto"
            elif is_top3_7d and roas_ratio >= 0.9:
                a["confidence"] = "alto"
            elif roas_ratio >= 0.7 and conv >= 3:
                a["confidence"] = "medio"
            elif roas_ratio < 0.5 and spend > 200:
                a["confidence"] = "ruim"
            elif velocity <= -30 and days > 7:
                a["confidence"] = "declinando"
            elif conv >= 1:
                a["confidence"] = "tendencia"
            else:
                a["confidence"] = "baixo"

            # Data confidence
            if days >= 14 or spend >= 2000:
                a["data_confidence"] = "alta"
            elif days >= 7 or spend >= 500:
                a["data_confidence"] = "media"
            else:
                a["data_confidence"] = "baixa"

            # Score
            volume = conv
            roas_factor = max(roas, 0.5)
            share_factor = 1 + (share_total / 100.0) * 3
            spend_factor = math.log(max(spend, 1) + 1)
            trend_boost = 1.3 if a["trend"] == "escalando" else (0.7 if a["trend"] == "caindo" else 1.0)
            a["score"] = round(volume * roas_factor * share_factor * spend_factor * trend_boost, 2)

            # Frequency ja esta em totals_by_ad


def _fetch_creatives_for_campaigns(sales_campaigns, date_from, date_to, warnings=None):
    """Busca criativos de campanhas com métricas avançadas.

    Otimizacao (pos-Nutricao): em vez de 2 calls por campanha (N~52 em Nutricao =
    104 calls), agrupa por conta e faz 2 calls por CONTA — uma pro /ads com
    filtering=campaign.id IN [...], outra pro /insights level=ad com mesmo filter.
    Reduz de ~2N para ~2*contas (tipicamente 2-4 calls no total).
    """
    if warnings is None:
        warnings = []
    insight_fields = INSIGHT_FIELDS_AD
    ads_by_campaign = {}
    daily_by_ad = {}

    # Mapa de campanha por id (pra associar ads depois)
    camp_by_id = {c["id"]: c for c in sales_campaigns}

    # Agrupa por conta (Meta nao suporta cross-account em 1 query)
    by_account = {}
    for c in sales_campaigns:
        acc = c.get("_account_id") or ACCOUNT_ID
        by_account.setdefault(acc, []).append(c["id"])

    # Pre-filtro: campanhas com impressoes no periodo (1 call por conta)
    camps_with_data = set()
    camp_spend = {}
    for acc_id, ids_in_acc in by_account.items():
        try:
            check_rows = meta_get_all_pages(
                f"{acc_id}/insights",
                {
                    "fields": "campaign_id,impressions,spend",
                    "time_range": json.dumps({"since": date_from, "until": date_to}),
                    "level": "campaign",
                    "filtering": json.dumps([
                        {"field": "campaign.id", "operator": "IN", "value": ids_in_acc},
                        {"field": "impressions", "operator": "GREATER_THAN", "value": 0},
                    ]),
                    "limit": 500,
                }
            )
            for r in check_rows:
                cid = r.get("campaign_id")
                if cid:
                    camps_with_data.add(cid)
                    camp_spend[cid] = float(r.get("spend", 0) or 0)
        except Exception as e:
            print(f"[WARN] Pre-filtro falhou em {acc_id}, incluindo todas daquela conta: {e}")
            camps_with_data.update(ids_in_acc)
    print(f"[OPT] {len(camps_with_data)}/{len(sales_campaigns)} campanhas com impressoes no periodo ({len(by_account)} conta(s))")

    # Estrategia otimizada: usa 4 queries de AGREGADO por conta (sem time_increment=1).
    #
    # Meta bloqueia BUC quando time_increment=1 com full fields gera tabela enorme
    # (500 ads x 30 dias = 15k rows com video_p25..p100, actions, etc). Cada row
    # exige computacao pesada do lado da Meta — BUC estourava em poucos minutos.
    #
    # Agora: 4 queries por conta, todas AGREGADAS (nao por dia):
    #   1) /ads: lista ads + creative metadata (nao agrega)
    #   2) /insights level=ad, periodo total: metricas completas por ad
    #   3) /insights level=ad, ultimos 7d: metricas por ad pra bloco 7D
    #   4) /insights level=ad, ultimos 3d: metricas por ad pra bloco 3D
    #
    # Total: 4 calls x 2 contas = 8 calls, cada uma MUITO mais leve. Share/velocity
    # sao calculadas a partir desses agregados (ratio spend_3d/spend_total etc).
    dt_end = datetime.strptime(date_to, "%Y-%m-%d")
    dt_7d_from = (dt_end - timedelta(days=6)).strftime("%Y-%m-%d")
    dt_3d_from = (dt_end - timedelta(days=2)).strftime("%Y-%m-%d")
    totals_by_ad = {}
    totals_7d_by_ad = {}
    totals_3d_by_ad = {}

    # Chunking adaptativo: comeca em 20 campanhas por batch. Se algum batch
    # falhar ("Please reduce the amount of data"), divide ao meio e tenta
    # recursivamente ate batch=1. Antes usavamos tamanho fixo (50) e batch
    # inteiro falhava sem refinar — agora o sistema achou sozinho o limite real
    # da conta. Depth max 5 cobre 20 -> 10 -> 5 -> 3 -> 2 -> 1.
    CAMP_BATCH_INITIAL = 20

    def _fetch_ads_with_split(acc_id, id_batch, depth=0, batch_label="0"):
        """Fetcha /ads pros ids do batch. Em caso de erro de volume, divide ao meio."""
        try:
            return meta_get_all_pages(
                f"{acc_id}/ads",
                {
                    "fields": "id,name,status,created_time,campaign_id,creative{id,name,thumbnail_url}",
                    "filtering": json.dumps([
                        {"field": "campaign.id", "operator": "IN", "value": id_batch}
                    ]),
                    "limit": 500,
                }
            )
        except Exception as e:
            msg = str(e)
            is_volume_err = "reduce the amount of data" in msg or "(#100)" in msg or "(#613)" in msg
            if is_volume_err and len(id_batch) > 1 and depth < 5:
                mid = len(id_batch) // 2
                print(f"[ADS-SPLIT] batch {batch_label} de {len(id_batch)} falhou por volume, divide em {mid} + {len(id_batch)-mid}")
                left = _fetch_ads_with_split(acc_id, id_batch[:mid], depth + 1, batch_label + "L")
                right = _fetch_ads_with_split(acc_id, id_batch[mid:], depth + 1, batch_label + "R")
                return left + right
            warnings.append({"step": f"fetch_ads_batch_{batch_label}", "account_id": acc_id, "error": msg})
            print(f"[ERROR] batch ads {batch_label} falhou em {acc_id}: {e}")
            return []

    def _fetch_agg_with_split(acc_id, id_batch, d_from, d_to, label, depth=0):
        """Fetcha /insights agregado pros ids do batch. Split em caso de volume."""
        try:
            return meta_get_all_pages(
                f"{acc_id}/insights",
                {
                    "fields": insight_fields,
                    "time_range": json.dumps({"since": d_from, "until": d_to}),
                    "level": "ad",
                    "filtering": json.dumps([
                        {"field": "campaign.id", "operator": "IN", "value": id_batch},
                        {"field": "impressions", "operator": "GREATER_THAN", "value": 0},
                    ]),
                    "limit": 500,
                }
            )
        except Exception as e:
            msg = str(e)
            is_volume_err = "reduce the amount of data" in msg or "(#100)" in msg or "(#613)" in msg
            if is_volume_err and len(id_batch) > 1 and depth < 5:
                mid = len(id_batch) // 2
                print(f"[AGG-SPLIT] {label} de {len(id_batch)} falhou por volume, divide em {mid} + {len(id_batch)-mid}")
                left = _fetch_agg_with_split(acc_id, id_batch[:mid], d_from, d_to, label + "L", depth + 1)
                right = _fetch_agg_with_split(acc_id, id_batch[mid:], d_from, d_to, label + "R", depth + 1)
                return left + right
            warnings.append({"step": f"fetch_agg_{label}", "account_id": acc_id, "error": msg})
            print(f"[ERROR] agg {label} falhou em {acc_id}: {e}")
            return []

    def _chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    for acc_id, ids_in_acc in by_account.items():
        camp_ids_active = [cid for cid in ids_in_acc if cid in camps_with_data]
        if not camp_ids_active:
            continue

        # Call 1: ads — batches de 20 com auto-split se necessario
        acc_ads = []
        for batch_idx, id_batch in enumerate(_chunks(camp_ids_active, CAMP_BATCH_INITIAL)):
            acc_ads.extend(_fetch_ads_with_split(acc_id, id_batch, 0, str(batch_idx)))

        # Calls 2-4: agregados totais / 7d / 3d
        def _collect_agg(d_from, d_to, base_label, target_dict):
            for batch_idx, id_batch in enumerate(_chunks(camp_ids_active, CAMP_BATCH_INITIAL)):
                rows = _fetch_agg_with_split(acc_id, id_batch, d_from, d_to, f"{base_label}_{batch_idx}")
                for row in rows:
                    ad_id = row.get("ad_id")
                    if ad_id:
                        target_dict[ad_id] = parse_insights(row)

        _collect_agg(date_from, date_to, "total", totals_by_ad)
        _collect_agg(dt_7d_from, date_to, "7d", totals_7d_by_ad)
        _collect_agg(dt_3d_from, date_to, "3d", totals_3d_by_ad)

        # Agrupa ads por campanha e processa
        ads_by_camp_local = {}
        for ad in acc_ads:
            cid = ad.get("campaign_id")
            if cid in camps_with_data:
                ads_by_camp_local.setdefault(cid, []).append(ad)

        empty_metrics = {
            "spend": 0, "impressions": 0, "clicks": 0, "reach": 0,
            "purchases": 0, "revenue": 0, "roas": 0, "cpa": 0,
            "ctr": 0, "cpm": 0, "cpp": 0, "link_clicks": 0,
            "cost_per_link_click": 0,
        }

        for cid, ads in ads_by_camp_local.items():
            camp = camp_by_id.get(cid, {"id": cid, "name": cid})
            camp_ads = []
            for ad in ads:
                ad_id = ad["id"]
                ad_metrics = totals_by_ad.get(ad_id)
                if not ad_metrics or ad_metrics.get("impressions", 0) == 0:
                    continue
                creative = ad.get("creative", {})
                metrics = {**empty_metrics, **ad_metrics}
                created = ad.get("created_time", "")
                days_active = 0
                if created:
                    try:
                        days_active = (dt_end - datetime.fromisoformat(created[:10])).days
                    except Exception:
                        pass
                entry = {
                    "campaign_id": cid,
                    "campaign_name": camp.get("name", ""),
                    "account_id": acc_id,
                    "ad_id": ad_id,
                    "ad_name": ad.get("name", ""),
                    "ad_status": ad.get("status", ""),
                    "creative_id": creative.get("id", ""),
                    "creative_name": creative.get("name", ""),
                    "thumbnail_url": creative.get("thumbnail_url", ""),
                    "days_active": days_active,
                    **metrics,
                }
                camp_ads.append(entry)
            ads_by_campaign[cid] = {"name": camp.get("name", ""), "ads": camp_ads}

    # Calcular metricas avancadas (3d, 7d, velocity, trend, score) usando AGREGADOS pre-computados.
    _compute_advanced_metrics_from_aggregates(
        ads_by_campaign, totals_by_ad, totals_7d_by_ad, totals_3d_by_ad, date_from, date_to
    )

    # Flatten
    all_creatives = []
    for camp_id, cdata in ads_by_campaign.items():
        all_creatives.extend(cdata["ads"])

    all_creatives.sort(key=lambda x: x.get("score", 0), reverse=True)
    return all_creatives


@app.route("/api/dashboard/campaigns/<campaign_id>/creatives")
@not_viewer_required
def api_campaign_creatives(campaign_id):
    """Lista criativos de uma campanha com métricas avançadas."""
    try:
        date_from = request.args.get("date_from", _default_date_from())
        date_to = request.args.get("date_to", _yesterday())
        g.camp_type = _camp_type_from_request()

        blocked = _enforce_range_for_role(date_from, date_to)
        if blocked:
            return blocked

        # Buscar nome da campanha
        camp_info = meta_get(campaign_id, {"fields": "id,name,objective"})
        camp = {"id": campaign_id, "name": camp_info.get("name", "")}

        result = _fetch_creatives_for_campaigns([camp], date_from, date_to)
        return jsonify({"ok": True, "data": result})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/dashboard/daily-summary")
@not_viewer_required
def api_daily_summary():
    """Insights diários agregados de TODAS as campanhas do tipo selecionado (somatório)."""
    try:
        date_from = request.args.get("date_from", _default_date_from())
        date_to = request.args.get("date_to", _yesterday())
        camp_status = request.args.get("camp_status", "active")
        force = request.args.get("force", "false") == "true"
        camp_type = _camp_type_from_request()
        g.camp_type = camp_type

        blocked = _enforce_range_for_role(date_from, date_to)
        if blocked:
            return blocked

        cache_key = f"daily_summary_v9_{camp_type}_{camp_status}_{date_from}_{date_to}"
        if not force:
            cached = get_cached(cache_key)
            if cached:
                return jsonify(cached)

        # 1+2. Usa daily insights compartilhado (mesma fonte que api_multi_insights)
        # — evita duplicar fetch pesado com time_increment=1.
        sales_campaigns, rows = _get_shared_daily_insights(camp_type, date_from, date_to, camp_status)

        if not sales_campaigns:
            return jsonify({"ok": True, "data": []})

        # Crescimento: sobrescreve purchases com atribuicao proporcional ao spend diario
        ig_net_total = 0
        cresc_share = 1.0
        cresc_spend_total = 0
        if camp_type == CAMP_TYPE_CRESCIMENTO:
            try:
                ig_follower, ig_non = fetch_ig_follower_gain_total(IG_PROFILE_ID_JRM, date_from, date_to)
                ig_net_total = max(0, ig_follower - ig_non)
                cresc_spend_total = sum(float(r.get("spend", 0) or 0) for r in rows)
                accounts = _get_accounts_for_type(camp_type)
                total_spend = _fetch_total_spend_all_accounts(accounts, date_from, date_to)
                other_spend = max(0, total_spend - cresc_spend_total)
                cresc_share = compute_crescimento_share(cresc_spend_total, other_spend)
            except Exception as e:
                print(f"[CRESCIMENTO daily-summary] Falha IG API: {e}")

        # Agregar por dia
        by_date = {}
        for row in rows:
            d = row.get("date_start", "")
            parsed = parse_insights(row)
            if d not in by_date:
                by_date[d] = {
                    "date": d, "spend": 0, "revenue": 0, "purchases": 0,
                    "impressions": 0, "clicks": 0, "link_clicks": 0,
                    "lpv": 0, "profile_visits": 0, "initiate_checkout": 0,
                    "video_plays": 0, "video_p25": 0, "video_p50": 0, "video_p75": 0,
                    "video_p95": 0, "video_p100": 0, "video_thruplay": 0,
                }
            by_date[d]["spend"] += parsed.get("spend", 0)
            by_date[d]["revenue"] += parsed.get("revenue", 0)
            by_date[d]["purchases"] += parsed.get("purchases", 0)
            by_date[d]["impressions"] += parsed.get("impressions", 0)
            by_date[d]["clicks"] += parsed.get("clicks", 0)
            by_date[d]["link_clicks"] += parsed.get("link_clicks", 0)
            by_date[d]["lpv"] += parsed.get("lpv", 0)
            by_date[d]["profile_visits"] += parsed.get("profile_visits", 0)
            by_date[d]["initiate_checkout"] += parsed.get("initiate_checkout", 0)
            by_date[d]["video_plays"] += parsed.get("video_plays", 0)
            by_date[d]["video_p25"] += parsed.get("video_p25", 0)
            by_date[d]["video_p50"] += parsed.get("video_p50", 0)
            by_date[d]["video_p75"] += parsed.get("video_p75", 0)
            by_date[d]["video_p95"] += parsed.get("video_p95", 0)
            by_date[d]["video_p100"] += parsed.get("video_p100", 0)
            by_date[d]["video_thruplay"] += parsed.get("video_thruplay", 0)

        # Crescimento: distribui total_net * share entre dias proporcional ao spend
        if camp_type == CAMP_TYPE_CRESCIMENTO and ig_net_total > 0 and cresc_spend_total > 0:
            cresc_total_attributed = ig_net_total * cresc_share
            for d, row in by_date.items():
                spend_day = row.get("spend", 0)
                if spend_day > 0:
                    row["purchases"] = max(0, int(round(cresc_total_attributed * spend_day / cresc_spend_total)))
                else:
                    row["purchases"] = 0

        # Calcular m&eacute;tricas derivadas por dia
        daily = []
        for d in sorted(by_date.keys()):
            row = by_date[d]
            row["spend"] = round(row["spend"], 2)
            row["revenue"] = round(row["revenue"], 2)
            row["roas"] = round(row["revenue"] / row["spend"], 2) if row["spend"] > 0 else 0
            row["cpa"] = round(row["spend"] / row["purchases"], 2) if row["purchases"] > 0 else 0
            row["cpm"] = round((row["spend"] / row["impressions"]) * 1000, 2) if row["impressions"] > 0 else 0
            row["ctr"] = round((row["clicks"] / row["impressions"]) * 100, 2) if row["impressions"] > 0 else 0
            row["cost_per_ic"] = round(row["spend"] / row["initiate_checkout"], 2) if row["initiate_checkout"] > 0 else 0
            pv = row.get("profile_visits", 0)
            row["cost_per_profile_visit"] = round(row["spend"] / pv, 2) if pv > 0 else 0
            tp = row.get("video_thruplay", 0)
            row["cost_per_thruplay"] = round(row["spend"] / tp, 2) if tp > 0 else 0
            vplays = row.get("video_plays", 0)
            row["cost_per_video_play"] = round(row["spend"] / vplays, 2) if vplays > 0 else 0
            row["rate_play_thruplay"] = round((tp / vplays) * 100, 2) if vplays > 0 else 0
            lc = row["link_clicks"]
            row["rate_click_lpv"] = round((row["lpv"] / lc) * 100, 2) if lc > 0 else 0
            row["rate_lpv_ic"] = round((row["initiate_checkout"] / row["lpv"]) * 100, 2) if row["lpv"] > 0 else 0
            row["rate_ic_purchase"] = round((row["purchases"] / row["initiate_checkout"]) * 100, 2) if row["initiate_checkout"] > 0 else 0
            row["rate_click_purchase"] = round((row["purchases"] / lc) * 100, 2) if lc > 0 else 0
            daily.append(row)

        # 3. Buscar dados AGREGADOS (sem time_increment) para reach e frequency reais
        agg_totals = {"reach": 0, "frequency": 0}
        try:
            agg_rows = meta_get_all_pages(
                f"{ACCOUNT_ID}/insights",
                {
                    "fields": "reach,frequency,impressions",
                    "time_range": json.dumps({"since": date_from, "until": date_to}),
                    "level": "account",
                    "filtering": json.dumps([{"field": "campaign.id", "operator": "IN", "value": camp_ids}]),
                }
            )
            if agg_rows:
                agg_totals["reach"] = int(agg_rows[0].get("reach", 0))
                agg_totals["frequency"] = round(float(agg_rows[0].get("frequency", 0)), 2)
        except Exception as e:
            print(f"[WARN] Falha ao buscar reach/frequency agregados: {e}")

        response = {"ok": True, "data": daily, "aggregated": agg_totals}
        set_cached(cache_key, response, ttl_hours=_cache_ttl_for_range(date_from, date_to))
        return jsonify(response)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


# Dedupe de computes em andamento — evita que 2 retries do frontend
# disparem 2 bg threads simultaneas pro mesmo cache_key.
_resumo_computing = set()
_resumo_computing_lock = threading.Lock()


@app.route("/api/dashboard/resumo")
@not_viewer_required
def api_resumo():
    """Consolidado da conta toda: gasto + KPI principal de cada tipo +
    comparacao vs periodo anterior + top campanhas + serie diaria por tipo.

    Reusa shared_daily_v1 cache por tipo (4h). Adiciona 'Outros' = gasto
    total da(s) conta(s) - soma dos tipos mapeados.
    """
    try:
        date_from = request.args.get("date_from", _default_date_from())
        date_to = request.args.get("date_to", _yesterday())
        force = request.args.get("force", "false") == "true"

        blocked = _enforce_range_for_role(date_from, date_to)
        if blocked:
            return blocked

        cache_key = f"resumo_v16_{date_from}_{date_to}"
        if not force:
            cached = get_cached(cache_key)
            if cached:
                return jsonify(cached)

            # Cache miss: dispara compute COMPLETO em background via test_client
            # (com force=true pra bypassar esta propria logica de 202 no bg).
            # Bg thread nao tem timeout de gunicorn, pode rodar 5-10min sem problema.
            # Quando termina, ele mesmo cacheia resumo_v16 — proxima retry do
            # frontend pega direto do cache.
            with _resumo_computing_lock:
                already_running = cache_key in _resumo_computing
                if not already_running:
                    _resumo_computing.add(cache_key)
            if not already_running:
                dfrom_bg, dto_bg = date_from, date_to
                ck_bg = cache_key
                def _bg_compute():
                    # Se BUC critico, espera drenar antes de comecar
                    if _buc_is_critical():
                        print(f"[RESUMO-BG] BUC critico — aguardando antes de {dfrom_bg}")
                        _wait_for_buc_ok(max_wait_seconds=600)
                        if _buc_is_critical():
                            print(f"[RESUMO-BG] BUC ainda critico apos espera — abortando {dfrom_bg}")
                            with _resumo_computing_lock:
                                _resumo_computing.discard(ck_bg)
                            return
                    print(f"[RESUMO-BG] Computando {dfrom_bg} a {dto_bg} em background")
                    try:
                        with app.test_client() as client:
                            with client.session_transaction() as sess:
                                sess["logged_in"] = True
                                sess["username"] = SUPER_ADMIN_EMAIL
                                sess["role"] = "super_admin"
                            client.get(
                                f"/api/dashboard/resumo?date_from={dfrom_bg}&date_to={dto_bg}&force=true",
                                headers={"X-Internal-Scheduler": "resumo_bg_compute"}
                            )
                        print(f"[RESUMO-BG] Concluido {dfrom_bg} a {dto_bg}")
                    except Exception as e:
                        print(f"[RESUMO-BG] Erro computando {dfrom_bg} a {dto_bg}: {e}")
                    finally:
                        with _resumo_computing_lock:
                            _resumo_computing.discard(ck_bg)
                threading.Thread(target=_bg_compute, daemon=True).start()
            return jsonify({
                "warming": True,
                "in_flight": already_running,
                "message": "Calculando resumo em segundo plano. Nova tentativa automatica em ~30s.",
            }), 202

        dt_from = datetime.strptime(date_from, "%Y-%m-%d")
        dt_to_obj = datetime.strptime(date_to, "%Y-%m-%d")
        period_days = (dt_to_obj - dt_from).days + 1
        prev_to = (dt_from - timedelta(days=1)).strftime("%Y-%m-%d")
        prev_from = (dt_from - timedelta(days=period_days)).strftime("%Y-%m-%d")

        # Metadados por tipo: label, icone, KPI principal e de custo.
        # Crescimento aparece com Visitas no Perfil (nao Seguidores) porque
        # a atribuicao de seguidores exige IG API + calculo caro que nao
        # vale replicar no resumo — no detalhe da aba Crescimento continua.
        type_meta = {
            CAMP_TYPE_VENDAS: {
                "label": "Vendas", "icon": "V",
                "kpi": "purchases", "kpi_label": "Compras", "kpi_fmt": "int",
                "kpi_cost_label": "CPA", "kpi_cost_fmt": "currency",
            },
            CAMP_TYPE_METEORICOS: {
                "label": "Meteoricos", "icon": "M",
                "kpi": "purchases", "kpi_label": "Leads", "kpi_fmt": "int",
                "kpi_cost_label": "CPL", "kpi_cost_fmt": "currency",
            },
            CAMP_TYPE_COMERCIAL: {
                "label": "Comercial", "icon": "C",
                "kpi": "purchases", "kpi_label": "Leads", "kpi_fmt": "int",
                "kpi_cost_label": "CPL", "kpi_cost_fmt": "currency",
            },
            CAMP_TYPE_CRESCIMENTO: {
                "label": "Crescimento", "icon": "G",
                "kpi": "profile_visits", "kpi_label": "Visitas Perfil", "kpi_fmt": "int",
                "kpi_cost_label": "Custo/Visita", "kpi_cost_fmt": "currency",
            },
            CAMP_TYPE_NUTRICAO: {
                "label": "Nutricao", "icon": "N",
                "kpi": "video_thruplay", "kpi_label": "ThruPlays", "kpi_fmt": "int",
                "kpi_cost_label": "CPTP", "kpi_cost_fmt": "currency",
            },
        }

        def _aggregate_type(ct, d_from, d_to, want_detail=True):
            """Agrega totais + serie diaria + top campanhas de 1 tipo.

            Estrategia de cache em 2 niveis:
              1. Exact-range cache (campaigns_v7 + daily_summary_v9 do range pedido):
                 o scheduler diario warma 30d/7d que o dashboard usa com frequencia.
                 Se bate, retorna instantaneo.
              2. Chunking por mes: se o range cold, split em segmentos mensais.
                 Meses completos dentro do range mapeam pros caches pinados
                 (warmup mensal) — instantaneo. So segmentos parciais (prefixo/
                 sufixo do mes corrente) fazem fetch Meta, e em range pequeno.
            Evita timeout do NGINX em ranges multi-mes cold."""
            kpi_field = type_meta[ct]["kpi"]
            key_camp = f"campaigns_v8_{ct}_all_{d_from}_{d_to}"
            key_daily = f"daily_summary_v9_{ct}_all_{d_from}_{d_to}"
            camp_cached = get_cached(key_camp)
            daily_cached = get_cached(key_daily) if want_detail else None

            # Se cache exato bate, pula direto — caminho rapido pra ranges
            # warmed pelo scheduler (30d/7d).
            if camp_cached and (not want_detail or daily_cached):
                pass
            else:
                # Chunking por mes: resolve cada segmento via cache (pinado
                # se for mes completo) ou fetch curto
                segments = _split_range_by_month_segments(d_from, d_to)
                seg_camp = []
                seg_daily = []
                try:
                    with app.test_client() as client:
                        with client.session_transaction() as sess:
                            sess["logged_in"] = True
                            sess["username"] = SUPER_ADMIN_EMAIL
                            sess["role"] = "super_admin"
                        hdr = {"X-Internal-Scheduler": "resumo_chunked"}
                        for seg_from, seg_to in segments:
                            sk_camp = f"campaigns_v8_{ct}_all_{seg_from}_{seg_to}"
                            sk_daily = f"daily_summary_v9_{ct}_all_{seg_from}_{seg_to}"
                            sc = get_cached(sk_camp)
                            sd = get_cached(sk_daily) if want_detail else None
                            if not sc:
                                client.get(f"/api/dashboard/campaigns?camp_type={ct}&date_from={seg_from}&date_to={seg_to}&camp_status=all", headers=hdr)
                                sc = get_cached(sk_camp)
                            if want_detail and not sd:
                                client.get(f"/api/dashboard/daily-summary?camp_type={ct}&date_from={seg_from}&date_to={seg_to}&camp_status=all", headers=hdr)
                                sd = get_cached(sk_daily)
                            if sc:
                                seg_camp.append(sc)
                            if sd:
                                seg_daily.append(sd)
                except Exception as e:
                    print(f"[RESUMO] Falha chunked {ct} {d_from}-{d_to}: {e}")

                camp_cached = _merge_campaigns_data(seg_camp) if seg_camp else camp_cached
                if want_detail:
                    daily_cached = _merge_daily_data(seg_daily) if seg_daily else daily_cached

            campaigns_data = (camp_cached or {}).get("data", []) or []
            summary = (camp_cached or {}).get("summary", {}) or {}

            tot_spend = float(summary.get("total_spend", 0) or 0)
            tot_revenue = float(summary.get("total_revenue", 0) or 0)
            tot_purchases = int(summary.get("total_purchases", 0) or 0)

            # Mapeia KPI summary conforme o tipo
            summary_kpi_map = {
                "revenue": "total_revenue",
                "purchases": "total_purchases",
                "profile_visits": "total_profile_visits",
                "video_thruplay": "total_thruplay",
            }
            tot_kpi = float(summary.get(summary_kpi_map.get(kpi_field, ""), 0) or 0)

            # Custo/Resultado: spend/kpi pra todos os tipos (CPA/CPL/CPS/CPTP/Custo-Visita)
            kpi_cost = round(tot_spend / tot_kpi, 2) if tot_kpi > 0 else 0

            # Agrupa campanhas por evento/produto/cidade. Usa event_name como base
            # removendo sufixo " (N)" que o event_grouper adiciona quando divide
            # um mesmo produto em sub-eventos por gap de datas. Ex:
            # "Professional & Self Coaching (1)" e "Professional & Self Coaching (2)"
            # viram um so "Professional & Self Coaching".
            import re as _re
            def _norm_event_name(nm):
                if not nm:
                    return "Outros"
                return _re.sub(r"\s*\(\d+\)\s*$", "", nm).strip() or "Outros"

            by_event = {}
            for c in campaigns_data:
                cid = c.get("id") or ""
                if not cid:
                    continue
                base_name = _norm_event_name(c.get("event_name", ""))
                cspend = float(c.get("spend", 0) or 0)
                ckpi_raw = float(c.get(kpi_field, 0) or 0)
                crev = float(c.get("revenue", 0) or 0)
                if base_name not in by_event:
                    by_event[base_name] = {
                        "id": base_name,
                        "name": base_name,
                        "spend": 0.0,
                        "kpi_raw": 0.0,
                        "revenue": 0.0,
                        "campaign_count": 0,
                    }
                e = by_event[base_name]
                e["spend"] += cspend
                e["kpi_raw"] += ckpi_raw
                e["revenue"] += crev
                e["campaign_count"] += 1

            full_events = []
            for e in by_event.values():
                espend = round(e["spend"], 2)
                ekpi = round(e["kpi_raw"], 2)
                ecost = round(espend / ekpi, 2) if ekpi > 0 else 0
                display_name = e["name"] + (" · " + str(e["campaign_count"]) + " camps" if e["campaign_count"] > 1 else "")
                full_events.append({
                    "id": e["id"],
                    "name": display_name,
                    "spend": espend,
                    "kpi": ekpi,
                    "kpi_cost": ecost,
                    "campaign_count": e["campaign_count"],
                })
            full_events.sort(key=lambda x: x["spend"], reverse=True)
            top_camps = full_events[:50]

            # Lista de campanhas INDIVIDUAIS (sem agrupar) pra usar no Top 10
            # Global — onde o usuario quer ver as campanhas reais que mais
            # investiram, nao grupos de eventos.
            individual_camps = []
            for c in campaigns_data:
                cid = c.get("id") or ""
                if not cid:
                    continue
                cspend = float(c.get("spend", 0) or 0)
                ckpi = float(c.get(kpi_field, 0) or 0)
                ccost = round(cspend / ckpi, 2) if ckpi > 0 else 0
                individual_camps.append({
                    "id": cid,
                    "name": c.get("name", ""),
                    "spend": round(cspend, 2),
                    "kpi": round(ckpi, 2),
                    "kpi_cost": ccost,
                })
            individual_camps.sort(key=lambda x: x["spend"], reverse=True)

            # Daily series — usa daily_summary cache (campo corresponde ao kpi_field)
            daily_list = []
            if daily_cached:
                for r in daily_cached.get("data", []) or []:
                    daily_list.append({
                        "date": r.get("date", ""),
                        "spend": round(float(r.get("spend", 0) or 0), 2),
                        "kpi": round(float(r.get(kpi_field, 0) or 0), 2),
                    })

            active_count = len([c for c in campaigns_data if (c.get("spend", 0) or 0) > 0])

            return {
                "type": ct,
                "label": type_meta[ct]["label"],
                "icon": type_meta[ct]["icon"],
                "spend": round(tot_spend, 2),
                "campaigns_active": active_count,
                "kpi_label": type_meta[ct]["kpi_label"],
                "kpi_value": round(tot_kpi, 2),
                "kpi_fmt": type_meta[ct]["kpi_fmt"],
                "kpi_cost_label": type_meta[ct]["kpi_cost_label"],
                "kpi_cost_value": kpi_cost,
                "kpi_cost_fmt": type_meta[ct]["kpi_cost_fmt"],
                "revenue": round(tot_revenue, 2),
                "purchases": tot_purchases,
                "daily": daily_list,
                "top_campaigns": top_camps,
                # IDs completos de campanhas do tipo — usado pra dedupe no passo
                # de classificacao Outros (apagado antes do jsonify).
                "_active_ids": set(c.get("id", "") for c in campaigns_data if c.get("id")),
                # Campanhas individuais (sem agrupar por evento) — usado pra
                # montar o Top 10 Global com campanhas reais, nao eventos.
                "_individual_campaigns": individual_camps,
            }

        per_type = [_aggregate_type(ct, date_from, date_to) for ct in VALID_CAMP_TYPES]
        prev_by_type = {
            ct: _aggregate_type(ct, prev_from, prev_to, want_detail=False)["spend"]
            for ct in VALID_CAMP_TYPES
        }

        # 1 call por conta pra /insights level=campaign (leve — 1 row por
        # campanha com impressoes no periodo). Uso triplo:
        #   a) Enriquecer per_type com spend das campanhas que estao em mapped_ids
        #      mas nao apareceram em shared_daily 'active' (ex: archived).
        #   b) Lista drill-down 'Outros' (campanhas nao mapeadas com gasto).
        #   c) total_acc_spend via soma direta (mais confiavel que level=account).
        all_accounts = set()
        for ct in VALID_CAMP_TYPES:
            for acc in _get_accounts_for_type(ct):
                if acc:
                    all_accounts.add(acc)
        all_accounts = list(all_accounts)

        # Outros = total_acc - soma_tipos. Pra o TOTAL usamos account_total_spend
        # (level=account, 1 row, ~1s e cacheado 20min). A LISTA top_unmapped
        # (drill-down) tem endpoint separado /api/dashboard/resumo/unmapped
        # — pre-warmed pelo scheduler/boot/monthly pra estar sempre pronto.
        #
        # Se o cache unmapped ja existe, inline aqui pra zero latencia no
        # drill-down. Se nao, o frontend lazy-fetcha quando usuario clicar.
        _unmapped_cached = get_cached(f"resumo_unmapped_v1_{date_from}_{date_to}")
        top_unmapped = (_unmapped_cached or {}).get("data", []) if _unmapped_cached else []
        total_acc_spend = _fetch_total_spend_all_accounts(all_accounts, date_from, date_to)
        total_acc_spend_prev = _fetch_total_spend_all_accounts(all_accounts, prev_from, prev_to)

        typed_spend = sum(t["spend"] for t in per_type)
        prev_typed_spend = sum(prev_by_type.values())
        # Outros = gasto total da conta - tudo que as tabs classificaram
        outros_spend = max(0.0, round(total_acc_spend - typed_spend, 2))
        outros_spend_prev = max(0.0, round(total_acc_spend_prev - prev_typed_spend, 2))

        total_spend_curr = round(typed_spend + outros_spend, 2)
        total_spend_prev = round(prev_typed_spend + outros_spend_prev, 2)

        for t in per_type:
            t["spend_prev"] = round(prev_by_type.get(t["type"], 0), 2)
            t["spend_pct"] = round((t["spend"] / total_spend_curr) * 100, 1) if total_spend_curr > 0 else 0

        per_type.append({
            "type": "outros",
            "label": "Outros",
            "icon": "?",
            "spend": outros_spend,
            "spend_prev": outros_spend_prev,
            "spend_pct": round((outros_spend / total_spend_curr) * 100, 1) if total_spend_curr > 0 else 0,
            "campaigns_active": len(top_unmapped) if top_unmapped else None,
            "kpi_label": None, "kpi_value": None, "kpi_fmt": None,
            "kpi_cost_label": None, "kpi_cost_value": None, "kpi_cost_fmt": None,
            "revenue": 0, "purchases": 0,
            "daily": [], "top_campaigns": [],
            "top_unmapped": top_unmapped,
        })

        # Top 10 campanhas INDIVIDUAIS (nao agrupadas por evento) ordenadas por gasto.
        # Usa _individual_campaigns que preserva cada campanha separada.
        all_top = []
        for t in per_type:
            if t["type"] == "outros":
                continue
            for c in (t.get("_individual_campaigns") or []):
                all_top.append({
                    "id": c["id"],
                    "name": c["name"],
                    "type": t["type"],
                    "type_label": t["label"],
                    "spend": c["spend"],
                    "kpi_label": t["kpi_label"],
                    "kpi_value": c["kpi"],
                    "kpi_fmt": t["kpi_fmt"],
                })
        all_top.sort(key=lambda x: x["spend"], reverse=True)
        all_top = all_top[:10]

        total_active = sum((t["campaigns_active"] or 0) for t in per_type)

        # Remove campos internos antes de serializar (set nao eh JSON-serializable)
        for t in per_type:
            t.pop("_active_ids", None)
            t.pop("_individual_campaigns", None)

        response = {
            "ok": True,
            "period": {"from": date_from, "to": date_to, "days": period_days},
            "period_prev": {"from": prev_from, "to": prev_to, "days": period_days},
            "totals": {
                "spend": total_spend_curr,
                "spend_prev": total_spend_prev,
                "campaigns_active": total_active,
            },
            "per_type": per_type,
            "top_campaigns": all_top,
        }
        # Se o range eh inteiramente composto de meses COMPLETOS (fechados),
        # pina o resumo_v16 por 180d — dados historicos nao mudam.
        _range_segments = _split_range_by_month_segments(date_from, date_to)
        if _range_segments and all(_is_completed_month(sf, st) for sf, st in _range_segments):
            set_cached(cache_key, response, ttl_hours=_MONTHLY_TTL_HOURS)
        else:
            set_cached(cache_key, response, ttl_hours=_cache_ttl_for_range(date_from, date_to))
        return jsonify(response)
    except Exception as e:
        import traceback
        return jsonify({"ok": False, "error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/dashboard/resumo/unmapped")
@not_viewer_required
def api_resumo_unmapped():
    """Drill-down do Outros: lista de campanhas nao classificadas com gasto
    no periodo. Separado do /resumo pra nao bloquear o carregamento inicial
    com Meta calls pesadas (level=campaign, paginado). Frontend chama quando
    usuario expande a seta de Outros."""
    try:
        date_from = request.args.get("date_from", _default_date_from())
        date_to = request.args.get("date_to", _yesterday())
        blocked = _enforce_range_for_role(date_from, date_to)
        if blocked:
            return blocked

        cache_key = f"resumo_unmapped_v1_{date_from}_{date_to}"
        cached = get_cached(cache_key)
        if cached is not None:
            return jsonify(cached)

        all_accounts = set()
        for ct in VALID_CAMP_TYPES:
            for acc in _get_accounts_for_type(ct):
                if acc:
                    all_accounts.add(acc)
        all_accounts = list(all_accounts)

        overrides = _load_overrides()
        def _classify_by_name(nm, objective):
            if _is_nutricao_campaign(nm): return CAMP_TYPE_NUTRICAO
            if _is_meteoricos_campaign(nm): return CAMP_TYPE_METEORICOS
            if _is_comercial_campaign(nm): return CAMP_TYPE_COMERCIAL
            if _is_crescimento_campaign(nm): return CAMP_TYPE_CRESCIMENTO
            if objective == "OUTCOME_SALES": return CAMP_TYPE_VENDAS
            if _is_vendas_campaign_by_name(nm): return CAMP_TYPE_VENDAS
            return None

        obj_by_id = {}
        for acc in all_accounts:
            try:
                for c in _fetch_account_campaigns(acc, "id,name,objective", '["ACTIVE","PAUSED","ARCHIVED"]'):
                    obj_by_id[c.get("id", "")] = c.get("objective", "")
            except Exception as e:
                print(f"[UNMAPPED] Falha meta campanhas {acc}: {e}")

        top_unmapped = []
        for acc in all_accounts:
            rows = _fetch_acc_insights_chunked(acc, date_from, date_to)
            for r in rows:
                cid = r.get("campaign_id", "")
                if not cid: continue
                sp = float(r.get("spend", 0) or 0)
                if sp <= 0: continue
                nm = r.get("campaign_name", "")
                ov = overrides.get(cid)
                ct = ov.get("camp_type") if ov else None
                if not ct:
                    ct = _classify_by_name(nm, obj_by_id.get(cid, ""))
                if ct not in VALID_CAMP_TYPES:
                    top_unmapped.append({
                        "id": cid, "name": nm,
                        "spend": round(sp, 2), "account_id": acc,
                    })
        top_unmapped.sort(key=lambda x: x["spend"], reverse=True)
        top_unmapped = top_unmapped[:20]

        response = {"ok": True, "data": top_unmapped}
        # TTL longo se range for todos meses completos
        _segs = _split_range_by_month_segments(date_from, date_to)
        if _segs and all(_is_completed_month(sf, st) for sf, st in _segs):
            set_cached(cache_key, response, ttl_hours=_MONTHLY_TTL_HOURS)
        else:
            set_cached(cache_key, response, ttl_hours=_cache_ttl_for_range(date_from, date_to))
        return jsonify(response)
    except Exception as e:
        import traceback
        return jsonify({"ok": False, "error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/dashboard/cumulative-reach")
@not_viewer_required
def api_cumulative_reach():
    """Reach e Frequency cumulativos reais (janela crescente dia a dia).

    Cada ponto = reach/frequency do dia 1 ATÉ aquele dia (crescente).
    Busca a cada 2 dias para reduzir chamadas API (interpola o resto).
    """
    try:
        date_from = request.args.get("date_from", _default_date_from())
        date_to = request.args.get("date_to", _yesterday())
        camp_type = _camp_type_from_request()
        g.camp_type = camp_type

        blocked = _enforce_range_for_role(date_from, date_to)
        if blocked:
            return blocked
        campaign_id = request.args.get("campaign_id", "")  # 1 ID ou vários separados por vírgula
        force = request.args.get("force", "false") == "true"

        cache_key = f"cumulative_reach_{camp_type}_{campaign_id or 'all'}_{date_from}_{date_to}"
        if not force:
            cached = get_cached(cache_key)
            if cached:
                return jsonify(cached)

        # Suporta: 1 ID, múltiplos IDs (vírgula), ou vazio (todas do tipo)
        # Multi-conta: para multiplos IDs, precisamos saber a conta de cada um.
        # Como campaign_id pode vir da UI sem contexto, assumimos conta principal
        # para vendas/meteoricos e varremos contas comerciais para comercial.
        if campaign_id:
            ids_list = [i.strip() for i in campaign_id.split(",") if i.strip()]
            if len(ids_list) == 1:
                endpoint = f"{ids_list[0]}/insights"
                filtering = None
            else:
                endpoint = f"{ACCOUNT_ID}/insights"
                filtering = json.dumps([{"field": "campaign.id", "operator": "IN", "value": ids_list}])
        else:
            camp_status = request.args.get("camp_status", "active")
            filtered = _fetch_type_campaigns(
                camp_type, "id,name,objective", _camp_status_filter(camp_status)
            )
            if not filtered:
                return jsonify({"ok": True, "data": []})
            # cumulative-reach usa apenas 1 endpoint agregador. Se houver campanhas
            # em mais de uma conta, usa a conta com mais campanhas (aproximacao).
            by_acc = {}
            for c in filtered:
                by_acc.setdefault(c.get("_account_id") or ACCOUNT_ID, []).append(c["id"])
            main_acc = max(by_acc, key=lambda k: len(by_acc[k]))
            camp_ids = by_acc[main_acc]
            endpoint = f"{main_acc}/insights"
            filtering = json.dumps([{"field": "campaign.id", "operator": "IN", "value": camp_ids}])

        # Gerar datas: a cada 2 dias + último dia
        dt_from = datetime.strptime(date_from, "%Y-%m-%d")
        dt_to = datetime.strptime(date_to, "%Y-%m-%d")
        total_days = (dt_to - dt_from).days + 1
        step = max(1, min(3, total_days // 15))  # 1-3 dias de step (máx 15 pontos)

        points = []
        day = 0
        while day < total_days:
            dt_end = dt_from + timedelta(days=day)
            params = {
                "fields": "reach,frequency,impressions",
                "time_range": json.dumps({"since": date_from, "until": dt_end.strftime("%Y-%m-%d")}),
            }
            if filtering:
                params["filtering"] = filtering
                params["level"] = "account"

            try:
                rows = meta_get_all_pages(endpoint, params)
                if rows:
                    points.append({
                        "date": dt_end.strftime("%Y-%m-%d"),
                        "reach": int(rows[0].get("reach", 0)),
                        "frequency": round(float(rows[0].get("frequency", 0)), 2),
                        "impressions": int(rows[0].get("impressions", 0)),
                    })
            except Exception as e:
                print(f"[WARN] cumulative-reach falhou para {dt_end}: {e}")

            day += step

        # Garantir que o último dia está incluído
        if points and points[-1]["date"] != date_to:
            params = {
                "fields": "reach,frequency,impressions",
                "time_range": json.dumps({"since": date_from, "until": date_to}),
            }
            if filtering:
                params["filtering"] = filtering
                params["level"] = "account"
            try:
                rows = meta_get_all_pages(endpoint, params)
                if rows:
                    points.append({
                        "date": date_to,
                        "reach": int(rows[0].get("reach", 0)),
                        "frequency": round(float(rows[0].get("frequency", 0)), 2),
                        "impressions": int(rows[0].get("impressions", 0)),
                    })
            except Exception:
                pass

        response = {"ok": True, "data": points}
        set_cached(cache_key, response, ttl_hours=_cache_ttl_for_range(date_from, date_to))
        return jsonify(response)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/dashboard/ads/<ad_id>/insights")
@login_required
def api_ad_insights(ad_id):
    """Insights diários de um anúncio individual + funil completo."""
    try:
        date_from = request.args.get("date_from", _default_date_from())
        date_to = request.args.get("date_to", _yesterday())
        camp_type = _camp_type_from_request()
        g.camp_type = camp_type

        cache_key = f"ad_insights_{camp_type}_{ad_id}_{date_from}_{date_to}"
        cached = get_cached(cache_key)
        if cached:
            return jsonify(cached)

        # Buscar dados básicos do ad (nome, criativo, status, campanha, post link, preview)
        ad_info = meta_get(ad_id, {
            "fields": "id,name,status,created_time,campaign{id,name},creative{id,name,thumbnail_url,effective_object_story_id,object_story_spec},adcreatives{effective_object_story_id}"
        })

        # Buscar insights di&aacute;rios
        rows = meta_get_all_pages(
            f"{ad_id}/insights",
            {
                "fields": INSIGHT_FIELDS_DAILY,
                "time_range": json.dumps({"since": date_from, "until": date_to}),
                "time_increment": 1,
                "limit": 500,
            }
        )
        daily = []
        for row in rows:
            parsed = parse_insights(row)
            parsed["date"] = row.get("date_start", "")
            daily.append(parsed)
        daily.sort(key=lambda x: x["date"])

        # Total agregado
        totals = _aggregate_daily_total(rows)

        # Buscar dados di&aacute;rios da campanha (para comparativo)
        campaign_id = (ad_info.get("campaign") or {}).get("id", "")
        campaign_daily = []
        if campaign_id:
            try:
                camp_rows = meta_get_all_pages(
                    f"{campaign_id}/insights",
                    {
                        "fields": INSIGHT_FIELDS_DAILY,
                        "time_range": json.dumps({"since": date_from, "until": date_to}),
                        "time_increment": 1,
                        "limit": 500,
                    }
                )
                for row in camp_rows:
                    parsed = parse_insights(row)
                    parsed["date"] = row.get("date_start", "")
                    campaign_daily.append(parsed)
                campaign_daily.sort(key=lambda x: x["date"])
            except Exception as e:
                print(f"[WARN] Falha ao buscar insights di&aacute;rios da campanha {campaign_id}: {e}")

        response = {
            "ok": True,
            "ad": {
                "id": ad_info.get("id"),
                "name": ad_info.get("name"),
                "status": ad_info.get("status"),
                "created_time": ad_info.get("created_time"),
                "campaign_name": (ad_info.get("campaign") or {}).get("name", ""),
                "campaign_id": campaign_id,
                "thumbnail_url": (ad_info.get("creative") or {}).get("thumbnail_url", ""),
                "creative_name": (ad_info.get("creative") or {}).get("name", ""),
                "story_id": (ad_info.get("creative") or {}).get("effective_object_story_id", "") or _find_story_id(ad_info),
                "page_id": ((ad_info.get("creative") or {}).get("object_story_spec") or {}).get("page_id", ""),
                "instagram_user_id": ((ad_info.get("creative") or {}).get("object_story_spec") or {}).get("instagram_user_id", ""),
                "ad_manager_url": f"https://adsmanager.facebook.com/adsmanager/manage/ads?act={ACCOUNT_ID.replace('act_','')}&business_id=984796621536780&selected_campaign_ids={campaign_id}&selected_ad_ids={ad_id}&filter_set=SEARCH_BY_ADGROUP_IDS-STRING_SET%1EANY%1E%5B%22{ad_id}%22%5D",
            },
            "daily": daily,
            "totals": totals,
            "campaign_daily": campaign_daily,
        }
        set_cached(cache_key, response, ttl_hours=_cache_ttl_for_range(date_from, date_to))
        return jsonify(response)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/dashboard/all-creatives")
@login_required
def api_all_creatives():
    """Busca criativos de TODAS as campanhas do tipo selecionado com métricas avançadas."""
    try:
        date_from = request.args.get("date_from", _default_date_from())
        date_to = request.args.get("date_to", _yesterday())
        camp_status = request.args.get("camp_status", "active")
        force = request.args.get("force", "false") == "true"
        camp_type = _camp_type_from_request()
        g.camp_type = camp_type

        # Viewer: so pode usar dados ja em cache (evita acionar chamadas pesadas a API).
        # Se o cache nao tiver o range solicitado, retorna erro pedindo ranges padrao.
        is_viewer = session.get("role") == "viewer"

        cache_key = f"all_creatives_v8_{camp_type}_{camp_status}_{date_from}_{date_to}"
        if not force:
            cached = get_cached(cache_key)
            if cached:
                return jsonify(cached)

        if is_viewer:
            # Viewer pode acionar a API apenas para os ranges pre-aprovados (1/7/14/30d
            # terminando em ontem). Primeira chamada popula o cache, demais vem do cache.
            def _is_approved_viewer_range():
                try:
                    d_from = datetime.strptime(date_from, "%Y-%m-%d")
                    d_to = datetime.strptime(date_to, "%Y-%m-%d")
                    diff = (d_to - d_from).days + 1
                    yesterday = (_now_br() - timedelta(days=1)).date()
                    if d_to.date() != yesterday:
                        return False
                    return diff in (1, 7, 14, 30)
                except Exception:
                    return False

            if not _is_approved_viewer_range():
                return jsonify({
                    "ok": False,
                    "error": "Periodo nao disponivel para o perfil Visualizador. Escolha 1d, 7d, 14d ou 30d.",
                    "viewer_cache_only": True,
                }), 403
            # Range permitido: segue adiante (chama API e cacheia)

        sales_campaigns = _fetch_type_campaigns(
            camp_type, "id,name,objective", _camp_status_filter(camp_status)
        )

        warnings = []
        result = _fetch_creatives_for_campaigns(sales_campaigns, date_from, date_to, warnings)
        response = {"ok": True, "data": result, "warnings": warnings}

        # Politica de cache:
        # - Sucesso total (sem warnings): cacheia com TTL normal
        # - Sucesso parcial (teve warnings mas result > 0): cacheia com TTL/3
        #   pra dar chance de revalidar cedo. Melhor ter dados parciais em cache
        #   do que fazer todas as calls de novo na proxima abertura.
        # - Falha total (warnings E result vazio): NAO cacheia. Antes cacheava
        #   cache vazio com TTL/3 = 6h+ — usuario via tela em branco por
        #   horas ate a proxima revalidacao. Agora a proxima tentativa forca
        #   refetch direto.
        if warnings and not result:
            print(f"[ALL-CREATIVES] Pulando cache: {len(warnings)} warnings + 0 resultados (forca refetch na proxima)")
            return jsonify(response)
        ttl = _cache_ttl_for_range(date_from, date_to)
        if warnings:
            ttl = max(0.5, ttl / 3)
        set_cached(cache_key, response, ttl_hours=ttl)
        return jsonify(response)

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/dashboard/comparison")
@not_viewer_required
def api_comparison():
    """Insights diários de múltiplas campanhas para comparação."""
    try:
        campaign_ids = request.args.get("ids", "").split(",")
        campaign_ids = [cid.strip() for cid in campaign_ids if cid.strip()]
        date_from = request.args.get("date_from", _default_date_from())
        date_to = request.args.get("date_to", _yesterday())
        g.camp_type = _camp_type_from_request()

        blocked = _enforce_range_for_role(date_from, date_to)
        if blocked:
            return blocked

        if not campaign_ids:
            return jsonify({"ok": False, "error": "Nenhuma campanha selecionada"}), 400

        result = {}
        for cid in campaign_ids:
            rows = meta_get_all_pages(
                f"{cid}/insights",
                {
                    "fields": "campaign_name,"+INSIGHT_FIELDS_DAILY,
                    "time_range": json.dumps({"since": date_from, "until": date_to}),
                    "time_increment": 1,
                    "limit": 500,
                }
            )
            daily = []
            name = ""
            for row in rows:
                parsed = parse_insights(row)
                parsed["date"] = row.get("date_start", "")
                daily.append(parsed)
                if not name:
                    name = row.get("campaign_name", cid)
            daily.sort(key=lambda x: x["date"])
            result[cid] = {"name": name, "daily": daily}

        return jsonify({"ok": True, "data": result})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


# ── Cache management ──────────────────────────────────────────────────

@app.route("/api/cache/stats")
@login_required
def api_cache_stats():
    return jsonify({"ok": True, **cache_stats()})


@app.route("/api/cache/clear", methods=["POST"])
@login_required
def api_cache_clear():
    clear_cache()
    return jsonify({"ok": True, "message": "Cache limpo"})


@app.route("/api/cache/refresh", methods=["POST"])
@login_required
def api_cache_refresh():
    """Força atualização do cache para o período padrão (30d até ontem)."""
    try:
        clear_cache()
        date_from = _default_date_from()
        date_to = _yesterday()

        # Pré-carregar campaigns
        campaigns = meta_get_all_pages(
            f"{ACCOUNT_ID}/campaigns",
            {"fields": "id,name,status,objective,daily_budget,lifetime_budget,start_time,created_time",
             "effective_status": '["ACTIVE"]'}
        )
        sales = [c for c in campaigns if c.get("objective") == "OUTCOME_SALES"]
        print(f"[REFRESH] {len(sales)} campanhas de vendas encontradas")

        # Pré-carregar daily summary
        with app.test_request_context(f"/api/dashboard/daily-summary?date_from={date_from}&date_to={date_to}&force=true"):
            session["logged_in"] = True
            api_daily_summary()
        print("[REFRESH] daily-summary cacheado")

        return jsonify({"ok": True, "message": f"Cache atualizado para {date_from} até {date_to}"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


def _scheduled_refresh():
    """Chamada pelo scheduler às 2h da manhã."""
    with app.app_context():
        try:
            clear_expired()
            date_from = _default_date_from()
            date_to = _yesterday()

            # Simular request context para os endpoints
            with app.test_request_context(f"/api/dashboard/campaigns?date_from={date_from}&date_to={date_to}&force=true"):
                session["logged_in"] = True
                api_campaigns()
            print("[SCHEDULER] campaigns cacheado")

            with app.test_request_context(f"/api/dashboard/daily-summary?date_from={date_from}&date_to={date_to}&force=true"):
                session["logged_in"] = True
                api_daily_summary()
            print("[SCHEDULER] daily-summary cacheado")

            print(f"[SCHEDULER] Cache atualizado: {date_from} até {date_to}")
        except Exception as e:
            print(f"[SCHEDULER] Erro: {e}")


def _find_story_id(ad_info):
    """Tenta encontrar o effective_object_story_id de fontes alternativas."""
    # Tentar em adcreatives
    adcreatives = ad_info.get("adcreatives", {}).get("data", [])
    for ac in adcreatives:
        sid = ac.get("effective_object_story_id", "")
        if sid:
            return sid
    # Tentar buscar o effective_object_story_id do criativo diretamente
    creative = ad_info.get("creative", {})
    creative_id = creative.get("id", "")
    if creative_id:
        try:
            cr_data = meta_get(creative_id, {
                "fields": "effective_object_story_id"
            })
            sid = cr_data.get("effective_object_story_id", "")
            if sid:
                return sid
        except Exception:
            pass
    return ""


# ── Post Comments ──────────────────────────────────────────────────────

@app.route("/api/dashboard/post/<path:story_id>/comments")
@login_required
def api_post_comments(story_id):
    """Busca comentarios de um post do Facebook/Instagram."""
    try:
        _enforce_rate_limit()
        data = meta_get(story_id + "/comments", {
            "fields": "id,message,from{id,name},created_time,like_count,comments{id,message,from{id,name},created_time}",
            "limit": "100",
            "order": "reverse_chronological"
        })
        comments = []
        for c in data.get("data", []):
            comment = {
                "message": c.get("message", ""),
                "from": c.get("from"),
                "created_time": c.get("created_time", ""),
                "like_count": c.get("like_count", 0),
                "replies": []
            }
            for r in (c.get("comments", {}).get("data", [])):
                comment["replies"].append({
                    "message": r.get("message", ""),
                    "from": r.get("from"),
                    "created_time": r.get("created_time", "")
                })
            comments.append(comment)
        return jsonify({"ok": True, "data": comments})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


# ── Breakdowns (idade, sexo, dia da semana) ────────────────────────────

@app.route("/api/dashboard/breakdowns")
@not_viewer_required
def api_breakdowns():
    """Retorna dados segmentados por idade, sexo e dia da semana."""
    try:
        date_from = request.args.get("date_from", _default_date_from())
        date_to = request.args.get("date_to", _yesterday())
        force = request.args.get("force", "false") == "true"
        camp_type = _camp_type_from_request()
        g.camp_type = camp_type

        blocked = _enforce_range_for_role(date_from, date_to)
        if blocked:
            return blocked
        campaign_id = request.args.get("campaign_id", "")

        cache_key = f"breakdowns_v7_{camp_type}_{campaign_id or 'all'}_{date_from}_{date_to}"
        if not force:
            cached = get_cached(cache_key)
            if cached:
                return jsonify(cached)

        conv_types = _get_conversion_types(camp_type)

        # Determinar endpoint: campanha especifica ou conta toda (filtrada por tipo)
        if campaign_id:
            endpoint = campaign_id + "/insights"
            base_params = {
                "time_range": json.dumps({"since": date_from, "until": date_to}),
            }
        else:
            endpoint = ACCOUNT_ID + "/insights"
            base_params = {
                "time_range": json.dumps({"since": date_from, "until": date_to}),
            }
            if camp_type in (CAMP_TYPE_METEORICOS, CAMP_TYPE_COMERCIAL, CAMP_TYPE_CRESCIMENTO):
                # Filtrar pelos IDs das campanhas do tipo (multi-conta)
                filtered = _fetch_type_campaigns(
                    camp_type, "id,name,objective", '["ACTIVE","PAUSED"]'
                )
                target_ids = [c["id"] for c in filtered]
                if not target_ids:
                    return jsonify({"ok": True, "age": [], "gender": [], "weekday": [], "campaign_id": "all"})
                # breakdowns usa 1 endpoint agregador: usa a conta com mais campanhas
                by_acc = {}
                for c in filtered:
                    by_acc.setdefault(c.get("_account_id") or ACCOUNT_ID, []).append(c["id"])
                main_acc = max(by_acc, key=lambda k: len(by_acc[k]))
                endpoint = main_acc + "/insights"
                base_params["filtering"] = json.dumps([{"field": "campaign.id", "operator": "IN", "value": by_acc[main_acc]}])
            else:
                base_params["filtering"] = json.dumps([{"field": "campaign.objective", "operator": "IN", "value": ["OUTCOME_SALES"]}])

        ins_fields = (
            "spend,impressions,clicks,actions,action_values,purchase_roas,website_purchase_roas,results,"
            + VIDEO_METRIC_FIELDS
        )

        def extract_purchase(row):
            conv = 0
            revenue = 0
            roas = 0
            for a in (row.get("actions") or []):
                if a.get("action_type") in conv_types:
                    conv = int(a.get("value", 0))
                    break
            # Leads nao tem revenue/ROAS
            if camp_type == CAMP_TYPE_VENDAS:
                for a in (row.get("action_values") or []):
                    if a.get("action_type") in conv_types:
                        revenue = float(a.get("value", 0))
                        break
                for a in (row.get("purchase_roas") or []):
                    if a.get("action_type") in conv_types:
                        roas = float(a.get("value", 0))
                        break
                if roas == 0:
                    for a in (row.get("website_purchase_roas") or []):
                        if a.get("action_type") in conv_types:
                            roas = float(a.get("value", 0))
                            break
                if roas == 0 and revenue > 0:
                    s = float(row.get("spend", 0))
                    if s > 0:
                        roas = round(revenue / s, 2)
            return conv, revenue, roas

        # 0. Buscar totais gerais para calcular ticket médio.
        # 'results' e 'unique_actions' sao necessarios pra extrair profile_visits
        # em Crescimento (coluna Resultados do Gerenciador). Sem esses campos
        # o total de visitas sai zero e a atribuicao nao distribui nada.
        _enforce_rate_limit()
        totals_data = meta_get_all_pages(endpoint, {
            **base_params,
            "fields": "spend,actions,action_values,results,unique_actions",
        })
        total_spend = sum(float(r.get("spend", 0)) for r in totals_data)
        total_conv = 0
        total_revenue = 0
        for r in totals_data:
            c, rev, _ = extract_purchase(r)
            total_conv += c
            total_revenue += rev
        ticket_medio = total_revenue / total_conv if total_conv > 0 else 0

        # Crescimento: breakdowns da API Meta nao entregam profile_visits nem follows
        # por demografia. Distribuimos o total proporcional ao gasto de cada bucket.
        crescimento_total_pv = 0
        crescimento_total_seguidores = 0
        if camp_type == CAMP_TYPE_CRESCIMENTO and total_spend > 0:
            # Consulta dedicada em level=campaign: o campo 'results' so vem
            # populado nesse nivel (em level=account ele some). Sem isso,
            # crescimento_total_pv ficava em 0 e atribuicao nao distribuia nada.
            try:
                pv_rows = meta_get_all_pages(endpoint, {
                    **base_params,
                    "fields": "spend,actions,results",
                    "level": "campaign",
                })
                crescimento_total_pv = sum(_extract_profile_visits_from_row(r) for r in pv_rows)
            except Exception as e:
                print(f"[BREAKDOWNS crescimento pv] Falha: {e}")
                crescimento_total_pv = sum(_extract_profile_visits_from_row(r) for r in totals_data)
            try:
                ig_follower, ig_non = fetch_ig_follower_gain_total(IG_PROFILE_ID_JRM, date_from, date_to)
                ig_net = max(0, ig_follower - ig_non)
                if ig_net > 0:
                    accounts = _get_accounts_for_type(camp_type)
                    total_acc_spend = _fetch_total_spend_all_accounts(accounts, date_from, date_to)
                    other_spend = max(0, total_acc_spend - total_spend)
                    share = compute_crescimento_share(total_spend, other_spend)
                    crescimento_total_seguidores = int(round(ig_net * share))
            except Exception as e:
                print(f"[BREAKDOWNS crescimento] Falha IG API: {e}")

        # Pra variar o custo por visita entre buckets (idade/sexo/dia), distribuimos
        # profile_visits proporcional a link_click do bucket, nao ao gasto. link_click
        # e a acao mais correlacionada com profile_visit_view (~1:1 nas campanhas IBC)
        # e e retornada por breakdown. Se usassemos gasto, o custo/visita daria igual
        # em todo bucket (constante = total_spend/total_pv).
        def _extract_proxy(row):
            """Extrai a acao proxy pra distribuir profile_visits no bucket."""
            for a in (row.get("actions") or []):
                if a.get("action_type") == "link_click":
                    try:
                        return int(float(a.get("value", 0) or 0))
                    except Exception:
                        return 0
            return int(row.get("clicks", 0) or 0)

        def _attrib_crescimento(spend, default_conv, default_pv, proxy_val=0, proxy_total=0):
            """Para Crescimento, distribui totais proporcional ao proxy (link_click)
            quando disponivel; fallback para gasto se proxy_total zero. Isso faz
            o custo por visita variar entre buckets, refletindo diferenca real
            de eficiencia por demografia.
            Caso os valores nativos da linha ja venham populados (ex: Meteoricos),
            usa-os direto. Retorna (conv, profile_visits)."""
            if camp_type != CAMP_TYPE_CRESCIMENTO:
                return default_conv, default_pv
            # Proxy-based share quando disponivel (varia por bucket -> custo/visita varia)
            if proxy_total > 0:
                share = proxy_val / proxy_total
            elif total_spend > 0:
                share = spend / total_spend
            else:
                return 0, default_pv
            seguidores = int(round(crescimento_total_seguidores * share))
            pv = default_pv if default_pv > 0 else int(round(crescimento_total_pv * share))
            return seguidores, pv

        # 1. Por idade
        _enforce_rate_limit()
        age_data = meta_get_all_pages(endpoint, {
            **base_params,
            "fields": ins_fields,
            "breakdowns": "age",
        })

        # 2. Por sexo
        _enforce_rate_limit()
        gender_data = meta_get_all_pages(endpoint, {
            **base_params,
            "fields": ins_fields,
            "breakdowns": "gender",
        })

        # 3. Por dia da semana (usando time_increment=1 e agrupando por weekday)
        _enforce_rate_limit()
        daily_data = meta_get_all_pages(endpoint, {
            **base_params,
            "fields": "spend,impressions,clicks,actions,action_values,purchase_roas," + VIDEO_METRIC_FIELDS,
            "time_increment": 1,
        })

        def calc_roas_fallback(conv, revenue, roas, spend):
            """Calcula ROAS usando ticket médio se API não retornar."""
            if roas > 0:
                return round(roas, 2), round(revenue, 2)
            if revenue > 0 and spend > 0:
                return round(revenue / spend, 2), round(revenue, 2)
            # Estimar receita usando ticket médio
            est_revenue = conv * ticket_medio
            est_roas = round(est_revenue / spend, 2) if spend > 0 else 0
            return est_roas, round(est_revenue, 2)

        # Processar idade
        age_proxy_total = sum(_extract_proxy(r) for r in age_data) if camp_type == CAMP_TYPE_CRESCIMENTO else 0
        age_result = []
        for row in age_data:
            conv, revenue, roas = extract_purchase(row)
            spend = float(row.get("spend", 0))
            roas, revenue = calc_roas_fallback(conv, revenue, roas, spend)
            pv_raw = _extract_profile_visits_from_row(row)
            conv, pv = _attrib_crescimento(spend, conv, pv_raw, _extract_proxy(row), age_proxy_total)
            # Nutricao: metricas de video vem direto no breakdown
            tp = _extract_video_metric(row, "video_thruplay_watched_actions")
            vplays = _extract_video_metric(row, "video_play_actions")
            vp100 = _extract_video_metric(row, "video_p100_watched_actions")
            age_result.append({
                "age": row.get("age", "?"),
                "spend": round(spend, 2),
                "impressions": int(row.get("impressions", 0)),
                "clicks": int(row.get("clicks", 0)),
                "conversions": conv,
                "revenue": round(revenue, 2),
                "roas": roas,
                "cpa": round(spend / conv, 2) if conv > 0 else 0,
                "profile_visits": pv,
                "cost_per_profile_visit": round(spend / pv, 2) if pv > 0 else 0,
                "video_thruplay": tp,
                "video_plays": vplays,
                "video_p100": vp100,
                "cost_per_thruplay": round(spend / tp, 2) if tp > 0 else 0,
            })

        # Processar sexo
        gender_proxy_total = sum(_extract_proxy(r) for r in gender_data) if camp_type == CAMP_TYPE_CRESCIMENTO else 0
        gender_result = []
        gender_labels = {"male": "Masculino", "female": "Feminino", "unknown": "Desconhecido"}
        for row in gender_data:
            conv, revenue, roas = extract_purchase(row)
            spend = float(row.get("spend", 0))
            roas, revenue = calc_roas_fallback(conv, revenue, roas, spend)
            pv_raw = _extract_profile_visits_from_row(row)
            conv, pv = _attrib_crescimento(spend, conv, pv_raw, _extract_proxy(row), gender_proxy_total)
            tp = _extract_video_metric(row, "video_thruplay_watched_actions")
            vplays = _extract_video_metric(row, "video_play_actions")
            vp100 = _extract_video_metric(row, "video_p100_watched_actions")
            gender_result.append({
                "gender": gender_labels.get(row.get("gender", ""), row.get("gender", "?")),
                "spend": round(spend, 2),
                "impressions": int(row.get("impressions", 0)),
                "clicks": int(row.get("clicks", 0)),
                "conversions": conv,
                "revenue": round(revenue, 2),
                "roas": roas,
                "cpa": round(spend / conv, 2) if conv > 0 else 0,
                "profile_visits": pv,
                "cost_per_profile_visit": round(spend / pv, 2) if pv > 0 else 0,
                "video_thruplay": tp,
                "video_plays": vplays,
                "video_p100": vp100,
                "cost_per_thruplay": round(spend / tp, 2) if tp > 0 else 0,
            })

        # Processar dia da semana
        weekdays = {0: "Segunda", 1: "Terca", 2: "Quarta", 3: "Quinta", 4: "Sexta", 5: "Sabado", 6: "Domingo"}
        weekday_totals = {i: {"spend": 0, "impressions": 0, "clicks": 0, "conversions": 0, "revenue": 0, "profile_visits": 0, "proxy": 0, "video_thruplay": 0, "video_plays": 0, "video_p100": 0, "days": 0} for i in range(7)}

        for row in daily_data:
            date_str = row.get("date_start", "")
            if not date_str:
                continue
            try:
                from datetime import datetime as dt
                d = dt.strptime(date_str, "%Y-%m-%d")
                wd = d.weekday()
            except Exception:
                continue
            conv, revenue, roas = extract_purchase(row)
            weekday_totals[wd]["spend"] += float(row.get("spend", 0))
            weekday_totals[wd]["impressions"] += int(row.get("impressions", 0))
            weekday_totals[wd]["clicks"] += int(row.get("clicks", 0))
            weekday_totals[wd]["conversions"] += conv
            weekday_totals[wd]["revenue"] += revenue
            weekday_totals[wd]["profile_visits"] += _extract_profile_visits_from_row(row)
            weekday_totals[wd]["proxy"] += _extract_proxy(row)
            weekday_totals[wd]["video_thruplay"] += _extract_video_metric(row, "video_thruplay_watched_actions")
            weekday_totals[wd]["video_plays"] += _extract_video_metric(row, "video_play_actions")
            weekday_totals[wd]["video_p100"] += _extract_video_metric(row, "video_p100_watched_actions")
            weekday_totals[wd]["days"] += 1

        weekday_proxy_total = sum(weekday_totals[i]["proxy"] for i in range(7)) if camp_type == CAMP_TYPE_CRESCIMENTO else 0

        weekday_result = []
        for i in range(7):
            t = weekday_totals[i]
            days_count = max(t["days"], 1)
            rev = t["revenue"]
            if rev == 0 and t["conversions"] > 0:
                rev = t["conversions"] * ticket_medio
            conv, pv = _attrib_crescimento(t["spend"], t["conversions"], t["profile_visits"], t.get("proxy", 0), weekday_proxy_total)
            tp = t["video_thruplay"]
            weekday_result.append({
                "day": weekdays[i],
                "day_num": i,
                "spend": round(t["spend"], 2),
                "spend_avg": round(t["spend"] / days_count, 2),
                "conversions": conv,
                "conv_avg": round(conv / days_count, 1),
                "revenue": round(rev, 2),
                "roas": round(rev / t["spend"], 2) if t["spend"] > 0 else 0,
                "impressions": t["impressions"],
                "clicks": t["clicks"],
                "profile_visits": pv,
                "profile_visits_avg": round(pv / days_count, 1),
                "cost_per_profile_visit": round(t["spend"] / pv, 2) if pv > 0 else 0,
                "video_thruplay": tp,
                "video_thruplay_avg": round(tp / days_count, 1),
                "video_plays": t["video_plays"],
                "video_p100": t["video_p100"],
                "cost_per_thruplay": round(t["spend"] / tp, 2) if tp > 0 else 0,
            })

        response = {
            "ok": True,
            "age": age_result,
            "gender": gender_result,
            "weekday": weekday_result,
            "campaign_id": campaign_id or "all",
        }
        set_cached(cache_key, response, ttl_hours=_cache_ttl_for_range(date_from, date_to))
        return jsonify(response)

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


# ── Auto Update ────────────────────────────────────────────────────────

@app.route("/api/admin/check-update")
def api_check_update():
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    try:
        import subprocess
        cwd = os.path.dirname(__file__)
        fetch = subprocess.run(["git", "fetch", "origin", "master"], capture_output=True, text=True, timeout=30, cwd=cwd)
        fetch_ok = fetch.returncode == 0
        fetch_err = (fetch.stderr or fetch.stdout or "").strip() if not fetch_ok else ""
        local = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, cwd=cwd).stdout.strip()
        remote = subprocess.run(["git", "rev-parse", "origin/master"], capture_output=True, text=True, cwd=cwd).stdout.strip()
        has_update = bool(local and remote and local != remote)
        return jsonify({
            "ok": True,
            "has_update": has_update,
            "fetch_ok": fetch_ok,
            "fetch_err": fetch_err,
            "local": local[:7] if local else "?",
            "remote": remote[:7] if remote else "?",
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/admin/apply-update", methods=["POST"])
def api_apply_update():
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    try:
        import subprocess
        import threading
        cwd = os.path.dirname(__file__)
        subprocess.run(["git", "stash"], capture_output=True, text=True, cwd=cwd, timeout=15)
        pull = subprocess.run(["git", "pull", "origin", "master"], capture_output=True, text=True, cwd=cwd, timeout=60)
        # pip do venv ativo (gunicorn pode nao ter pip no PATH em producao)
        pip_bin = os.path.join(os.path.dirname(sys.executable), "pip")
        try:
            subprocess.run([pip_bin, "install", "-r", "requirements.txt"], capture_output=True, text=True, cwd=cwd, timeout=120)
        except FileNotFoundError:
            subprocess.run(["pip", "install", "-r", "requirements.txt"], capture_output=True, text=True, cwd=cwd, timeout=120)

        # Reinicia o servico em thread separada com delay curto,
        # para o response HTTP conseguir voltar antes do processo cair.
        # Depende de /etc/sudoers.d/ibc-dash permitir systemctl restart ibc-dash sem senha.
        # Usa /usr/bin/systemctl (path real no Ubuntu/Debian modernos) com fallback para /bin.
        systemctl_bin = "/usr/bin/systemctl"
        if not os.path.exists(systemctl_bin):
            systemctl_bin = "/bin/systemctl"

        def _delayed_restart():
            time.sleep(1.5)
            try:
                subprocess.run(["sudo", "-n", systemctl_bin, "restart", "ibc-dash"], capture_output=True, text=True, timeout=30)
            except Exception as e:
                print(f"[UPDATE] Falha ao reiniciar servico: {e}")

        restart_available = False
        restart_err = ""
        try:
            # Testa se conseguimos rodar sudo sem senha para o comando do systemctl
            test = subprocess.run(["sudo", "-n", systemctl_bin, "is-active", "ibc-dash"], capture_output=True, text=True, timeout=5)
            restart_available = test.returncode == 0
            if not restart_available:
                restart_err = (test.stderr or test.stdout or "").strip()[:200]
        except Exception as e:
            restart_available = False
            restart_err = str(e)[:200]

        if restart_available:
            threading.Thread(target=_delayed_restart, daemon=True).start()
            msg = "Atualizado! Reiniciando o servico em 2 segundos — recarregue a pagina em 10s."
        else:
            diag = f" (diag: {restart_err})" if restart_err else ""
            msg = "Codigo atualizado. Reinicie o servico manualmente (sudo systemctl restart ibc-dash) para aplicar." + diag

        return jsonify({
            "ok": True,
            "message": msg,
            "restart_scheduled": restart_available,
            "restart_err": restart_err,
            "systemctl_path": systemctl_bin,
            "git_output": pull.stdout + pull.stderr,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/admin/update-history")
def api_update_history():
    """Retorna os ultimos commits aplicados no servidor (git log)."""
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    try:
        import subprocess
        cwd = os.path.dirname(__file__)
        # %h = hash curto, %aI = autor ISO strict, %s = assunto, separados por |
        result = subprocess.run(
            ["git", "log", "-n", "20", "--format=%h|%aI|%s"],
            capture_output=True, text=True, timeout=15, cwd=cwd
        )
        if result.returncode != 0:
            return jsonify({"ok": False, "error": "Git nao disponivel: " + result.stderr.strip()})

        commits = []
        for line in result.stdout.strip().split("\n"):
            if not line:
                continue
            parts = line.split("|", 2)
            if len(parts) == 3:
                commits.append({"hash": parts[0], "date": parts[1], "message": parts[2]})

        # Data da ultima aplicacao: mtime do .git/FETCH_HEAD (quando foi o ultimo git fetch/pull)
        last_applied = None
        fetch_head = os.path.join(cwd, ".git", "FETCH_HEAD")
        if os.path.exists(fetch_head):
            # Timezone explicito (UTC) para conversao correta no frontend
            last_applied = datetime.fromtimestamp(os.path.getmtime(fetch_head), tz=timezone.utc).isoformat(timespec="seconds")

        return jsonify({"ok": True, "commits": commits, "last_applied": last_applied})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ── Rate Limit Info ────────────────────────────────────────────────────

@app.route("/api/dashboard/rate-limit")
@login_required
def api_dash_rate_limit():
    return jsonify({"ok": True, "data": get_dashboard_rate_info()})


# ── Admin ──────────────────────────────────────────────────────────────

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "ibcadmin2026!")

@app.route("/admin")
def admin_page():
    if not session.get("logged_in"):
        return redirect(url_for("login_page"))
    # Super admin real sempre entra (mesmo em preview com role viewer/viewer2 ativa)
    if _is_super_admin(session.get("username")):
        return render_template("admin.html")
    # Demais perfis: viewer/viewer2 bloqueado, admin passa
    if session.get("role") in ("viewer", "viewer2"):
        return redirect(url_for("dashboard"))
    return render_template("admin.html")

@app.route("/admin/reset-password")
def admin_reset_password():
    if not session.get("logged_in"):
        return redirect(url_for("login_page"))
    return render_template("admin_reset.html")


@app.route("/api/admin/reset-password", methods=["POST"])
def api_reset_password():
    if not session.get("logged_in"):
        return jsonify({"ok": False, "error": "Nao autorizado"}), 401
    data = request.get_json()
    new_pass = data.get("new_password", "").strip()
    if len(new_pass) < 6:
        return jsonify({"ok": False, "error": "Senha deve ter pelo menos 6 caracteres"}), 400
    users = _get_users()
    username = session.get("username")
    if username in users:
        users[username]["password"] = new_pass
        users[username]["must_reset"] = False
        _save_users(users)
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "Usuario nao encontrado"}), 404


@app.route("/api/admin/users", methods=["GET"])
def api_list_users():
    if not session.get("logged_in") or not _is_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    users = _get_users()
    requester = session.get("username")
    is_super = _is_super_admin(requester)

    # Indice de presenca: mapa email -> (ultimo_ts, evento) a partir do activity_log
    presence_map = {}
    try:
        if os.path.exists(ACTIVITY_LOG_FILE):
            with open(ACTIVITY_LOG_FILE, "r") as f:
                log = json.load(f)
            for e in log:
                em = (e.get("email") or "").lower()
                ts = e.get("ts") or ""
                ev = e.get("event") or ""
                if not em or not ts:
                    continue
                cur = presence_map.get(em)
                if not cur or ts > cur[0]:
                    presence_map[em] = (ts, ev)
    except Exception as ex:
        print(f"[PRESENCE] Falha lendo log: {ex}")

    now = datetime.now(timezone.utc)
    def _compute_presence(email):
        entry = presence_map.get(email.lower())
        if not entry:
            return ("offline", "")
        ts, ev = entry
        # Logout explicito = offline independente de tempo
        if ev == "logout":
            return ("offline", ts)
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            diff = (now - dt).total_seconds()
        except Exception:
            return ("offline", ts)
        # Heartbeat e a cada 60s — tolerancia generosa pro primeiro bucket
        if diff < 120:
            return ("online", ts)
        if diff < 600:
            return ("standby", ts)
        return ("offline", ts)

    result = []
    for email, u in users.items():
        # Super admin é invisível para admins secundários
        if u.get("role") == "super_admin" and not is_super:
            continue
        presence, last_seen = _compute_presence(email)
        result.append({
            "email": email,
            "name": u.get("name", ""),
            "role": u.get("role", "viewer"),
            "must_reset": u.get("must_reset", False),
            "last_login": u.get("last_login", ""),
            "created_by": u.get("created_by", ""),
            "created_at": u.get("created_at", ""),
            "presence": presence,
            "last_seen": last_seen,
            "password": u.get("password", "") if is_super else "********"  # Só super admin vê senhas
        })
    return jsonify({"ok": True, "data": result, "is_super": is_super})


@app.route("/api/admin/users/create", methods=["POST"])
def api_create_user():
    if not session.get("logged_in") or not _is_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    data = request.get_json()
    email = data.get("email", "").strip().lower()
    password = data.get("password", "").strip()
    role = data.get("role", "viewer")
    if not email or not password:
        return jsonify({"ok": False, "error": "Email e senha obrigatorios"}), 400
    if role == "super_admin":
        return jsonify({"ok": False, "error": "Nao e possivel criar super admin"}), 400
    users = _get_users()
    if email in users:
        return jsonify({"ok": False, "error": "Usuario ja existe"}), 400
    users[email] = {
        "name": data.get("name", "").strip(),
        "password": password,
        "role": role,
        "must_reset": True,
        "created_by": session.get("username", ""),
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    _save_users(users)
    return jsonify({"ok": True})


@app.route("/api/dashboard/heartbeat", methods=["POST"])
def api_heartbeat():
    """Chamado pelo frontend a cada ~60s enquanto o dashboard esta aberto.
    Usado pra calcular tempo online (diferenca entre login e ultimo heartbeat)."""
    if not session.get("logged_in"):
        return jsonify({"ok": False}), 401
    log_activity(session.get("username", ""), "heartbeat", session_id=session.get("session_id", ""))
    return jsonify({"ok": True})


@app.route("/api/admin/users/<path:email>/log")
def api_user_log(email):
    """Retorna historico de atividade do usuario, agrupado por blocos de atividade.
    Um novo bloco começa quando ha gap > 15min entre eventos (usuario ficou inativo
    e voltou sem fazer logout). Acessivel apenas pelo super admin."""
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403

    email = (email or "").strip().lower()
    if not os.path.exists(ACTIVITY_LOG_FILE):
        return jsonify({"ok": True, "blocks": []})

    try:
        with open(ACTIVITY_LOG_FILE, "r") as f:
            log = json.load(f)
    except Exception:
        log = []

    # Filtra e ordena eventos do usuario por timestamp
    user_events = sorted(
        [e for e in log if (e.get("email") or "").lower() == email],
        key=lambda x: x.get("ts", "")
    )

    # Detecta blocos de atividade: gap > 15min entre eventos consecutivos = novo bloco
    INACTIVITY_GAP_SEC = 15 * 60
    blocks_raw = []
    current = []
    for ev in user_events:
        if not current:
            current = [ev]
            continue
        prev_ts = current[-1].get("ts", "")
        curr_ts = ev.get("ts", "")
        gap = 0
        try:
            if prev_ts and curr_ts:
                dp = datetime.fromisoformat(prev_ts.replace("Z", "+00:00"))
                dc = datetime.fromisoformat(curr_ts.replace("Z", "+00:00"))
                gap = (dc - dp).total_seconds()
        except Exception:
            pass
        # Logout fecha o bloco atual; login apos gap tb abre novo bloco
        if ev.get("event") == "logout":
            current.append(ev)
            blocks_raw.append(current)
            current = []
        elif gap > INACTIVITY_GAP_SEC:
            blocks_raw.append(current)
            current = [ev]
        else:
            current.append(ev)
    if current:
        blocks_raw.append(current)

    # Monta resumo de cada bloco
    blocks = []
    for blk in blocks_raw:
        if not blk:
            continue
        start = blk[0].get("ts", "")
        end = blk[-1].get("ts", "")
        ip = blk[0].get("ip", "")
        has_logout = any(e.get("event") == "logout" for e in blk)
        has_login = any(e.get("event") == "login" for e in blk)
        duration = None
        try:
            if start and end:
                ds = datetime.fromisoformat(start.replace("Z", "+00:00"))
                de = datetime.fromisoformat(end.replace("Z", "+00:00"))
                duration = int((de - ds).total_seconds())
        except Exception:
            pass
        blocks.append({
            "start": start,
            "end": end,
            "ip": ip,
            "duration_seconds": duration,
            "closed": has_logout,
            "is_login": has_login,
        })

    blocks.sort(key=lambda b: b.get("start") or "", reverse=True)

    # Timeline detalhado: fusiona login/logout/heartbeat + API calls + gaps de ausencia.
    # Aceita filter=all|login|api|away pra o frontend.
    filter_kind = (request.args.get("filter") or "all").lower()
    events = []

    # 1) Eventos do activity_log (login/logout/heartbeat)
    for ev in user_events[-500:]:  # cap pra nao inchar resposta
        ev_type = ev.get("event") or "heartbeat"
        kind = "login" if ev_type in ("login", "logout") else "heartbeat"
        events.append({
            "ts": ev.get("ts", ""),
            "kind": kind,
            "event": ev_type,
            "ip": ev.get("ip", ""),
        })

    # 2) Chamadas de API do usuario (via api_usage_log)
    api_calls = get_api_calls_for_user(email, days=7, limit=500)
    for c in api_calls:
        events.append({
            "ts": c.get("ts", ""),
            "kind": "api",
            "endpoint": c.get("endpoint", ""),
            "camp_type": c.get("camp_type"),
            "meta_calls": c.get("meta_calls", 0),
            "cache_hit": bool(c.get("cache_hit")),
            "duration_ms": c.get("duration_ms"),
            "worst_buc_pct": c.get("worst_buc_pct"),
        })

    # 3) Periodos de ausencia (gaps >15min entre atividades consecutivas)
    all_ts = sorted(
        [e.get("ts") for e in user_events if e.get("ts")]
        + [c.get("ts") for c in api_calls if c.get("ts")]
    )
    AWAY_THRESHOLD = 15 * 60
    for i in range(1, len(all_ts)):
        try:
            dp = datetime.fromisoformat(all_ts[i-1].replace("Z", "+00:00"))
            dc = datetime.fromisoformat(all_ts[i].replace("Z", "+00:00"))
            gap = (dc - dp).total_seconds()
            if gap > AWAY_THRESHOLD:
                events.append({
                    "ts": all_ts[i-1],
                    "kind": "away",
                    "duration_seconds": int(gap),
                    "end_ts": all_ts[i],
                })
        except Exception:
            pass

    # Aplica filtro
    if filter_kind != "all":
        events = [e for e in events if e.get("kind") == filter_kind]

    # Ordena mais recente primeiro
    events.sort(key=lambda e: e.get("ts") or "", reverse=True)
    events = events[:300]  # cap de resposta

    return jsonify({"ok": True, "blocks": blocks[:60], "events": events, "filter": filter_kind})


@app.route("/api/admin/preview-as", methods=["POST"])
def api_preview_as():
    """Super admin simula visao de outro perfil. Nao muda permissoes reais
    (o email continua f4cure@... e ele pode sair quando quiser). Somente
    a UI/endpoints de dashboard passam a enxergar o role escolhido."""
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    data = request.get_json() or {}
    target = (data.get("role") or "").strip()
    if target not in ("viewer", "viewer2", "admin", "super_admin"):
        return jsonify({"ok": False, "error": "Perfil invalido"}), 400
    # Garante que temos real_role salvo (fluxo antigo pode nao ter setado)
    if not session.get("real_role"):
        session["real_role"] = "super_admin"
    session["role"] = target
    return jsonify({"ok": True, "role": target})


@app.route("/api/admin/preview-exit", methods=["POST"])
def api_preview_exit():
    """Volta a usar o role real do usuario."""
    if not session.get("logged_in"):
        return jsonify({"ok": False, "error": "Nao autorizado"}), 401
    real = session.get("real_role")
    if not real:
        real = "super_admin" if _is_super_admin(session.get("username")) else "viewer"
        session["real_role"] = real
    session["role"] = real
    return jsonify({"ok": True, "role": real})


@app.route("/api/admin/users/backfill-creators", methods=["POST"])
def api_backfill_creators():
    """Preenche created_by/created_at para usuarios antigos que ainda nao tem
    esses campos. Usa o super admin logado como criador e o horario atual."""
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403

    # Le o arquivo direto (nao pelo _load_users, pra nao contaminar com o super admin injetado)
    if not os.path.exists(USERS_FILE):
        return jsonify({"ok": True, "updated": 0})

    with open(USERS_FILE, "r") as f:
        users = json.load(f)

    requester = session.get("username")
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    updated = 0
    for email, u in users.items():
        if not u.get("created_by"):
            u["created_by"] = requester
            updated += 1
        if not u.get("created_at"):
            u["created_at"] = now

    if updated:
        with open(USERS_FILE, "w") as f:
            json.dump(users, f, indent=2)

    return jsonify({"ok": True, "updated": updated, "creator": requester})


@app.route("/api/admin/users/update", methods=["POST"])
def api_update_user():
    if not session.get("logged_in") or not _is_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    data = request.get_json()
    email = data.get("email", "").strip().lower()
    requester = session.get("username")
    users = _get_users()
    if email not in users:
        return jsonify({"ok": False, "error": "Usuario nao encontrado"}), 404
    # Super admin só pode ser editado por ele mesmo
    if email == SUPER_ADMIN_EMAIL and requester != SUPER_ADMIN_EMAIL:
        return jsonify({"ok": False, "error": "Sem permissao"}), 403
    # Ninguém pode excluir o super admin
    if email == SUPER_ADMIN_EMAIL and data.get("action") == "delete":
        return jsonify({"ok": False, "error": "Nao pode excluir o admin principal"}), 400
    # Ninguém pode mudar role para super_admin
    if data.get("role") == "super_admin":
        return jsonify({"ok": False, "error": "Nao e possivel criar super admin"}), 400
    if data.get("action") == "delete":
        del users[email]
    elif data.get("action") == "reset":
        users[email]["must_reset"] = True
    elif data.get("new_password"):
        users[email]["password"] = data["new_password"]
    elif data.get("role"):
        users[email]["role"] = data["role"]
    elif data.get("action") == "edit":
        # Edicao de dados: nome e/ou email. Se email mudou, renomeia a chave.
        new_name = data.get("name", None)
        new_email = (data.get("new_email") or "").strip().lower()
        if new_name is not None:
            users[email]["name"] = new_name.strip()
        if new_email and new_email != email:
            if new_email in users:
                return jsonify({"ok": False, "error": "Ja existe um usuario com esse email"}), 400
            if email == SUPER_ADMIN_EMAIL:
                return jsonify({"ok": False, "error": "Nao e possivel alterar o email do admin principal"}), 400
            users[new_email] = users.pop(email)
    elif "name" in data:
        users[email]["name"] = data.get("name", "").strip()
    _save_users(users)
    return jsonify({"ok": True})


@app.route("/api/admin/token-expiry")
def api_token_expiry():
    if not session.get("logged_in"):
        return jsonify({"ok": False}), 401
    try:
        resp = requests.get(f"{BASE_URL}/debug_token", params={
            "input_token": TOKEN, "access_token": TOKEN
        }, timeout=10)
        data = resp.json().get("data", {})
        expires_at = data.get("expires_at", 0)
        if expires_at:
            from datetime import datetime
            expires_date = datetime.fromtimestamp(expires_at)
            days_left = (expires_date - datetime.now()).days
            return jsonify({"ok": True, "days_left": days_left, "expires_at": expires_date.strftime("%Y-%m-%d")})
        return jsonify({"ok": True, "days_left": -1})
    except Exception:
        return jsonify({"ok": True, "days_left": -1})


# ── Meteoricos Preview (diagnostico temporario) ───────────────────────
# ── Gerenciamento de contas de anuncio (multi-account) ────────────────

# Mapeamento de account_status da Meta API para label humano
_META_ACCOUNT_STATUS = {
    1: ("Ativa", "ACTIVE"),
    2: ("Desativada", "DISABLED"),
    3: ("Nao liquidada", "UNSETTLED"),
    7: ("Revisao de risco", "PENDING_RISK_REVIEW"),
    8: ("Pag. pendente", "PENDING_SETTLEMENT"),
    9: ("Em periodo de carencia", "IN_GRACE_PERIOD"),
    100: ("Fechamento pendente", "PENDING_CLOSURE"),
    101: ("Fechada", "CLOSED"),
}


def _fetch_account_meta(acc_id):
    """Consulta Meta API para obter nome e status da conta. Cache em memoria simples."""
    try:
        info = meta_get(acc_id, {"fields": "name,account_status,currency,timezone_name"})
        status_num = info.get("account_status", 0)
        status_label, status_key = _META_ACCOUNT_STATUS.get(status_num, (f"Status {status_num}", "UNKNOWN"))
        return {
            "id": acc_id,
            "name": info.get("name", acc_id),
            "status": status_key,
            "status_label": status_label,
            "currency": info.get("currency", ""),
            "timezone": info.get("timezone_name", ""),
            "error": None,
        }
    except Exception as e:
        return {
            "id": acc_id,
            "name": acc_id,
            "status": "ERROR",
            "status_label": "Erro ao consultar",
            "currency": "",
            "timezone": "",
            "error": str(e),
        }


@app.route("/api/admin/ad-accounts", methods=["GET"])
def api_ad_accounts_list():
    """Lista todas as contas sincronizadas (principal + extras) com metadados da Meta API."""
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    extras = _load_ad_accounts()
    extras_by_id = {a.get("id"): a for a in extras}

    # Lista unificada: principal primeiro, depois extras
    result = []
    seen = set()
    if ACCOUNT_ID:
        meta = _fetch_account_meta(ACCOUNT_ID)
        meta["is_main"] = True
        meta["label"] = meta.get("name") or ACCOUNT_ID
        # Principal sempre cobre todos os tipos (conta default)
        meta["camp_types"] = list(VALID_CAMP_TYPES)
        result.append(meta)
        seen.add(ACCOUNT_ID)

    for extra in extras:
        acc_id = (extra.get("id") or "").strip()
        if not acc_id or acc_id in seen:
            continue
        meta = _fetch_account_meta(acc_id)
        meta["is_main"] = False
        # Prefere nome da Meta. Label salvo so e usado se for customizado
        # (diferente do proprio ID — entradas antigas salvavam label=acc_id).
        stored_label = (extra.get("label") or "").strip()
        custom_label = stored_label if stored_label and stored_label != acc_id else ""
        meta["label"] = custom_label or meta.get("name") or acc_id
        meta["camp_types"] = extra.get("camp_types") or []
        meta["created_by"] = extra.get("created_by", "")
        meta["created_at"] = extra.get("created_at", "")
        result.append(meta)
        seen.add(acc_id)

    return jsonify({
        "ok": True,
        "accounts": result,
        "valid_camp_types": list(VALID_CAMP_TYPES),
    })


@app.route("/api/admin/ad-accounts", methods=["POST"])
def api_ad_accounts_add():
    """Adiciona uma conta extra. Body: {id, label, camp_types}."""
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    data = request.get_json() or {}
    acc_id = (data.get("id") or "").strip()
    label = (data.get("label") or "").strip()
    camp_types = data.get("camp_types") or []
    # Validacao
    if not acc_id:
        return jsonify({"ok": False, "error": "ID da conta e obrigatorio"}), 400
    # Normaliza: extrai apenas digitos e reconstroi act_<digitos>.
    # Absorve variantes como "act_123", "123", "act_=123", "act_ 123", etc.
    digits = "".join(ch for ch in acc_id if ch.isdigit())
    if not digits:
        return jsonify({"ok": False, "error": "ID da conta invalido (sem digitos)"}), 400
    acc_id = "act_" + digits
    if not isinstance(camp_types, list) or not camp_types:
        return jsonify({"ok": False, "error": "Selecione ao menos um tipo de campanha"}), 400
    for ct in camp_types:
        if ct not in VALID_CAMP_TYPES:
            return jsonify({"ok": False, "error": f"Tipo invalido: {ct}"}), 400
    if acc_id == ACCOUNT_ID:
        return jsonify({"ok": False, "error": "Esta e a conta principal (ja usada automaticamente)"}), 400

    accounts = _load_ad_accounts()
    # Se ja existe, atualiza; senao adiciona
    existing = next((a for a in accounts if a.get("id") == acc_id), None)
    if existing:
        existing["label"] = label or existing.get("label", "")
        existing["camp_types"] = camp_types
    else:
        accounts.append({
            "id": acc_id,
            "label": label,  # vazio => usa nome da Meta na exibicao
            "camp_types": camp_types,
            "created_by": session.get("username", ""),
            "created_at": _now_br().strftime("%Y-%m-%d"),
        })
    _save_ad_accounts(accounts)
    return jsonify({"ok": True, "extra_accounts": accounts})


@app.route("/api/admin/ad-accounts/<path:acc_id>", methods=["DELETE"])
def api_ad_accounts_delete(acc_id):
    """Remove uma conta extra."""
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    accounts = _load_ad_accounts()
    before = len(accounts)
    accounts = [a for a in accounts if a.get("id") != acc_id]
    if len(accounts) == before:
        return jsonify({"ok": False, "error": "Conta nao encontrada"}), 404
    _save_ad_accounts(accounts)
    return jsonify({"ok": True, "extra_accounts": accounts})


# ── Diagnostico de uso de API (historico 7 dias) ──────────────────────
@app.route("/api/admin/usage-stats")
def api_admin_usage_stats():
    """Retorna estatisticas de uso da API dos ultimos N dias.
    Query params: days (1-30), source (all|user|auto), user (email filter)."""
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    try:
        days = int(request.args.get("days", "7"))
        days = max(1, min(days, 30))
        source = request.args.get("source", "all")  # all, user, auto
        user_filter = request.args.get("user", "").strip()
        from_ts = request.args.get("from_ts", "").strip()
        stats = get_usage_stats(days=days, source=source, user_filter=user_filter, from_ts=from_ts)
        return jsonify({"ok": True, "data": stats})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


# ── Campanhas nao identificadas: lista campanhas de TODAS contas/tipos ──
# ── Diagnostico Crescimento: identifica action_type correto p/ seguidores ──
@app.route("/api/admin/ig-test")
def api_ig_test():
    """Testa a integracao com Instagram Graph API usando o token atual do .env.
    Retorna se o token tem as permissoes necessarias e se a IG API esta respondendo."""
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    try:
        now_br = _now_br()
        dt_to = (now_br - timedelta(days=1)).strftime("%Y-%m-%d")
        dt_from = (now_br - timedelta(days=30)).strftime("%Y-%m-%d")

        # 1. Checa permissoes do token via /debug_token
        token_info = {}
        try:
            debug_resp = requests.get(
                f"{BASE_URL}/debug_token",
                params={"input_token": TOKEN, "access_token": TOKEN},
                timeout=10
            ).json()
            token_info = debug_resp.get("data", {})
        except Exception as e:
            token_info = {"error": str(e)}

        scopes = token_info.get("scopes", [])
        has_ig_basic = "instagram_basic" in scopes
        has_ig_insights = "instagram_manage_insights" in scopes

        # 2. Tenta listar perfis IG conectados
        profiles_result = None
        profiles_error = None
        try:
            resp = meta_get("me/accounts", {
                "fields": "name,instagram_business_account{id,username,name}"
            })
            profiles_result = resp.get("data", [])
        except Exception as e:
            profiles_error = str(e)

        # 3. Tenta puxar ganho de seguidores do perfil JRM
        follower_follower = follower_non = 0
        follower_error = None
        try:
            follower_follower, follower_non = fetch_ig_follower_gain_total(
                IG_PROFILE_ID_JRM, dt_from, dt_to
            )
        except Exception as e:
            follower_error = str(e)

        # 4. Tenta puxar breakdown diario
        daily_gains = {}
        daily_error = None
        try:
            daily_gains = fetch_ig_follower_gain_by_day(IG_PROFILE_ID_JRM, dt_from, dt_to)
        except Exception as e:
            daily_error = str(e)

        return jsonify({
            "ok": True,
            "token_info": {
                "scopes": scopes,
                "has_instagram_basic": has_ig_basic,
                "has_instagram_manage_insights": has_ig_insights,
                "expires_at": token_info.get("expires_at", 0),
                "is_valid": token_info.get("is_valid", False),
            },
            "profiles": profiles_result,
            "profiles_error": profiles_error,
            "follower_gain_30d": {
                "follower": follower_follower,
                "non_follower": follower_non,
                "net_gain": follower_follower - follower_non,
                "error": follower_error,
            },
            "daily_gains": {
                "total_days": len(daily_gains),
                "total_net_gain": sum(daily_gains.values()) if daily_gains else 0,
                "sample": dict(list(daily_gains.items())[:7]) if daily_gains else {},
                "error": daily_error,
            },
            "ig_profile_id_used": IG_PROFILE_ID_JRM,
            "period": f"{dt_from} -> {dt_to}",
        })
    except Exception as e:
        import traceback
        return jsonify({"ok": False, "error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/admin/crescimento-preview")
def api_crescimento_preview():
    """Retorna rankings de actions/unique_actions/cost_per_action_type nas
    campanhas de CRESCIMENTO dos ultimos 30d. Usado para identificar qual
    campo/action_type representa 'Seguidores' no Gerenciador Meta."""
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    try:
        now_br = _now_br()
        dt_to = (now_br - timedelta(days=1)).strftime("%Y-%m-%d")
        dt_from = (now_br - timedelta(days=30)).strftime("%Y-%m-%d")

        filtered = _fetch_type_campaigns(
            CAMP_TYPE_CRESCIMENTO, "id,name,status", _camp_status_filter("all")
        )
        if not filtered:
            return jsonify({"ok": True, "total": 0, "campaigns": [], "action_types_seen": []})

        insights_raw = _fetch_insights_for_tagged_campaigns(
            filtered,
            base_params={
                # 'results' = coluna "Resultados" do Gerenciador. Demais sao pra debug de action_types.
                "fields": "campaign_id,campaign_name,spend,impressions,actions,unique_actions,conversions,conversion_values,cost_per_action_type,cost_per_unique_action_type,cost_per_conversion,results",
                "time_range": json.dumps({"since": dt_from, "until": dt_to}),
                "level": "campaign",
                "action_attribution_windows": json.dumps(["1d_view", "7d_click", "28d_click"]),
                "use_unified_attribution_setting": "true",
                "limit": 500,
            }
        )

        def _aggregate(field_name):
            agg = {}
            for row in insights_raw:
                for act in (row.get(field_name) or []):
                    at = act.get("action_type", "")
                    dest = act.get("action_destination", "")
                    key = at + (" @ " + dest if dest else "")
                    v = 0
                    try: v = float(act.get("value", 0))
                    except Exception: pass
                    if at and v > 0:
                        prev = agg.get(key, {"count_campaigns": 0, "total_value": 0, "action_type": at, "action_destination": dest})
                        prev["count_campaigns"] += 1
                        prev["total_value"] += v
                        agg[key] = prev
            return sorted(agg.values(), key=lambda x: x["total_value"], reverse=True)[:30]

        def _format_cost(field_name):
            """cost_per_action_type vem ja como custo unitario. Pega o menor (mais barato)."""
            agg = {}
            for row in insights_raw:
                for act in (row.get(field_name) or []):
                    at = act.get("action_type", "")
                    v = 0
                    try: v = float(act.get("value", 0))
                    except Exception: pass
                    if at and v > 0:
                        prev = agg.get(at, {"action_type": at, "sum": 0, "n": 0})
                        prev["sum"] += v
                        prev["n"] += 1
                        agg[at] = prev
            result = [{"action_type": k, "avg_cost": round(v["sum"]/v["n"], 2)} for k, v in agg.items()]
            return sorted(result, key=lambda x: x["avg_cost"])[:30]

        total_spend = sum(float(r.get("spend", 0)) for r in insights_raw)

        # Amostra da resposta crua: campanha com maior gasto (pra inspecao rapida)
        sample_row = None
        if insights_raw:
            top = max(insights_raw, key=lambda r: float(r.get("spend", 0)))
            sample_row = {
                "campaign_name": top.get("campaign_name", ""),
                "spend": top.get("spend", ""),
                "raw_keys": list(top.keys()),
                "results_sample": top.get("results"),
                "actions_sample": (top.get("actions") or [])[:50],
                "unique_actions_sample": (top.get("unique_actions") or [])[:50],
                "conversions_sample": (top.get("conversions") or [])[:50],
                "cost_per_action_type_sample": (top.get("cost_per_action_type") or [])[:50],
            }

        # Agregacao do campo 'results' em todas as campanhas — mostra o que Meta devolve
        # nesse campo pra facilitar o mapeamento pra profile_visit.
        results_agg = {}
        for row in insights_raw:
            for r in (row.get("results") or []):
                if not isinstance(r, dict):
                    continue
                ind = r.get("indicator") or r.get("action_type") or "(sem indicator)"
                # Soma valor (tenta todos os formatos)
                val = 0
                v = r.get("value")
                if v is not None and not isinstance(v, (list, dict)):
                    try: val = int(float(v))
                    except Exception: pass
                else:
                    for vv in (r.get("values") or []):
                        if isinstance(vv, dict):
                            try: val += int(float(vv.get("value", 0) or 0))
                            except Exception: pass
                prev = results_agg.get(ind, {"indicator": ind, "total": 0, "n_campaigns": 0})
                prev["total"] += val
                prev["n_campaigns"] += 1
                results_agg[ind] = prev
        results_ranked = sorted(results_agg.values(), key=lambda x: x["total"], reverse=True)[:30]

        return jsonify({
            "ok": True,
            "total": len(filtered),
            "period": f"{dt_from} -> {dt_to}",
            "total_spend": round(total_spend, 2),
            "current_follow_types": FOLLOW_TYPES,
            "current_profile_visit_types": PROFILE_VISIT_TYPES,
            "results_aggregated": results_ranked,
            "actions": _aggregate("actions"),
            "unique_actions": _aggregate("unique_actions"),
            "conversions": _aggregate("conversions"),
            "cost_per_action_type": _format_cost("cost_per_action_type"),
            "cost_per_unique_action_type": _format_cost("cost_per_unique_action_type"),
            "sample_raw_row": sample_row,
        })
    except Exception as e:
        import traceback
        return jsonify({"ok": False, "error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/admin/unidentified-campaigns")
def api_unidentified_campaigns():
    """Lista campanhas que nao foram alocadas a nenhum evento/produto.
    Por padrao, retorna somente campanhas com impressoes/spend no ano corrente
    (param only_with_data=1). Para ver tudo, passar only_with_data=0."""
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    only_with_data = request.args.get("only_with_data", "1") == "1"
    try:
        # Busca campanhas de TODAS as contas configuradas
        all_accounts = set()
        if ACCOUNT_ID:
            all_accounts.add(ACCOUNT_ID)
        for extra in _load_ad_accounts():
            if extra.get("id"):
                all_accounts.add(extra["id"])

        all_camps = []
        for acc in all_accounts:
            try:
                rows = meta_get_all_pages(
                    f"{acc}/campaigns",
                    {"fields": "id,name,status,objective,created_time",
                     "effective_status": _camp_status_filter("all")}
                )
                for c in rows:
                    c["_account_id"] = acc
                all_camps.extend(rows)
            except Exception as e:
                print(f"[UNIDENTIFIED] Falha {acc}: {e}")

        # Se only_with_data, consulta insights agregados do ano corrente para
        # descobrir quais campanhas tiveram impressoes > 0 em 2026 (ou ano atual)
        campaigns_with_data = None
        if only_with_data and all_camps:
            now_br = _now_br()
            year_start = f"{now_br.year}-01-01"
            year_end = (now_br - timedelta(days=1)).strftime("%Y-%m-%d")
            campaigns_with_data = set()
            for acc in all_accounts:
                try:
                    rows = meta_get_all_pages(
                        f"{acc}/insights",
                        {
                            "fields": "campaign_id,impressions",
                            "time_range": json.dumps({"since": year_start, "until": year_end}),
                            "level": "campaign",
                            "filtering": json.dumps([{"field": "impressions", "operator": "GREATER_THAN", "value": 0}]),
                            "limit": 500,
                        }
                    )
                    for r in rows:
                        cid = r.get("campaign_id")
                        if cid:
                            campaigns_with_data.add(cid)
                except Exception as e:
                    print(f"[UNIDENTIFIED] Falha insights {acc}: {e}")

        overrides = _load_overrides()
        unidentified = []
        override_list = []

        for c in all_camps:
            cid = c["id"]
            name = c.get("name", "")

            # Aplica filtro de atividade (se habilitado)
            if campaigns_with_data is not None and cid not in campaigns_with_data:
                continue

            # Campanha com override ativo — separa em outra lista
            if cid in overrides:
                ov = overrides[cid]
                override_list.append({
                    "id": cid,
                    "name": name,
                    "status": c.get("status", ""),
                    "account_id": c.get("_account_id", ""),
                    "created_time": c.get("created_time", ""),
                    "override_type": ov.get("camp_type", ""),
                    "override_event": ov.get("event_name", ""),
                    "override_by": ov.get("created_by", ""),
                    "override_at": ov.get("created_at", ""),
                })
                continue

            # Detecta tipo auto + tenta parsear
            auto_type = None
            if _is_nutricao_campaign(name):
                auto_type = CAMP_TYPE_NUTRICAO
            elif _is_meteoricos_campaign(name):
                auto_type = CAMP_TYPE_METEORICOS
            elif _is_comercial_campaign(name):
                auto_type = CAMP_TYPE_COMERCIAL
            elif _is_crescimento_campaign(name):
                auto_type = CAMP_TYPE_CRESCIMENTO
            elif c.get("objective") == "OUTCOME_SALES":
                auto_type = CAMP_TYPE_VENDAS
            elif _is_vendas_campaign_by_name(name):
                auto_type = CAMP_TYPE_VENDAS

            parsed = _parse_name(name) if auto_type else None

            # Nao identificada: sem auto_type OU sem parse
            if not auto_type or not parsed:
                unidentified.append({
                    "id": cid,
                    "name": name,
                    "status": c.get("status", ""),
                    "objective": c.get("objective", ""),
                    "account_id": c.get("_account_id", ""),
                    "created_time": c.get("created_time", ""),
                    "auto_detected_type": auto_type,
                    "in_dashboard": bool(auto_type),
                })

        unidentified.sort(key=lambda x: x.get("created_time", ""), reverse=True)
        override_list.sort(key=lambda x: x.get("override_at", ""), reverse=True)

        return jsonify({
            "ok": True,
            "total": len(unidentified),
            "total_campaigns_scanned": len(all_camps),
            "only_with_data": only_with_data,
            "campaigns": unidentified,
            "overrides": override_list,
            "valid_camp_types": list(VALID_CAMP_TYPES),
        })
    except Exception as e:
        import traceback
        return jsonify({"ok": False, "error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/admin/campaign-override", methods=["POST"])
def api_campaign_override_set():
    """Aplica ou atualiza override para uma campanha.
    Body: {campaign_id, camp_type, event_name, event_key?}"""
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    data = request.get_json() or {}
    cid = (data.get("campaign_id") or "").strip()
    ct = _normalize_camp_type(data.get("camp_type", ""))
    event_name = (data.get("event_name") or "").strip()
    event_key = (data.get("event_key") or "").strip()

    if not cid:
        return jsonify({"ok": False, "error": "campaign_id obrigatorio"}), 400
    if not event_name:
        return jsonify({"ok": False, "error": "event_name obrigatorio"}), 400
    if data.get("camp_type") and data.get("camp_type") not in VALID_CAMP_TYPES:
        return jsonify({"ok": False, "error": "camp_type invalido"}), 400

    overrides = _load_overrides()
    overrides[cid] = {
        "camp_type": ct,
        "event_name": event_name,
        "event_key": event_key or event_name.upper().replace(" ", "_"),
        "created_by": session.get("username", ""),
        "created_at": _now_br().strftime("%Y-%m-%d"),
    }
    _save_overrides(overrides)
    return jsonify({"ok": True, "override": overrides[cid]})


@app.route("/api/admin/campaign-override/<path:cid>", methods=["DELETE"])
def api_campaign_override_delete(cid):
    """Remove override manual."""
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    overrides = _load_overrides()
    if cid not in overrides:
        return jsonify({"ok": False, "error": "Override nao encontrado"}), 404
    del overrides[cid]
    _save_overrides(overrides)
    return jsonify({"ok": True})


@app.route("/api/admin/meteoricos-preview")
def api_meteoricos_preview():
    """Endpoint de diagnostico: lista todas as campanhas que comecam com METEORICO_
    e mostra objective + metricas basicas dos ultimos 30 dias. Usado para entender
    a estrutura dos dados antes de refatorar o dashboard para suportar esse tipo."""
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    try:
        now_br = _now_br()
        dt_to = (now_br - timedelta(days=1)).strftime("%Y-%m-%d")
        dt_from = (now_br - timedelta(days=30)).strftime("%Y-%m-%d")

        # 1. Puxar TODAS as campanhas (ativas + pausadas) e filtrar pelo prefixo no nome
        all_campaigns = meta_get_all_pages(
            f"{ACCOUNT_ID}/campaigns",
            {
                "fields": "id,name,status,objective,daily_budget,lifetime_budget,start_time,created_time",
                "effective_status": '["ACTIVE","PAUSED","ARCHIVED"]',
            }
        )
        meteoricos = [c for c in all_campaigns if c.get("name", "").upper().startswith("METEORICO_")]

        if not meteoricos:
            return jsonify({
                "ok": True,
                "total": 0,
                "message": "Nenhuma campanha encontrada com prefixo METEORICO_",
                "sample_other_campaigns": [c.get("name", "") for c in all_campaigns[:10]],
            })

        # 2. Contar por objective (pra descobrir qual usam)
        objectives = {}
        for c in meteoricos:
            obj = c.get("objective", "UNKNOWN")
            objectives[obj] = objectives.get(obj, 0) + 1

        # 3. Pegar insights dos ultimos 30d e inspecionar os action_types disponiveis
        camp_ids = [c["id"] for c in meteoricos]
        insights_raw = []
        action_types_seen = {}
        if camp_ids:
            try:
                insights_raw = meta_get_all_pages(
                    f"{ACCOUNT_ID}/insights",
                    {
                        "level": "campaign",
                        "fields": "campaign_id,campaign_name,spend,impressions,clicks,actions,cost_per_action_type",
                        "time_range": json.dumps({"since": dt_from, "until": dt_to}),
                        "filtering": json.dumps([{"field": "campaign.id", "operator": "IN", "value": camp_ids}]),
                    }
                )
                # Indexar por campaign_id e coletar todos os action_types vistos
                for row in insights_raw:
                    for act in (row.get("actions") or []):
                        at = act.get("action_type", "")
                        v = 0
                        try: v = int(float(act.get("value", 0)))
                        except Exception: pass
                        if at:
                            prev = action_types_seen.get(at, {"count_campaigns": 0, "total_value": 0})
                            prev["count_campaigns"] += 1 if v > 0 else 0
                            prev["total_value"] += v
                            action_types_seen[at] = prev
            except Exception as e:
                print(f"[METEORICOS-PREVIEW] Erro insights: {e}")

        insights_by_camp = {r.get("campaign_id"): r for r in insights_raw}

        # 4. Montar lista de campanhas com dados
        result = []
        for c in meteoricos:
            ins = insights_by_camp.get(c["id"], {})
            # Tentar extrair leads de varios action_types possiveis
            lead_candidates = {}
            for act in (ins.get("actions") or []):
                at = act.get("action_type", "")
                if "lead" in at.lower() or "subscribe" in at.lower():
                    try: lead_candidates[at] = int(float(act.get("value", 0)))
                    except Exception: pass
            result.append({
                "id": c["id"],
                "name": c.get("name", ""),
                "status": c.get("status", ""),
                "objective": c.get("objective", ""),
                "daily_budget": (float(c.get("daily_budget", 0)) / 100) if c.get("daily_budget") else None,
                "lifetime_budget": (float(c.get("lifetime_budget", 0)) / 100) if c.get("lifetime_budget") else None,
                "start_time": c.get("start_time", ""),
                "spend_30d": float(ins.get("spend", 0)) if ins else 0,
                "impressions_30d": int(ins.get("impressions", 0)) if ins else 0,
                "clicks_30d": int(ins.get("clicks", 0)) if ins else 0,
                "lead_candidates": lead_candidates,
            })

        # Ordenar por gasto desc
        result.sort(key=lambda x: x.get("spend_30d", 0), reverse=True)

        # Ranking de action_types mais comuns (top 20)
        action_types_ranked = sorted(
            [{"action_type": k, **v} for k, v in action_types_seen.items()],
            key=lambda x: x["total_value"],
            reverse=True
        )[:20]

        return jsonify({
            "ok": True,
            "total": len(meteoricos),
            "period": f"{dt_from} -> {dt_to}",
            "objectives_count": objectives,
            "action_types_seen": action_types_ranked,
            "campaigns": result,
        })
    except Exception as e:
        import traceback
        return jsonify({"ok": False, "error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/admin/config", methods=["GET"])
def admin_get_config():
    if not session.get("logged_in"):
        return jsonify({"ok": False, "error": "Nao autorizado"}), 401
    # Mostrar token parcial por segurança
    token = TOKEN
    masked = token[:15] + "..." + token[-10:] if len(token) > 30 else "NAO CONFIGURADO"
    return jsonify({
        "ok": True,
        "token_masked": masked,
        "token_length": len(token),
        "account_id": ACCOUNT_ID,
        "app_id": os.getenv("META_APP_ID", ""),
    })

@app.route("/api/admin/update-app-config", methods=["POST"])
def admin_update_app_config():
    """Atualiza META_APP_ID e META_APP_SECRET no .env. Super admin apenas."""
    if not session.get("logged_in"):
        return jsonify({"ok": False, "error": "Nao autorizado"}), 401
    if not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Apenas o administrador principal"}), 403

    data = request.get_json() or {}
    admin_pass = data.get("admin_password", "")
    new_app_id = (data.get("app_id") or "").strip()
    new_app_secret = (data.get("app_secret") or "").strip()

    if admin_pass != ADMIN_PASSWORD:
        return jsonify({"ok": False, "error": "Senha admin incorreta"}), 403
    if not new_app_id or not new_app_secret:
        return jsonify({"ok": False, "error": "Informe App ID e App Secret"}), 400
    if not new_app_id.isdigit():
        return jsonify({"ok": False, "error": "App ID deve ser numerico"}), 400
    if len(new_app_secret) < 20:
        return jsonify({"ok": False, "error": "App Secret parece invalido (muito curto)"}), 400

    env_path = os.path.join(os.path.dirname(__file__), ".env")
    try:
        lines = []
        if os.path.exists(env_path):
            with open(env_path, "r") as f:
                lines = f.readlines()

        new_lines = []
        found_id = found_secret = False
        for line in lines:
            if line.startswith("META_APP_ID="):
                new_lines.append(f"META_APP_ID={new_app_id}\n")
                found_id = True
            elif line.startswith("META_APP_SECRET="):
                new_lines.append(f"META_APP_SECRET={new_app_secret}\n")
                found_secret = True
            else:
                new_lines.append(line)
        if not found_id:
            new_lines.append(f"META_APP_ID={new_app_id}\n")
        if not found_secret:
            new_lines.append(f"META_APP_SECRET={new_app_secret}\n")

        with open(env_path, "w") as f:
            f.writelines(new_lines)

        # Atualizar variaveis de ambiente (codigo le via os.getenv)
        os.environ["META_APP_ID"] = new_app_id
        os.environ["META_APP_SECRET"] = new_app_secret

        return jsonify({"ok": True, "message": "App ID e Secret atualizados. Agora atualize o token."})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/admin/update-token", methods=["POST"])
def admin_update_token():
    if not session.get("logged_in"):
        return jsonify({"ok": False, "error": "Nao autorizado"}), 401
    if not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Apenas o administrador principal pode alterar o token"}), 403

    data = request.get_json()
    admin_pass = data.get("admin_password", "")
    new_token = data.get("new_token", "").strip()

    if admin_pass != ADMIN_PASSWORD:
        return jsonify({"ok": False, "error": "Senha admin incorreta"}), 403

    if not new_token or len(new_token) < 50:
        return jsonify({"ok": False, "error": "Token invalido"}), 400

    # Atualizar .env
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    try:
        lines = []
        if os.path.exists(env_path):
            with open(env_path, "r") as f:
                lines = f.readlines()

        new_lines = []
        found = False
        for line in lines:
            if line.startswith("META_ACCESS_TOKEN="):
                new_lines.append("META_ACCESS_TOKEN=" + new_token + "\n")
                found = True
            else:
                new_lines.append(line)
        if not found:
            new_lines.append("META_ACCESS_TOKEN=" + new_token + "\n")

        with open(env_path, "w") as f:
            f.writelines(new_lines)

        # Atualizar variável em memória
        global TOKEN
        TOKEN = new_token
        os.environ["META_ACCESS_TOKEN"] = new_token

        return jsonify({"ok": True, "message": "Token atualizado! Reinicie o servidor para garantir."})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ── Scheduled refresh ──────────────────────────────────────────────────

def _sleep_with_heartbeat(seconds, lock_name):
    """Dorme em blocos de 5min renovando o lock a cada bloco.

    Impede que outro worker roube o lock durante uma pausa longa entre
    etapas do scheduler. Antes, o lock ficava com idade > 15min durante
    os sleep(1800), e outro worker podia assumir e rodar o scheduler em
    paralelo, duplicando chamadas Meta e poluindo logs."""
    remaining = seconds
    step = 300
    while remaining > 0:
        chunk = min(step, remaining)
        time.sleep(chunk)
        remaining -= chunk
        try:
            refresh_scheduler_lock(lock_name)
        except Exception:
            pass


# ── Monthly warmup: meses COMPLETOS ficam cacheados eternamente ─────────
# Fluxo por mes:
#   D+1 apos o fim do mes  -> primeira warming (fresh Meta) + pin 180d
#   D+8 (~1 semana depois) -> revalidacao UNICA (atribuicoes atrasadas da Meta)
#   D+9 em diante          -> so re-pin (estende TTL, nao toca Meta)
# Estado por mes guardado em 'monthly_state_YYYY_MM' com TTL longo.

_MONTHLY_START_YEAR = 2026
_MONTHLY_START_MONTH = 1
_MONTHLY_TTL_HOURS = 4320        # 180 dias - cache dos meses em si
_MONTHLY_STATE_TTL_HOURS = 8760  # 365 dias - estado (warmed/revalidated)
_MONTHLY_REVALIDATE_AFTER_DAYS = 7


# Campos numericos das campaigns_v7 que sao aditivos quando combinamos
# segmentos de tempo (spend de Jan + spend de Fev = spend de Jan+Fev).
_CAMP_NUMERIC_FIELDS = [
    "spend", "revenue", "purchases", "impressions", "clicks",
    "profile_visits", "video_thruplay", "video_plays",
    "video_p25", "video_p50", "video_p75", "video_p95", "video_p100",
]
_SUMMARY_ADDITIVE_FIELDS = [
    "total_spend", "total_revenue", "total_purchases", "total_impressions",
    "total_clicks", "total_profile_visits", "total_thruplay",
    "total_video_plays", "total_video_p25", "total_video_p50",
    "total_video_p75", "total_video_p95", "total_video_p100",
]


def _is_completed_month(seg_from, seg_to):
    """True se [seg_from, seg_to] corresponde a um mes completo JA FECHADO
    (primeiro ao ultimo dia, e mes/ano < mes/ano atual BRT). Caches destes
    segmentos sao pinados com TTL longo pois os dados sao imutaveis."""
    import calendar
    from datetime import datetime as _dt
    try:
        df = _dt.strptime(seg_from, "%Y-%m-%d")
        dt_obj = _dt.strptime(seg_to, "%Y-%m-%d")
        if df.day != 1 or df.month != dt_obj.month or df.year != dt_obj.year:
            return False
        last_day = calendar.monthrange(df.year, df.month)[1]
        if dt_obj.day != last_day:
            return False
        now_br = _now_br()
        return (df.year, df.month) < (now_br.year, now_br.month)
    except Exception:
        return False


def _fetch_acc_insights_chunked(acc_id, d_from, d_to):
    """/insights level=campaign, chunked por mes + cache por segmento.
    Segmentos de mes COMPLETO (fechado) sao pinados 180d. Parciais/mes
    corrente tem TTL curto (6h). Retorna lista agregada [{campaign_id,
    campaign_name, spend}] somada por campanha across segmentos.

    DUAL-READ: se atoms cobrem o range, soma de atoms (zero call Meta)."""
    _sync_use_atoms_from_file()
    if USE_ATOMS:
        atoms_list, missing = get_atoms_for_range('acc', acc_id, d_from, d_to)
        if not missing and atoms_list:
            rows_by_cid = {}
            for atom in atoms_list:
                payload = atom.get('payload') or {}
                # Mapa de id->name das campanhas no payload (atom mais
                # recente vence se houver renomeacao)
                names = {}
                for c in (payload.get('campaigns') or []):
                    cid_c = c.get('id')
                    if cid_c:
                        names[cid_c] = c.get('name', '')
                for cid, ins in (payload.get('insights_by_id') or {}).items():
                    if cid not in rows_by_cid:
                        rows_by_cid[cid] = {
                            "campaign_id": cid,
                            "campaign_name": names.get(cid, '') or ins.get('campaign_name', ''),
                            "spend": 0.0,
                        }
                    rows_by_cid[cid]["spend"] += float(ins.get('spend', 0) or 0)
                    # Atualiza nome se atom mais novo tem nome melhor
                    if names.get(cid):
                        rows_by_cid[cid]["campaign_name"] = names[cid]
            return list(rows_by_cid.values())

    segments = _split_range_by_month_segments(d_from, d_to)
    rows_by_cid = {}
    for seg_from, seg_to in segments:
        ck = f"acc_insights_v1_{acc_id}_{seg_from}_{seg_to}"
        cached = get_cached(ck)
        if cached is None:
            try:
                raw = meta_get_all_pages(f"{acc_id}/insights", {
                    "fields": "campaign_id,campaign_name,spend",
                    "time_range": json.dumps({"since": seg_from, "until": seg_to}),
                    "level": "campaign",
                    "filtering": json.dumps([{"field": "impressions", "operator": "GREATER_THAN", "value": 0}]),
                    "limit": 500,
                })
                simplified = [{
                    "campaign_id": r.get("campaign_id", ""),
                    "campaign_name": r.get("campaign_name", ""),
                    "spend": float(r.get("spend", 0) or 0),
                } for r in raw]
                # TTL longo pra mes completo; curto pra partial/mes corrente.
                ttl = _MONTHLY_TTL_HOURS if _is_completed_month(seg_from, seg_to) else 6
                set_cached(ck, simplified, ttl_hours=ttl)
                cached = simplified
            except Exception as e:
                print(f"[RESUMO] Falha acc_insights {acc_id} {seg_from}-{seg_to}: {e}")
                cached = []
        for r in cached:
            cid = r.get("campaign_id", "")
            if not cid:
                continue
            if cid not in rows_by_cid:
                rows_by_cid[cid] = {
                    "campaign_id": cid,
                    "campaign_name": r.get("campaign_name", ""),
                    "spend": 0.0,
                }
            rows_by_cid[cid]["spend"] += float(r.get("spend", 0) or 0)
    return list(rows_by_cid.values())


def _fetch_acc_total_spend_chunked(acc_id, d_from, d_to):
    """Total spend de UMA conta em [d_from, d_to], chunked por mes.
    Reusa as mesmas caches por segmento do _fetch_acc_insights_chunked
    (soma spend de todas as campanhas do segmento). Segmentos ja cacheados
    = instantaneo.

    DUAL-READ: se atoms cobrem o range, soma de atoms (zero call Meta)."""
    _sync_use_atoms_from_file()
    if USE_ATOMS:
        atoms_list, missing = get_atoms_for_range('acc', acc_id, d_from, d_to)
        if not missing and atoms_list:
            total = 0.0
            for atom in atoms_list:
                payload = atom.get('payload') or {}
                for cid, ins in (payload.get('insights_by_id') or {}).items():
                    total += float(ins.get('spend', 0) or 0)
            return total

    total = 0.0
    for seg_from, seg_to in _split_range_by_month_segments(d_from, d_to):
        ck = f"acc_insights_v1_{acc_id}_{seg_from}_{seg_to}"
        cached = get_cached(ck)
        if cached is None:
            # Popula o cache via fetch completo (mais util que so o total)
            cached = _fetch_acc_insights_chunked_single(acc_id, seg_from, seg_to)
        for r in cached or []:
            total += float(r.get("spend", 0) or 0)
    return total


def _fetch_acc_insights_chunked_single(acc_id, seg_from, seg_to):
    """Helper interno: fetcha 1 segmento e cacheia. Extraido pro
    _fetch_acc_total_spend_chunked reutilizar sem duplicar logica."""
    ck = f"acc_insights_v1_{acc_id}_{seg_from}_{seg_to}"
    try:
        raw = meta_get_all_pages(f"{acc_id}/insights", {
            "fields": "campaign_id,campaign_name,spend",
            "time_range": json.dumps({"since": seg_from, "until": seg_to}),
            "level": "campaign",
            "filtering": json.dumps([{"field": "impressions", "operator": "GREATER_THAN", "value": 0}]),
            "limit": 500,
        })
        simplified = [{
            "campaign_id": r.get("campaign_id", ""),
            "campaign_name": r.get("campaign_name", ""),
            "spend": float(r.get("spend", 0) or 0),
        } for r in raw]
        ttl = _MONTHLY_TTL_HOURS if _is_completed_month(seg_from, seg_to) else 6
        set_cached(ck, simplified, ttl_hours=ttl)
        return simplified
    except Exception as e:
        print(f"[RESUMO] Falha single {acc_id} {seg_from}: {e}")
        return []


def _split_range_by_month_segments(d_from, d_to):
    """Quebra [d_from, d_to] em segmentos contiguos limitados por fronteiras
    de mes. Retorna [(seg_from, seg_to), ...] em strings YYYY-MM-DD.

    Exemplos:
      2026-03-01 a 2026-03-31 -> [(2026-03-01, 2026-03-31)]  (1 mes cheio)
      2026-03-15 a 2026-04-10 -> [(2026-03-15, 2026-03-31), (2026-04-01, 2026-04-10)]
      2026-01-01 a 2026-03-31 -> [(2026-01-01, 2026-01-31), (2026-02-01, 2026-02-28), (2026-03-01, 2026-03-31)]
    """
    import calendar
    from datetime import datetime as _dt, timedelta as _td
    try:
        cur = _dt.strptime(d_from, "%Y-%m-%d")
        end = _dt.strptime(d_to, "%Y-%m-%d")
    except Exception:
        return [(d_from, d_to)]
    if cur > end:
        return [(d_from, d_to)]
    segments = []
    while cur <= end:
        last_day = calendar.monthrange(cur.year, cur.month)[1]
        month_end = _dt(cur.year, cur.month, last_day)
        seg_end = min(month_end, end)
        segments.append((cur.strftime("%Y-%m-%d"), seg_end.strftime("%Y-%m-%d")))
        cur = seg_end + _td(days=1)
    return segments


def _merge_campaigns_data(segments):
    """Combina N dicts {data, summary} do endpoint /campaigns em um equivalente
    ao do range completo. Soma campos aditivos por campanha (merge by id) e
    recalcula derivadas (cpa/cpm/ctr/roas) em cima dos somatorios."""
    if not segments:
        return {"data": [], "summary": {}}
    if len(segments) == 1:
        return segments[0]

    merged_by_id = {}
    for seg in segments:
        for c in seg.get("data", []) or []:
            cid = c.get("id") or ""
            if not cid:
                continue
            if cid not in merged_by_id:
                # copia metadados (name, status, objective, event_name, etc)
                base = dict(c)
                for f in _CAMP_NUMERIC_FIELDS:
                    base[f] = float(c.get(f, 0) or 0)
                merged_by_id[cid] = base
            else:
                target = merged_by_id[cid]
                for f in _CAMP_NUMERIC_FIELDS:
                    target[f] = float(target.get(f, 0) or 0) + float(c.get(f, 0) or 0)
                # active_days: pega o maior (campanha ativa em qualquer segmento)
                ad_new = c.get("active_days", 0) or 0
                if ad_new > (target.get("active_days", 0) or 0):
                    target["active_days"] = ad_new

    # Recalcula derivadas por campanha
    for c in merged_by_id.values():
        sp = float(c.get("spend", 0) or 0)
        pr = float(c.get("purchases", 0) or 0)
        imp = float(c.get("impressions", 0) or 0)
        clk = float(c.get("clicks", 0) or 0)
        rev = float(c.get("revenue", 0) or 0)
        c["cpa"] = round(sp / pr, 2) if pr > 0 else 0
        c["cpm"] = round(sp / imp * 1000, 2) if imp > 0 else 0
        c["ctr"] = round(clk / imp * 100, 2) if imp > 0 else 0
        c["roas"] = round(rev / sp, 2) if sp > 0 else 0

    # Recalcula summary
    merged_summary = {}
    for f in _SUMMARY_ADDITIVE_FIELDS:
        merged_summary[f] = 0
    for seg in segments:
        s = seg.get("summary", {}) or {}
        for f in _SUMMARY_ADDITIVE_FIELDS:
            merged_summary[f] += float(s.get(f, 0) or 0)
    ts = merged_summary["total_spend"]
    tr = merged_summary["total_revenue"]
    tp = merged_summary["total_purchases"]
    ti = merged_summary["total_impressions"]
    tc = merged_summary["total_clicks"]
    merged_summary["total_campaigns"] = len(merged_by_id)
    merged_summary["avg_roas"] = round(tr / ts, 2) if ts > 0 else 0
    merged_summary["avg_cpa"] = round(ts / tp, 2) if tp > 0 else 0
    merged_summary["avg_cpm"] = round((ts / ti) * 1000, 2) if ti > 0 else 0
    merged_summary["avg_ctr"] = round((tc / ti) * 100, 2) if ti > 0 else 0

    data_list = list(merged_by_id.values())
    data_list.sort(key=lambda x: x.get("spend", 0), reverse=True)
    return {"data": data_list, "summary": merged_summary}


def _merge_daily_data(segments):
    """Concat series diarias de N segmentos (sem duplicar datas), ordenado."""
    if not segments:
        return {"data": []}
    if len(segments) == 1:
        return segments[0]
    seen = {}
    for seg in segments:
        for r in seg.get("data", []) or []:
            d = r.get("date", "")
            if d and d not in seen:
                seen[d] = r
    rows = [seen[k] for k in sorted(seen.keys())]
    return {"data": rows}


def _completed_months_since(year, month):
    """Retorna [(dt_from, dt_to), ...] de cada mes COMPLETO desde (year, month)
    ate o mes anterior ao atual (BRT). O mes corrente nao entra — so fecha
    apos o ultimo dia."""
    import calendar
    now_br = _now_br()
    cur_y, cur_m = now_br.year, now_br.month
    ranges = []
    y, m = year, month
    while (y, m) < (cur_y, cur_m):
        last_day = calendar.monthrange(y, m)[1]
        ranges.append((f"{y:04d}-{m:02d}-01", f"{y:04d}-{m:02d}-{last_day:02d}"))
        m += 1
        if m == 13:
            m, y = 1, y + 1
    return ranges


def _monthly_cache_keys(dt_from, dt_to):
    """Todas as chaves de cache relacionadas a um range mensal. Usado pra pinar."""
    keys = [f"resumo_v16_{dt_from}_{dt_to}"]
    for ct in VALID_CAMP_TYPES:
        keys.append(f"campaigns_v8_{ct}_all_{dt_from}_{dt_to}")
        keys.append(f"daily_summary_v9_{ct}_all_{dt_from}_{dt_to}")
    return keys


def _pin_monthly(dt_from, dt_to):
    """Pina (estende TTL) todas as chaves cacheadas desse mes."""
    pinned = 0
    for k in _monthly_cache_keys(dt_from, dt_to):
        if pin_cache_key(k, ttl_hours=_MONTHLY_TTL_HOURS):
            pinned += 1
    return pinned


def _warm_month_once(client, dt_from, dt_to, label="first"):
    """Faz uma passada completa de warming em UM mes: campaigns + daily_summary
    dos 5 tipos, acc_insights por conta, depois o endpoint resumo. Todas com
    force=true pra pegar dados frescos da Meta (usado na 1a carga e na
    revalidacao de D+7). Aborta se BUC critico — retoma em outra rodada."""
    if _buc_is_critical():
        print(f"[MONTHLY] BUC critico — abortando warmup de {dt_from}-{dt_to}, retoma depois")
        return
    hdr = {"X-Internal-Scheduler": f"monthly_{label}"}
    for ct in VALID_CAMP_TYPES:
        try:
            base = f"camp_type={ct}&date_from={dt_from}&date_to={dt_to}&camp_status=all&force=true"
            client.get(f"/api/dashboard/campaigns?{base}", headers=hdr)
            time.sleep(4)
            client.get(f"/api/dashboard/daily-summary?{base}", headers=hdr)
            time.sleep(4)
        except Exception as e:
            print(f"[MONTHLY] Erro {ct} {dt_from}: {e}")
    # Pre-cache do /insights nivel conta pra essa mesma janela (1 por conta)
    # — usado pelo resumo pra calcular total_acc_spend e top_unmapped.
    try:
        all_accs = set()
        for ct in VALID_CAMP_TYPES:
            for acc in _get_accounts_for_type(ct):
                if acc:
                    all_accs.add(acc)
        for acc in all_accs:
            try:
                _fetch_acc_insights_chunked_single(acc, dt_from, dt_to)
                time.sleep(3)
            except Exception as e:
                print(f"[MONTHLY] Erro acc_insights {acc} {dt_from}: {e}")
    except Exception as e:
        print(f"[MONTHLY] Erro listando contas {dt_from}: {e}")
    try:
        client.get(f"/api/dashboard/resumo?date_from={dt_from}&date_to={dt_to}&force=true", headers=hdr)
        time.sleep(2)
    except Exception as e:
        print(f"[MONTHLY] Erro resumo {dt_from}: {e}")
    # Drill-down Outros pre-warm (pinado 180d pra mes fechado)
    try:
        client.get(f"/api/dashboard/resumo/unmapped?date_from={dt_from}&date_to={dt_to}", headers=hdr)
        time.sleep(2)
    except Exception as e:
        print(f"[MONTHLY] Erro resumo/unmapped {dt_from}: {e}")


def _warmup_monthly_historical():
    """Warma e pina caches de meses completos. Idempotente — seguro chamar
    varias vezes por dia. Revalida cada mes UMA vez, ~1 semana apos a
    primeira carga (pra pegar atribuicoes atrasadas da Meta)."""
    from datetime import datetime as _dt
    months = _completed_months_since(_MONTHLY_START_YEAR, _MONTHLY_START_MONTH)
    if not months:
        print("[MONTHLY] Sem meses completos pra pre-carregar ainda")
        return
    print(f"[MONTHLY] {len(months)} mes(es) completo(s) — verificando estado de cada um")

    now_iso = _dt.now().isoformat()
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess["logged_in"] = True
            sess["username"] = SUPER_ADMIN_EMAIL
            sess["role"] = "super_admin"

        for dt_from, dt_to in months:
            ym = dt_from[:7].replace("-", "_")  # "2026_01"
            state_key = f"monthly_state_{ym}"
            state = get_cached(state_key) or {}

            # Se o state diz 'warmed' mas o cache real nao existe (bump de versao,
            # cache purgado, etc), invalida o state pra re-aquecer de fato.
            if state.get("warmed_at"):
                r_key = f"resumo_v16_{dt_from}_{dt_to}"
                if get_cached(r_key) is None:
                    print(f"[MONTHLY] {dt_from} a {dt_to}: state orfao (cache sumiu), re-aquecendo")
                    state = {}

            if not state.get("warmed_at"):
                # PRIMEIRA carga — mes acabou de fechar
                print(f"[MONTHLY] {dt_from} a {dt_to}: primeira carga")
                _warm_month_once(client, dt_from, dt_to, label="first")
                pinned = _pin_monthly(dt_from, dt_to)
                state = {"warmed_at": now_iso, "revalidated_at": None}
                set_cached(state_key, state, ttl_hours=_MONTHLY_STATE_TTL_HOURS)
                print(f"[MONTHLY] {dt_from} a {dt_to}: {pinned} chaves pinadas (180d)")
                continue

            if not state.get("revalidated_at"):
                # Verifica se ja passou 1 semana desde a primeira carga
                try:
                    warmed_at = _dt.fromisoformat(state["warmed_at"])
                    days = (_dt.now() - warmed_at).days
                except Exception:
                    days = 999
                if days >= _MONTHLY_REVALIDATE_AFTER_DAYS:
                    print(f"[MONTHLY] {dt_from} a {dt_to}: revalidando (D+{days})")
                    _warm_month_once(client, dt_from, dt_to, label="revalidate")
                    pinned = _pin_monthly(dt_from, dt_to)
                    state["revalidated_at"] = now_iso
                    set_cached(state_key, state, ttl_hours=_MONTHLY_STATE_TTL_HOURS)
                    print(f"[MONTHLY] {dt_from} a {dt_to}: revalidado, {pinned} pinadas")
                else:
                    # Ainda nao chegou o D+7 — so re-pin pra manter TTL
                    _pin_monthly(dt_from, dt_to)
                    print(f"[MONTHLY] {dt_from} a {dt_to}: aguardando D+7 (dia {days}/7), re-pinado")
                continue

            # Ja passou pela 1a carga E pela revalidacao — so manter TTL
            pinned = _pin_monthly(dt_from, dt_to)
            # Renova o state tambem pra nao expirar
            set_cached(state_key, state, ttl_hours=_MONTHLY_STATE_TTL_HOURS)
            print(f"[MONTHLY] {dt_from} a {dt_to}: settled, {pinned} re-pinadas")

    print("[MONTHLY] Warmup concluido")


def _scheduled_refresh():
    """Pre-carrega campanhas e criativos em etapas uma vez por dia (2am BRT).

    Escopo enxuto pra nao sobrecarregar BUC da Meta:
      - Apenas 30d e 7d (ranges mais usados no dashboard)
      - Ranges extras (1d/14d/60d/90d) vem sob demanda ou pelo refresh loop
    Antes rodava 5 ranges × 5 tipos × 5 endpoints (>125 dashboard-calls ≈
    800+ Meta calls) o que era inaceitavel pra conta ja carregada.
    """
    from datetime import datetime, timedelta

    refresh_scheduler_lock("daily_scheduler")
    now_br = _now_br()
    print(f"[SCHEDULER] DISPAROU as {now_br.strftime('%Y-%m-%d %H:%M:%S')} BRT (PID {os.getpid()})")
    dt_to = (now_br - timedelta(days=1)).strftime("%Y-%m-%d")
    # Ranges prioritarios: 30d (default do dashboard) + 7d (secundario mais comum)
    preload_ranges = [30, 7]
    ranges = [(d, (now_br - timedelta(days=d)).strftime("%Y-%m-%d")) for d in preload_ranges]

    print(f"[SCHEDULER] Iniciando atualizacao automatica — ranges: {preload_ranges} dias, ate {dt_to}")
    clear_expired()
    # Limpa logs de uso da API > 7 dias (diagnostico)
    clear_old_usage_logs(days=7)

    with app.test_client() as client:
        # Simular login para acessar endpoints protegidos
        with client.session_transaction() as sess:
            sess["logged_in"] = True
            sess["username"] = SUPER_ADMIN_EMAIL
            sess["role"] = "super_admin"

        # ── ETAPA 1 (2:00): Campanhas (todos os ranges, ambos tipos) ──
        try:
            print("[SCHEDULER] Etapa 1/5: Carregando campanhas (4 ranges x 2 tipos)...")
            for days, dt_from in ranges:
                for ct in VALID_CAMP_TYPES:
                    print(f"[SCHEDULER]   Campanhas {ct} {days}d ({dt_from} a {dt_to})")
                    # status=all para cobrir campanhas que tiveram veiculacao mesmo pausadas
                    client.get(f"/api/dashboard/campaigns?camp_type={ct}&date_from={dt_from}&date_to={dt_to}&camp_status=all&force=true", headers={"X-Internal-Scheduler":"daily"})
                    time.sleep(5)
            print("[SCHEDULER] Campanhas OK")
        except Exception as e:
            print(f"[SCHEDULER] Erro campanhas: {e}")

        # Pausa de 30 min entre etapas
        print("[SCHEDULER] Aguardando 30 min antes da proxima etapa...")
        _sleep_with_heartbeat(1800, "daily_scheduler")

        # ── ETAPA 2 (2:30): Resumo diario (todos os ranges, ambos tipos) ──
        try:
            print("[SCHEDULER] Etapa 2/5: Carregando resumo diario...")
            for days, dt_from in ranges:
                for ct in VALID_CAMP_TYPES:
                    print(f"[SCHEDULER]   Resumo diario {ct} {days}d")
                    client.get(f"/api/dashboard/daily-summary?camp_type={ct}&date_from={dt_from}&date_to={dt_to}&camp_status=all", headers={"X-Internal-Scheduler":"daily"})
                    time.sleep(5)
            print("[SCHEDULER] Resumo diario OK")
        except Exception as e:
            print(f"[SCHEDULER] Erro resumo diario: {e}")

        # ── ETAPA 2b: Pre-warm account_total_spend + endpoint Resumo ──
        # Resumo precisa de account_total_spend (level=account, 1 row rapido)
        # pra calcular Outros. Pre-warm antes do endpoint /resumo assim:
        #   1) account_total_spend_{acc}_{d_from}_{d_to} pronto
        #   2) /resumo so faz soma de caches (tabs warm + este)
        try:
            print("[SCHEDULER] Etapa 2b/5: Warm account_total_spend + /resumo...")
            all_accs = set()
            for ct in VALID_CAMP_TYPES:
                for acc in _get_accounts_for_type(ct):
                    if acc:
                        all_accs.add(acc)
            # Pre-warm account_total_spend pras janelas e pros periodos anteriores
            from datetime import timedelta as _td
            for days, dt_from_r in ranges:
                _dt_from_r = datetime.strptime(dt_from_r, "%Y-%m-%d")
                _dt_to_r = datetime.strptime(dt_to, "%Y-%m-%d")
                _period_days = (_dt_to_r - _dt_from_r).days + 1
                _prev_to = (_dt_from_r - _td(days=1)).strftime("%Y-%m-%d")
                _prev_from = (_dt_from_r - _td(days=_period_days)).strftime("%Y-%m-%d")
                for acc in all_accs:
                    try:
                        _fetch_account_total_spend(acc, dt_from_r, dt_to)
                        _fetch_account_total_spend(acc, _prev_from, _prev_to)
                        time.sleep(2)
                    except Exception as e:
                        print(f"[SCHEDULER] Erro account_total {acc} {days}d: {e}")
            # Agora o endpoint resumo em si — popula resumo_v16 final
            for days, dt_from_r in ranges:
                print(f"[SCHEDULER]   /resumo {days}d ({dt_from_r} a {dt_to})")
                client.get(f"/api/dashboard/resumo?date_from={dt_from_r}&date_to={dt_to}&force=true", headers={"X-Internal-Scheduler":"daily"})
                time.sleep(10)
            # Drill-down Outros (lista de campanhas nao mapeadas) — pre-warm
            # pra ficar instantaneo quando usuario clicar na seta.
            for days, dt_from_r in ranges:
                print(f"[SCHEDULER]   /resumo/unmapped {days}d")
                client.get(f"/api/dashboard/resumo/unmapped?date_from={dt_from_r}&date_to={dt_to}", headers={"X-Internal-Scheduler":"daily"})
                time.sleep(5)
            print("[SCHEDULER] Resumo endpoint OK")
        except Exception as e:
            print(f"[SCHEDULER] Erro resumo endpoint: {e}")

        # Pausa
        print("[SCHEDULER] Aguardando 30 min...")
        _sleep_with_heartbeat(1800, "daily_scheduler")

        # ── ETAPA 3 (3:00): Criativos de cada campanha (apenas 30d — aba campanha individual) ──
        try:
            print("[SCHEDULER] Etapa 3/5: Carregando criativos por campanha (range 30d)...")
            dt_from_30 = (now_br - timedelta(days=30)).strftime("%Y-%m-%d")
            all_camps_list = []
            try:
                data = meta_get(f"{ACCOUNT_ID}/campaigns", {
                    "fields": "id,name,objective",
                    "effective_status": '["ACTIVE"]',
                    "limit": "200"
                })
                # Pega campanhas de ambos os tipos (vendas + meteoricos) com o tipo marcado
                raw_camps = data.get("data", [])
                for ct in VALID_CAMP_TYPES:
                    for c in _filter_campaigns_by_type(raw_camps, ct):
                        all_camps_list.append((ct, c))
            except Exception as e:
                print(f"[SCHEDULER] Erro ao buscar campanhas: {e}")

            for i, (ct, camp) in enumerate(all_camps_list):
                try:
                    print(f"[SCHEDULER]   Criativos {ct} {i+1}/{len(all_camps_list)}: {camp.get('name', camp['id'])}")
                    client.get(f"/api/dashboard/campaigns/{camp['id']}/creatives?camp_type={ct}&date_from={dt_from_30}&date_to={dt_to}", headers={"X-Internal-Scheduler":"daily"})
                    time.sleep(10)
                except Exception as e:
                    print(f"[SCHEDULER]   Erro: {e}")
                    time.sleep(30)

            print("[SCHEDULER] Criativos OK")
        except Exception as e:
            print(f"[SCHEDULER] Erro criativos: {e}")

        # Pausa
        print("[SCHEDULER] Aguardando 30 min...")
        _sleep_with_heartbeat(1800, "daily_scheduler")

        # ── ETAPA 4 (3:30): Breakdowns SO no range principal (30d) ──
        # Breakdowns faz 4+ queries Meta (totals/age/gender/weekday) — pesado.
        # Antes rodava em 2 ranges x 5 tipos = 10 calls. Agora so 30d x 5 = 5.
        try:
            print("[SCHEDULER] Etapa 4/5: Carregando breakdowns (30d apenas)...")
            dt_from_30_bd = (now_br - timedelta(days=30)).strftime("%Y-%m-%d")
            for ct in VALID_CAMP_TYPES:
                print(f"[SCHEDULER]   Breakdowns {ct} 30d")
                client.get(f"/api/dashboard/breakdowns?camp_type={ct}&date_from={dt_from_30_bd}&date_to={dt_to}", headers={"X-Internal-Scheduler":"daily"})
                time.sleep(10)
            print("[SCHEDULER] Breakdowns OK")
        except Exception as e:
            print(f"[SCHEDULER] Erro breakdowns: {e}")

        print("[SCHEDULER] Aguardando 30 min...")
        _sleep_with_heartbeat(1800, "daily_scheduler")

        # ── ETAPA 5 (4:00): all-creatives SO no range principal (30d) e ativas ──
        # Antes rodava em todos os ranges x todos os tipos (10 calls pesadas com
        # pausas de 30s entre cada). Agora so 30d + status=active = 5 calls.
        try:
            print("[SCHEDULER] Etapa 5/5: Carregando todos criativos (30d apenas)...")
            dt_from_30_main = (now_br - timedelta(days=30)).strftime("%Y-%m-%d")
            for ct in VALID_CAMP_TYPES:
                # Meteoricos: usa status=all pra incluir eventos ja encerrados
                # (campanhas viram ARCHIVED rapido, default 'active' deixa
                # a aba vazia entre eventos).
                cs = "all" if ct == CAMP_TYPE_METEORICOS else "active"
                print(f"[SCHEDULER]   All-creatives {ct} 30d (status={cs})")
                client.get(f"/api/dashboard/all-creatives?camp_type={ct}&date_from={dt_from_30_main}&date_to={dt_to}&camp_status={cs}", headers={"X-Internal-Scheduler":"daily"})
                time.sleep(30)
            print("[SCHEDULER] Todos criativos OK")
        except Exception as e:
            print(f"[SCHEDULER] Erro todos criativos: {e}")

        # ── ETAPA 6 (4:30): Meses completos pra aba Inicio ──
        # Idempotente: pula meses ja cacheados (so re-pina TTL). Revalida cada
        # mes UMA vez, ~7 dias apos o fim, pra pegar conversoes atrasadas.
        try:
            print("[SCHEDULER] Etapa 6/6: Meses completos (aba Inicio)...")
            _warmup_monthly_historical()
            print("[SCHEDULER] Meses completos OK")
        except Exception as e:
            print(f"[SCHEDULER] Erro meses completos: {e}")

    print(f"[SCHEDULER] Atualizacao completa! Tudo cacheado ate {datetime.now().strftime('%H:%M')}")


def _test_warmup_quick():
    """Versao enxuta do scheduler pra testar o mecanismo.
    Roda campaigns + daily-summary dos 5 tipos (30d) em sequencia.
    Leva ~2-3 min total, em vez das 2.5h do scheduler completo.
    Usado pelo botao /admin > 'Disparar Warmup Agora' pra validar que o
    thread roda de fato e popula caches antes do 2am de verdade."""
    from datetime import datetime, timedelta
    now_br = _now_br()
    dt_to = (now_br - timedelta(days=1)).strftime("%Y-%m-%d")
    dt_from_30 = (now_br - timedelta(days=30)).strftime("%Y-%m-%d")
    print(f"[TEST-WARMUP] Iniciando em {now_br.strftime('%Y-%m-%d %H:%M %Z')} — PID {os.getpid()}")
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess["logged_in"] = True
            sess["username"] = SUPER_ADMIN_EMAIL
            sess["role"] = "super_admin"
        for ct in VALID_CAMP_TYPES:
            try:
                print(f"[TEST-WARMUP] {ct}: campaigns")
                client.get(f"/api/dashboard/campaigns?camp_type={ct}&date_from={dt_from_30}&date_to={dt_to}&camp_status=all&force=true",
                           headers={"X-Internal-Scheduler":"test_warmup"})
                time.sleep(3)
                print(f"[TEST-WARMUP] {ct}: daily-summary")
                client.get(f"/api/dashboard/daily-summary?camp_type={ct}&date_from={dt_from_30}&date_to={dt_to}&camp_status=all&force=true",
                           headers={"X-Internal-Scheduler":"test_warmup"})
                time.sleep(3)
            except Exception as e:
                print(f"[TEST-WARMUP] Erro em {ct}: {e}")
    print(f"[TEST-WARMUP] COMPLETO em {datetime.now(_BR_TZ).strftime('%H:%M:%S %Z')}")


@app.route("/api/admin/scheduler-test-fire", methods=["POST"])
def api_scheduler_test_fire():
    """Dispara o warmup enxuto em background. Opcional delay_seconds pra
    testar o mecanismo de agendamento (ex: delay=60 pra rodar em 1min)."""
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    try:
        body = request.get_json(silent=True) or {}
        delay = int(body.get("delay_seconds", 0))
        delay = max(0, min(delay, 3600))  # 0 a 1h

        def _run():
            if delay > 0:
                print(f"[TEST-WARMUP] Agendado pra {delay}s adiante — PID {os.getpid()}")
                time.sleep(delay)
            _test_warmup_quick()

        threading.Thread(target=_run, daemon=True).start()
        msg = ("Warmup agendado pra daqui a " + str(delay) + "s"
               if delay > 0
               else "Warmup disparado em background. Aguarde ~2-3min e cheque o Diagnostico.")
        return jsonify({"ok": True, "message": msg, "delay_seconds": delay})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/admin/monthly-warmup-fire", methods=["POST"])
def api_monthly_warmup_fire():
    """Dispara _warmup_monthly_historical em background.
    Usado pra pre-carregar meses completos sob demanda (ex: apos bump de
    cache ou quando o usuario quer acessar um mes que ainda nao foi warmed)."""
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    try:
        months = _completed_months_since(_MONTHLY_START_YEAR, _MONTHLY_START_MONTH)
        threading.Thread(target=_warmup_monthly_historical, daemon=True).start()
        est_min = max(1, len(months) * 2)
        return jsonify({
            "ok": True,
            "message": f"Warmup mensal disparado ({len(months)} meses). Estimado ~{est_min}min. Acompanhe nos Eventos com fonte 'auto:monthly_first' ou 'auto:monthly_revalidate'.",
            "months_count": len(months),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


def _warmup_camp_type(ct, days_list, dt_to):
    """Warmup de todos os endpoints cacheaveis de UM camp_type.
    Usado em paralelo (1 thread por tipo) pra popular cache mais rapido
    apos boot/bump de versao de cache."""
    now_br = _now_br
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess["logged_in"] = True
            sess["username"] = SUPER_ADMIN_EMAIL
            sess["role"] = "super_admin"
        for days in days_list:
            dt_from = (now_br() - timedelta(days=days)).strftime("%Y-%m-%d")
            try:
                k_camp = f"campaigns_v8_{ct}_all_{dt_from}_{dt_to}"
                k_daily = f"daily_summary_v9_{ct}_all_{dt_from}_{dt_to}"
                k_creat = f"all_creatives_v8_{ct}_active_{dt_from}_{dt_to}"
                k_bd = f"breakdowns_v6_{ct}_all_{dt_from}_{dt_to}"
                base = f"camp_type={ct}&date_from={dt_from}&date_to={dt_to}&force=true"

                hdr = {"X-Internal-Scheduler": "warmup"}
                # Warmup ENXUTO: so campaigns + daily_summary (aba Campanhas).
                # all-creatives e breakdowns sao pesados (all-creatives puxa todos
                # os ads de todas campanhas; breakdowns faz 4+ queries Meta) e
                # frequentemente batiam BUC em 90%+. Agora so buscam quando o
                # usuario abre essas abas — e ficam cacheadas apos.
                if should_refresh(k_camp):
                    print(f"[WARMUP-{ct}] campaigns {days}d")
                    client.get(f"/api/dashboard/campaigns?{base}&camp_status=all", headers=hdr)
                    time.sleep(6)
                if should_refresh(k_daily):
                    print(f"[WARMUP-{ct}] daily_summary {days}d")
                    client.get(f"/api/dashboard/daily-summary?{base}&camp_status=all", headers=hdr)
                    time.sleep(6)
            except Exception as e:
                print(f"[WARMUP-{ct}] Erro {days}d: {e}")


def _refresh_recent_loop():
    """Thread leve que revalida o cache so quando necessario.

    Todos os workers iniciam essa thread. O primeiro a chamar
    try_acquire_scheduler_lock("refresh_recent") vence e segue; os outros
    pulam. Isso sobrevive a crash de worker — se o dono atual morrer,
    outro assume na proxima iteracao (dentro de ~2h)."""
    import os as _os
    pid = _os.getpid()
    now_br = _now_br
    # Warmup apos boot — aguarda 30s pra garantir que o app estabilizou
    time.sleep(30)

    # Tenta pegar o lock; se outro worker ja pegou, entra em modo standby
    # (loop de 1min checando). Assim, se o dono atual morrer, este worker
    # assume em ate 1min.
    if not try_acquire_scheduler_lock("refresh_recent"):
        print(f"[REFRESH-LOOP] PID {pid}: standby (outro worker detem o lock)")
        while True:
            time.sleep(60)
            if try_acquire_scheduler_lock("refresh_recent"):
                print(f"[REFRESH-LOOP] PID {pid}: assumindo lock")
                break
    try:
        refresh_scheduler_lock("refresh_recent")
        dt_to = (now_br() - timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"[WARMUP] Checando cache dos {len(VALID_CAMP_TYPES)} tipos (30d) — {datetime.now().strftime('%H:%M')}")
        # Warmup so se o cache nao estiver populado (nao forca refresh desnecessario)
        initial_ranges = [30]
        for ct in VALID_CAMP_TYPES:
            _warmup_camp_type(ct, initial_ranges, dt_to)
            time.sleep(8)  # pausa maior pra distribuir carga
        print(f"[WARMUP] Concluido em {datetime.now().strftime('%H:%M')}")
    except Exception as e:
        print(f"[WARMUP] Erro inicial: {e}")

    # Warmup do endpoint /resumo pras janelas deslizantes (30d, 7d).
    # Pre-warmar account_total_spend ANTES do /resumo pra que o Resumo
    # vire pura soma de caches. Tudo pronto pra aba Inicio de manha.
    # Aborta se BUC critico — retoma no proximo ciclo do refresh loop.
    if _buc_is_critical():
        print("[BOOT] BUC critico — adiando resumo warmup")
        return
    try:
        print("[BOOT] Pre-warming account_total_spend + /resumo (30d, 7d)")
        dt_to_now = (now_br() - timedelta(days=1)).strftime("%Y-%m-%d")
        _all_accs = set()
        for ct in VALID_CAMP_TYPES:
            for acc in _get_accounts_for_type(ct):
                if acc:
                    _all_accs.add(acc)
        with app.test_client() as _c:
            with _c.session_transaction() as sess:
                sess["logged_in"] = True
                sess["username"] = SUPER_ADMIN_EMAIL
                sess["role"] = "super_admin"
            for _days in [30, 7]:
                _df = (now_br() - timedelta(days=_days)).strftime("%Y-%m-%d")
                _dt_to = dt_to_now
                _dt_from_obj = datetime.strptime(_df, "%Y-%m-%d")
                _dt_to_obj = datetime.strptime(_dt_to, "%Y-%m-%d")
                _pdays = (_dt_to_obj - _dt_from_obj).days + 1
                _prev_to = (_dt_from_obj - timedelta(days=1)).strftime("%Y-%m-%d")
                _prev_from = (_dt_from_obj - timedelta(days=_pdays)).strftime("%Y-%m-%d")
                # Pre-warm account_total_spend pras 4 chaves (atual+anterior)
                for acc in _all_accs:
                    try:
                        _fetch_account_total_spend(acc, _df, _dt_to)
                        _fetch_account_total_spend(acc, _prev_from, _prev_to)
                        time.sleep(2)
                    except Exception as e:
                        print(f"[BOOT] Erro account_total {acc} {_days}d: {e}")
                # Agora /resumo — so soma caches, deve ser rapido
                try:
                    _c.get(f"/api/dashboard/resumo?date_from={_df}&date_to={_dt_to}&force=true", headers={"X-Internal-Scheduler":"boot_warmup"})
                    time.sleep(5)
                except Exception as e:
                    print(f"[BOOT] Erro resumo {_days}d: {e}")
                # Drill-down Outros pre-warm (instantaneo quando usuario clicar)
                try:
                    _c.get(f"/api/dashboard/resumo/unmapped?date_from={_df}&date_to={_dt_to}", headers={"X-Internal-Scheduler":"boot_warmup"})
                    time.sleep(3)
                except Exception as e:
                    print(f"[BOOT] Erro resumo/unmapped {_days}d: {e}")
    except Exception as e:
        print(f"[BOOT] Erro resumo boot: {e}")

    # Warmup dos meses completos apos o boot (so o worker que pegou o lock).
    # Idempotente — passa rapido se ja tudo cacheado (so re-pina TTL).
    try:
        print("[BOOT] Disparando warmup mensal historico apos boot")
        _warmup_monthly_historical()
    except Exception as e:
        print(f"[BOOT] Erro warmup mensal: {e}")

    # Popula fila de backfill de atoms (idempotente — so adiciona faltantes)
    try:
        added = _populate_backfill_queue(days_back=30)
        print(f"[BOOT] Atom backfill queue: +{added} atoms enfileirados")
    except Exception as e:
        print(f"[BOOT] Erro popular fila atoms: {e}")

    # Boot one-time: refresh atoms D-1 a D-8 (atualiza dados de ontem
    # ate semana passada, captura atribuicao tardia acumulada). 8 atoms
    # × 3 contas = 24 calls UMA VEZ por deploy. Apos isso, so worker
    # diario faz revalidacao D+2 e D+8 (6 calls/dia). NUNCA refetcha
    # atoms com idade > 8 dias (imutaveis).
    def _bg_boot_refresh():
        try:
            time.sleep(180)  # 3min apos boot pra app estabilizar
            print("[BOOT] Boot refresh: atoms D-1 a D-8 (one-time)")
            r = _revalidate_recent_atoms(days_back=8, force_all=True)
            print(f"[BOOT] Boot refresh concluido: {r}")
        except Exception as e:
            print(f"[BOOT] Erro boot refresh: {e}")
    threading.Thread(target=_bg_boot_refresh, daemon=True).start()

    iteration = 1
    # Ranges cobertos pelo loop (30d e 7d apenas — os mais usados).
    LOOP_RANGES = [30, 7]
    CYCLE_SECONDS = 2 * 3600  # 2h por ciclo (antes 30min)
    while True:
        try:
            refresh_scheduler_lock("refresh_recent")
            dt_to = (now_br() - timedelta(days=1)).strftime("%Y-%m-%d")
            print(f"[REFRESH] Ciclo {iteration}: checando cache — {datetime.now().strftime('%H:%M')}")
            refreshed_count = 0
            with app.test_client() as client:
                with client.session_transaction() as sess:
                    sess["logged_in"] = True
                    sess["username"] = SUPER_ADMIN_EMAIL
                    sess["role"] = "super_admin"
                for days in LOOP_RANGES:
                    dt_from = (now_br() - timedelta(days=days)).strftime("%Y-%m-%d")
                    for ct in VALID_CAMP_TYPES:
                        try:
                            k_camp = f"campaigns_v8_{ct}_all_{dt_from}_{dt_to}"
                            k_daily = f"daily_summary_v9_{ct}_all_{dt_from}_{dt_to}"
                            k_creat = f"all_creatives_v8_{ct}_active_{dt_from}_{dt_to}"
                            base = f"camp_type={ct}&date_from={dt_from}&date_to={dt_to}&force=true"

                            hdr = {"X-Internal-Scheduler": "refresh_loop"}
                            # So refresca se cache realmente precisa (should_refresh verifica TTL).
                            # all-creatives EXCLUIDO do loop — e o mais pesado e
                            # trava BUC. So fetcha quando usuario abre a aba.
                            if should_refresh(k_camp):
                                client.get(f"/api/dashboard/campaigns?{base}&camp_status=all", headers=hdr)
                                refreshed_count += 1; time.sleep(5)
                            if should_refresh(k_daily):
                                client.get(f"/api/dashboard/daily-summary?{base}&camp_status=all", headers=hdr)
                                refreshed_count += 1; time.sleep(5)
                            if refreshed_count > 0:
                                refresh_scheduler_lock("refresh_recent")
                        except Exception as e:
                            print(f"[REFRESH] Erro {days}d/{ct}: {e}")
            print(f"[REFRESH] Ciclo {iteration}: {refreshed_count} caches atualizados — {datetime.now().strftime('%H:%M')}")
        except Exception as e:
            print(f"[REFRESH] Erro no loop: {e}")
        iteration += 1
        # Aguarda CYCLE_SECONDS, com heartbeat a cada 5min para manter o lock
        slept = 0
        while slept < CYCLE_SECONDS:
            time.sleep(300)
            slept += 300
            try: refresh_scheduler_lock("refresh_recent")
            except Exception: pass


# ── ENDPOINTS DA MIGRACAO DE ATOMS ─────────────────────────────────────

@app.route("/api/admin/atom-status")
def api_admin_atom_status():
    """Retorna status completo da migracao pra alimentar o painel /admin.
    Restrito a super_admin (f4cure@gmail.com)."""
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    try:
        _load_backfill_persistent_state()
        _sync_use_atoms_from_file()
        # Contadores
        atoms_meta = list_atoms_metadata(scope='acc')
        # Por conta+data
        by_acc = {}
        atom_dates = set()
        for m in atoms_meta:
            by_acc.setdefault(m['key'], set()).add(m['date'])
            atom_dates.add(m['date'])

        # Fila de backfill
        with _backfill_lock:
            queue = _load_backfill_queue()
        queue_size = len(queue)

        # Total esperado: 30 dias × accounts unicas
        all_accounts = set()
        for ct in VALID_CAMP_TYPES:
            for acc in _get_accounts_for_type(ct):
                if acc:
                    all_accounts.add(acc)
        if ACCOUNT_ID:
            all_accounts.add(ACCOUNT_ID)
        target_days = _backfill_state.get("target_days", 30)
        target_total = target_days * len(all_accounts)
        populated = sum(len(dates) for dates in by_acc.values())

        # Historico (ultimas 50 entradas do log)
        log_path = os.path.join(os.path.dirname(__file__), "atom_migration_log.json")
        history = []
        try:
            if os.path.exists(log_path):
                with open(log_path, "r", encoding="utf-8") as f:
                    full_log = json.load(f)
                    history = full_log[-50:][::-1]  # ultimos 50 invertidos
        except Exception:
            pass

        # Por conta — % populado
        per_account = []
        for acc in sorted(all_accounts):
            dates = by_acc.get(acc, set())
            per_account.append({
                "acc_id": acc,
                "atoms_count": len(dates),
                "target": target_days,
                "pct": round(len(dates) / target_days * 100, 1) if target_days > 0 else 0,
            })

        # BUC info
        try:
            rate_info = get_dashboard_rate_info()
        except Exception:
            rate_info = {"pct": 0, "worst_account": ""}

        return jsonify({
            "ok": True,
            "use_atoms": USE_ATOMS,
            "backfill_paused": _backfill_paused,
            "progress": {
                "populated": populated,
                "target": target_total,
                "queue_size": queue_size,
                "pct": round(populated / target_total * 100, 1) if target_total > 0 else 0,
            },
            "per_account": per_account,
            "pacing": {
                "current_h": _backfill_state.get("current_pacing_h", 10),
                "last_fetch_at": _backfill_state.get("last_fetch_at"),
            },
            "boost": {
                "active": bool(_backfill_boost.get("active") and time.time() < _backfill_boost.get("until_ts", 0)),
                "until_ts": _backfill_boost.get("until_ts", 0),
                "pacing_h": _backfill_boost.get("pacing_h", 90),
                "remaining_minutes": max(0, int((_backfill_boost.get("until_ts", 0) - time.time()) / 60)),
            },
            "buc": {
                "pct": rate_info.get("pct", 0),
                "worst_account": rate_info.get("worst_account", ""),
            },
            "divergences_recent": len(_atom_recent_divergences),
            "history": history,
        })
    except Exception as e:
        import traceback
        return jsonify({"ok": False, "error": str(e), "tb": traceback.format_exc()[:500]}), 500


@app.route("/api/admin/atom-toggle", methods=["POST"])
def api_admin_atom_toggle():
    """Liga/desliga USE_ATOMS manualmente. Super_admin only."""
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    body = request.get_json(silent=True) or {}
    new_value = bool(body.get("enable", False))
    _set_use_atoms(new_value, reason="manual toggle via /admin")
    # Reset divergencias quando ligar manualmente
    if new_value:
        _atom_recent_divergences.clear()
    return jsonify({"ok": True, "use_atoms": USE_ATOMS})


@app.route("/api/admin/atom-backfill-fire", methods=["POST"])
def api_admin_atom_backfill_fire():
    """Acelera o backfill — processa N atoms agora (sincrono, ignora pacing).
    Super_admin only. Retorna resultado de cada atom processado."""
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    body = request.get_json(silent=True) or {}
    n = max(1, min(20, int(body.get("n", 5))))

    def _bg():
        try:
            results = _backfill_force_n(n=n)
            print(f"[BACKFILL] Forced {len(results)} atoms: {sum(1 for r in results if r['status']=='ok')} OK")
        except Exception as e:
            print(f"[BACKFILL] Forced error: {e}")
    threading.Thread(target=_bg, daemon=True).start()
    return jsonify({"ok": True, "message": f"Disparando {n} atoms em background. Acompanhe no painel."})


@app.route("/api/admin/atom-backfill-pause", methods=["POST"])
def api_admin_atom_backfill_pause():
    """Pausa/resume worker de backfill. Super_admin only."""
    global _backfill_paused
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    body = request.get_json(silent=True) or {}
    _backfill_paused = bool(body.get("pause", False))
    return jsonify({"ok": True, "paused": _backfill_paused})


@app.route("/api/admin/atom-revalidate-recent", methods=["POST"])
def api_admin_atom_revalidate_recent():
    """Refetcha atoms dos ultimos N dias (default 7) — captura atribuicao
    tardia da Meta. Super_admin only. Sincrono — leva ~30-60s pra 7 dias."""
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    body = request.get_json(silent=True) or {}
    days = max(1, min(30, int(body.get("days", 7))))

    def _bg():
        try:
            r = _revalidate_recent_atoms(days_back=days, force_all=True)
            print(f"[REVALIDATE] {r}")
        except Exception as e:
            print(f"[REVALIDATE] erro: {e}")
    threading.Thread(target=_bg, daemon=True).start()
    return jsonify({
        "ok": True,
        "message": f"Revalidando atoms dos ultimos {days} dias em background. Acompanhe no painel.",
    })


@app.route("/api/admin/atom-debug-campaigns", methods=["POST"])
def api_admin_atom_debug_campaigns():
    """Debug: lista campanhas no cache por status, mostrando spend/compras.
    NAO chama Meta API — so le cache existente. Super_admin only."""
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    try:
        body = request.get_json(silent=True) or {}
        camp_type = body.get("camp_type", CAMP_TYPE_VENDAS)
        days = max(1, min(60, int(body.get("days", 30))))
        date_to = (_now_br() - timedelta(days=1)).strftime("%Y-%m-%d")
        date_from = (_now_br() - timedelta(days=days)).strftime("%Y-%m-%d")

        accounts = [a for a in _get_accounts_for_type(camp_type) if a]
        all_campaigns = []
        insights_map = {}
        for acc in accounts:
            raw = _fetch_account_raw_v1(acc, "all", date_from, date_to)
            if raw is None:
                continue
            for c in (raw.get("campaigns") or []):
                cc = dict(c); cc["_account_id"] = acc
                all_campaigns.append(cc)
            for cid, ins in (raw.get("insights_by_id") or {}).items():
                if cid not in insights_map:
                    insights_map[cid] = parse_insights(ins, camp_type=camp_type)

        # Filtra pelo tipo
        sales = _filter_campaigns_by_type(all_campaigns, camp_type)

        # Agrupa por status
        by_status = {}
        all_with_spend = []
        for c in sales:
            cid = c.get("id")
            status = c.get("status", "UNKNOWN")
            metrics = insights_map.get(cid, {})
            spend = float(metrics.get("spend", 0) or 0)
            purch = int(metrics.get("purchases", 0) or 0)
            entry = {
                "id": cid,
                "name": c.get("name", ""),
                "status": status,
                "objective": c.get("objective", ""),
                "spend": round(spend, 2),
                "purchases": purch,
                "account_id": c.get("_account_id", ""),
            }
            if status not in by_status:
                by_status[status] = {"count": 0, "spend": 0, "purchases": 0, "campaigns": []}
            by_status[status]["count"] += 1
            by_status[status]["spend"] = round(by_status[status]["spend"] + spend, 2)
            by_status[status]["purchases"] += purch
            by_status[status]["campaigns"].append(entry)
            if spend > 0:
                all_with_spend.append(entry)

        # Top 30 campanhas archived com spend
        archived_with_spend = sorted(
            [e for e in all_with_spend if e["status"] == "ARCHIVED"],
            key=lambda x: x["spend"], reverse=True
        )[:30]
        # Top 30 paused com spend
        paused_with_spend = sorted(
            [e for e in all_with_spend if e["status"] == "PAUSED"],
            key=lambda x: x["spend"], reverse=True
        )[:30]
        # Top 30 active com spend
        active_with_spend = sorted(
            [e for e in all_with_spend if e["status"] == "ACTIVE"],
            key=lambda x: x["spend"], reverse=True
        )[:30]

        return jsonify({
            "ok": True,
            "camp_type": camp_type,
            "date_from": date_from,
            "date_to": date_to,
            "total_campaigns": len(sales),
            "by_status": {k: {"count": v["count"], "spend": v["spend"], "purchases": v["purchases"]} for k, v in by_status.items()},
            "top_archived": archived_with_spend,
            "top_paused": paused_with_spend,
            "top_active": active_with_spend,
        })
    except Exception as e:
        import traceback
        return jsonify({"ok": False, "error": str(e), "tb": traceback.format_exc()[:500]}), 500


@app.route("/api/admin/atom-validate-now", methods=["POST"])
def api_admin_atom_validate_now():
    """Valida atoms vs legacy SINCRONO. Compara 3 metricas principais
    (spend, revenue, purchases) pra todos os tipos no range pedido.
    Super_admin only."""
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    body = request.get_json(silent=True) or {}
    days = max(1, min(60, int(body.get("days", 30))))

    date_to = (_now_br() - timedelta(days=1)).strftime("%Y-%m-%d")
    date_from = (_now_br() - timedelta(days=days)).strftime("%Y-%m-%d")

    def _summary(raw_per_acc, camp_type):
        all_camps = []
        ins_map = {}
        for acc, raw in raw_per_acc.items():
            # Atom path tem 'atom_parsed_metrics_by_id', legacy tem 'insights_by_id'
            if "atom_parsed_metrics_by_id" in raw:
                for c in (raw.get("campaigns") or []):
                    cc = dict(c); cc["_account_id"] = acc
                    all_camps.append(cc)
                for cid, parsed in raw.get("atom_parsed_metrics_by_id", {}).items():
                    if cid not in ins_map:
                        ins_map[cid] = parsed
            else:
                for c in (raw.get("campaigns") or []):
                    cc = dict(c); cc["_account_id"] = acc
                    all_camps.append(cc)
                for cid, ins in (raw.get("insights_by_id") or {}).items():
                    if cid not in ins_map:
                        ins_map[cid] = parse_insights(ins, camp_type=camp_type)
        sales = _filter_campaigns_by_type(all_camps, camp_type)
        ts = tr = tp = 0
        for c in sales:
            m = ins_map.get(c.get("id"), {}) or {}
            ts += m.get("spend", 0)
            tr += m.get("revenue", 0)
            tp += m.get("purchases", 0)
        return {
            "total_campaigns": len(sales),
            "total_spend": round(ts, 2),
            "total_revenue": round(tr, 2),
            "total_purchases": int(tp),
        }

    results = []
    for ct in VALID_CAMP_TYPES:
        accounts = [a for a in _get_accounts_for_type(ct) if a]
        if not accounts:
            continue

        # Atoms
        atoms_raw = {}
        atoms_complete = True
        for acc in accounts:
            r = _build_pseudo_raw_per_account_from_atoms(acc, date_from, date_to, camp_type=ct)
            if r is None:
                atoms_complete = False
                break
            atoms_raw[acc] = r
        if not atoms_complete:
            results.append({"type": ct, "status": "skipped", "reason": "atoms_incomplete"})
            continue
        atoms_sum = _summary(atoms_raw, ct)

        # Legacy (cache atual ou Meta fresh)
        legacy_raw = {}
        for acc in accounts:
            r = _fetch_account_raw_v1(acc, "all", date_from, date_to)
            legacy_raw[acc] = r or {"campaigns": [], "insights_by_id": {}}
        legacy_sum = _summary(legacy_raw, ct)

        # Compara metricas principais
        diffs = {}
        max_diff = 0.0
        for k in ("total_spend", "total_revenue", "total_purchases", "total_campaigns"):
            a = float(atoms_sum.get(k, 0) or 0)
            l = float(legacy_sum.get(k, 0) or 0)
            if l == 0 and a == 0:
                pct = 0.0
            else:
                pct = abs(a - l) / max(abs(l), 1) * 100
            diffs[k] = {
                "atoms": atoms_sum.get(k, 0),
                "legacy": legacy_sum.get(k, 0),
                "diff_pct": round(pct, 4),
            }
            max_diff = max(max_diff, pct)
            _validate_atom_vs_legacy(f"validate_now_{ct}_{k}", a, l, tolerance=0.0001)

        # Tolerancias mais realistas considerando atribuicao tardia da Meta:
        # < 0.1% = OK (perfeito)
        # < 1%   = warning (pequena drift, aceitavel)
        # >= 1%  = diverge (problema)
        status = "ok" if max_diff < 0.1 else ("warning" if max_diff < 1.0 else "diverge")
        results.append({
            "type": ct,
            "status": status,
            "max_diff_pct": round(max_diff, 4),
            "metrics": diffs,
        })

    return jsonify({
        "ok": True,
        "date_from": date_from,
        "date_to": date_to,
        "days": days,
        "results": results,
    })


@app.route("/api/admin/atom-populate-queue", methods=["POST"])
def api_admin_atom_populate_queue():
    """Forca repopulacao da fila (ex: bumpou versao de atom). Super_admin only."""
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    body = request.get_json(silent=True) or {}
    days = max(1, min(400, int(body.get("days_back", 30))))
    added = _populate_backfill_queue(days_back=days)
    return jsonify({"ok": True, "added": added, "days_back": days})


@app.route("/api/admin/atom-boost", methods=["POST"])
def api_admin_atom_boost():
    """Ativa boost mode no backfill worker: pacing acelerado por janela limitada.
    Body: {days_back: int (1-400), pacing_h: int (10-200), duration_h: float (0.1-48)}
    Faz tudo em 1 chamada: popula fila e ativa boost.
    Boost respeita BUC critico (>=70% volta pro pacing seguro). Super_admin only."""
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    body = request.get_json(silent=True) or {}
    days = max(1, min(400, int(body.get("days_back", 365))))
    pacing_h = max(10, min(200, int(body.get("pacing_h", 90))))
    duration_h = max(0.1, min(48.0, float(body.get("duration_h", 14.0))))
    seconds_between = max(18, int(round(3600 / pacing_h)))
    until_ts = time.time() + duration_h * 3600

    added = _populate_backfill_queue(days_back=days)
    _backfill_boost["active"] = True
    _backfill_boost["until_ts"] = until_ts
    _backfill_boost["seconds_between"] = seconds_between
    _backfill_boost["pacing_h"] = pacing_h
    _save_backfill_persistent_state()
    _log_atom_event("boost_started", {
        "days_back": days,
        "pacing_h": pacing_h,
        "seconds_between": seconds_between,
        "duration_h": duration_h,
        "added_to_queue": added,
    })
    return jsonify({
        "ok": True,
        "days_back": days,
        "pacing_h": pacing_h,
        "duration_h": duration_h,
        "added_to_queue": added,
        "until_ts": until_ts,
    })


@app.route("/api/admin/atom-boost-stop", methods=["POST"])
def api_admin_atom_boost_stop():
    """Desliga boost mode imediatamente. Super_admin only."""
    if not session.get("logged_in") or not _is_super_admin(session.get("username")):
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    _backfill_boost["active"] = False
    _save_backfill_persistent_state()
    _log_atom_event("boost_ended", {"reason": "manual_stop"})
    return jsonify({"ok": True})


# ── Bootstrap dos schedulers (roda tanto em `python` quanto em `gunicorn`) ───
# Cada worker do gunicorn inicia AMBAS as threads. A disputa pelo lock acontece
# na HORA de disparar (nao no boot). Assim, se o worker dono do lock morrer
# antes do horario, outro worker ainda roda na proxima janela.
#
# Antes: soh 1 worker startava a thread no boot. Se ele morresse, ninguem mais
# rodava o scheduler ate a proxima reboot — e a lock em SQLite ficava com idade
# < 15min, bloqueando qualquer tentativa de takeover. Era o bug reportado de
# morning-slowness todo dia.
start_scheduler(_scheduled_refresh)
print("[BOOT] Scheduler diario (2:00 BRT) iniciado")

threading.Thread(target=_refresh_recent_loop, daemon=True).start()
print("[BOOT] Loop de refresh recente (1d/7d a cada 2h) iniciado")

# Worker de backfill de atoms — popula atom cache durante a madrugada
_start_backfill_worker()
print("[BOOT] Backfill de atoms iniciado (pacing 10/h, auto-throttle BUC)")


# ── Run ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  IBC Dashboard de Performance - http://localhost:5001")
    print(f"  Login: {SUPER_ADMIN_EMAIL}")
    print("=" * 60)

    # Schedulers ja foram iniciados no import do modulo (ver bloco bootstrap acima)
    print("  Schedulers em background (ver logs [BOOT])")
    print("=" * 60)

    app.run(host="0.0.0.0", port=5001, debug=True)
