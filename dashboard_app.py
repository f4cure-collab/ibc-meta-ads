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
from cache_manager import get_cached, set_cached, clear_cache, cache_stats, start_scheduler, clear_expired, try_acquire_scheduler_lock, refresh_scheduler_lock, should_refresh
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


def _fetch_insights_for_tagged_campaigns(campaigns, base_params, extra_filters=None):
    """Busca insights para campanhas taggueadas com _account_id, agrupando as chamadas
    por conta. base_params deve ter fields, time_range, level, limit, etc. mas NAO
    filtering por campaign.id (sera injetado automaticamente)."""
    by_acc = {}
    for c in campaigns:
        acc = c.get("_account_id") or ACCOUNT_ID
        by_acc.setdefault(acc, []).append(c["id"])

    all_rows = []
    for acc, ids in by_acc.items():
        if not acc or not ids:
            continue
        filters = [{"field": "campaign.id", "operator": "IN", "value": ids}]
        if extra_filters:
            filters.extend(extra_filters)
        params = dict(base_params)
        params["filtering"] = json.dumps(filters)
        try:
            rows = meta_get_all_pages(f"{acc}/insights", params)
            all_rows.extend(rows)
        except Exception as e:
            print(f"[MULTI-ACCT] Falha insights {acc}: {e}")
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

CAMP_TYPE_VENDAS = "vendas"
CAMP_TYPE_METEORICOS = "meteoricos"
CAMP_TYPE_COMERCIAL = "comercial"
CAMP_TYPE_CRESCIMENTO = "crescimento"
VALID_CAMP_TYPES = (CAMP_TYPE_VENDAS, CAMP_TYPE_METEORICOS, CAMP_TYPE_COMERCIAL, CAMP_TYPE_CRESCIMENTO)

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
    # Trata como separadores: hifen, ponto, espaco, brackets, parenteses, barras, virgula
    for sep in ["-", ".", " ", "[", "]", "(", ")", "/", "\\", ","]:
        u = u.replace(sep, "_")
    u = (u.replace("Ç", "C").replace("Ã", "A").replace("Á", "A")
         .replace("É", "E").replace("Í", "I").replace("Ó", "O").replace("Ú", "U"))
    return set(t for t in u.split("_") if t)


def _is_meteoricos_campaign(name):
    """True se o nome contem token METEORICO/METEORICOS em qualquer posicao."""
    tokens = _name_tokens(name)
    return "METEORICO" in tokens or "METEORICOS" in tokens


def _is_crescimento_campaign(name):
    """True se o nome contem token CRESCIMENTO em qualquer posicao."""
    tokens = _name_tokens(name)
    return "CRESCIMENTO" in tokens


def _is_comercial_campaign(name):
    """True se o nome contem algum token de produto comercial (MTR/PSC/OHIO/CSI/PNL)
    E nao for campanha de nutricao/remarketing (que nao gera lead novo)."""
    tokens = _name_tokens(name)
    if not tokens:
        return False
    exclude = {"NUTRICAO", "RMKT", "REMARKETING", "RETARGETING", "NURTURE"}
    if tokens & exclude:
        return False
    return any(k in tokens for k in COMERCIAL_PRODUCTS)


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
        else:
            if c.get("objective") == "OUTCOME_SALES":
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
    """Estima uso atual aplicando decay linear desde o ultimo check.
    Meta usa janela rolante de ~1h (3600s) para call/cpu/time. Se ja passou
    X segundos sem novas chamadas, o contador real da Meta caiu por X/3600.
    Essa aproximacao evita mostrar 71% quando nada bate na API ha 10min."""
    last = u.get("last_check", 0)
    if not last:
        return 0, 0, 0
    elapsed = time.time() - last
    # Janela Meta ~1h; decay linear. Ignora se muito antigo.
    if elapsed > 3600:
        return 0, 0, 0
    factor = max(0.0, 1.0 - (elapsed / 3600.0))
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
    """Garante delay minimo entre chamadas e pausa se proximo do limite.
    Usa pct decayed (_worst_usage_pct) pra nao travar por valor obsoleto."""
    global _last_call_time
    now = time.time()
    elapsed = now - _last_call_time
    if elapsed < _MIN_DELAY:
        time.sleep(_MIN_DELAY - elapsed)

    pct, acc = _worst_usage_pct()  # ja aplica decay temporal
    if pct > 75:
        wait = 30
        print(f"[RATE LIMIT] {acc}: {pct}% (decayed) — pausando {wait}s")
        time.sleep(wait)
    elif pct > 50:
        print(f"[RATE LIMIT] {acc}: {pct}% (decayed) — desacelerando")
        time.sleep(3)

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


def meta_get(endpoint, params=None):
    _enforce_rate_limit()
    p = {"access_token": TOKEN}
    if params:
        p.update(params)
    resp = requests.get(f"{BASE_URL}/{endpoint}", params=p, timeout=60)
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


def _extract_results_from_row(row):
    """Extrai o 'results' da campanha (Meta coluna 'Resultados' do Gerenciador).
    Retorna a soma dos results de todos os action_types. Cai para link_click se
    a campanha nao tem results (ex: campanhas antigas ou sem objetivo definido)."""
    results = row.get("results") or []
    total = 0
    for r in results:
        try:
            total += int(float(r.get("value", 0) or 0))
        except Exception:
            pass
    if total > 0:
        return total
    # Fallback: link_click como proxy de visitas ao perfil
    return _extract_link_clicks_from_row(row)


# Peso relativo de campanhas NAO-Crescimento na atribuicao de seguidores.
# Default 0.025 = R$1 em outra campanha gera 0.025 seguidor vs 1.0 em Crescimento.
# Calibrado pelos dados reais: 109.258 atribuido Meta / 125.381 NET IG = 87%.
# Valor menor dessa tabela significa que campanhas que nao otimizam seguidor
# contribuem pouco pro ganho do perfil.
CRESCIMENTO_NON_CRESCIMENTO_WEIGHT = 0.025


def _fetch_account_total_spend(acc_id, date_from, date_to):
    """Retorna gasto total de UMA conta no periodo (todas campanhas, todos tipos).
    Cache interno via cache_manager — TTL 20min."""
    cache_key = f"account_total_spend_{acc_id}_{date_from}_{date_to}"
    cached = get_cached(cache_key)
    if cached is not None:
        return cached
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
    """Atribuicao de seguidores TOTAL proporcional aos 'results' (Visitas ao perfil) de cada campanha.

    Meta UI mostra coluna 'Resultados' com visitas ao perfil do Instagram. Esse numero
    e mais preciso que link_click (que inclui cliques que vao pra landing pages, nao so
    pro perfil). A API Meta expoe isso via campo 'results'. Fallback para link_click se
    results estiver ausente.

    ig_net_total: ganho liquido TOTAL do periodo (FOLLOWER - NON_FOLLOWER) do perfil IG
    crescimento_share: fracao do NET atribuivel a Crescimento (0-1)

    Para cada campanha:
      seguidores = ig_net_total * crescimento_share * (results_camp / total_results_cresc)

    Returns: dict {(campaign_id, date): seguidores_atribuidos_float}
    """
    # Agrupa results (visitas ao perfil) por campanha
    results_by_cid = {}
    days_by_cid = {}
    for row in daily_rows:
        cid = row.get("campaign_id", "")
        d = row.get("date_start", "")
        if not cid or not d:
            continue
        r = _extract_results_from_row(row)
        results_by_cid[cid] = results_by_cid.get(cid, 0) + r
        days_by_cid.setdefault(cid, []).append((d, r))

    total_results = sum(results_by_cid.values())
    if total_results <= 0 or ig_net_total <= 0:
        return {}

    gain_for_cresc = ig_net_total * crescimento_share
    attribution = {}

    for cid, total_cid_results in results_by_cid.items():
        if total_cid_results <= 0:
            continue
        cid_total_follows = gain_for_cresc * (total_cid_results / total_results)
        # Dentro da campanha, distribui por dia proporcional aos results do dia
        for (date, results_day) in days_by_cid[cid]:
            if results_day > 0:
                attribution[(cid, date)] = cid_total_follows * (results_day / total_cid_results)
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
    "reach,frequency,ctr,cpm,cpp,cpc,actions,action_values,purchase_roas"
)
INSIGHT_FIELDS_AD = (
    "ad_id,ad_name,spend,impressions,clicks,inline_link_clicks,"
    "reach,frequency,ctr,cpm,cpc,actions,action_values,purchase_roas,date_start"
)
INSIGHT_FIELDS_DAILY = (
    "spend,impressions,clicks,inline_link_clicks,reach,frequency,"
    "actions,action_values,purchase_roas,date_start"
)
INSIGHT_FIELDS_DAILY_CAMP = (
    "campaign_id,campaign_name,spend,impressions,clicks,inline_link_clicks,reach,frequency,"
    "actions,action_values,purchase_roas,results,date_start"
)


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

        cache_key = f"campaigns_{camp_type}_{camp_status}_{date_from}_{date_to}"
        if not force:
            cached = get_cached(cache_key)
            if cached:
                return jsonify(cached)

        # 1. Buscar campanhas do tipo selecionado (multi-conta)
        sales_campaigns = _fetch_type_campaigns(
            camp_type,
            "id,name,status,objective,daily_budget,lifetime_budget,start_time,created_time",
            _camp_status_filter(camp_status)
        )

        if not sales_campaigns:
            return jsonify({"ok": True, "data": [], "summary": {}})

        # 2. Buscar insights (agrupado por conta via _account_id)
        insights_data = _fetch_insights_for_tagged_campaigns(
            sales_campaigns,
            base_params={
                "fields": INSIGHT_FIELDS_CAMPAIGN,
                "time_range": json.dumps({"since": date_from, "until": date_to}),
                "level": "campaign",
                "limit": 500,
            },
            extra_filters=[{"field": "impressions", "operator": "GREATER_THAN", "value": 0}]
        )

        insights_map = {}
        for row in insights_data:
            insights_map[row.get("campaign_id")] = parse_insights(row, camp_type=camp_type)

        # 2b. Crescimento: sobrescreve 'purchases' com seguidores atribuidos do IG
        # (Meta Marketing API nao expoe follow por campanha — usamos IG Graph API
        # com atribuicao proporcional por link_click, ponderada por gasto total)
        if camp_type == CAMP_TYPE_CRESCIMENTO:
            try:
                # Usa TOTAL do periodo (daily breakdown nao funciona na IG API)
                ig_follower, ig_non = fetch_ig_follower_gain_total(IG_PROFILE_ID_JRM, date_from, date_to)
                ig_net_total = ig_follower - ig_non
                if ig_net_total > 0:
                    daily_rows = _fetch_insights_for_tagged_campaigns(
                        sales_campaigns,
                        base_params={
                            # 'results' = coluna 'Resultados' do Gerenciador = Visitas ao perfil
                            "fields": "campaign_id,date_start,inline_link_clicks,actions,spend,results",
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

        # Ordenar por gasto (maior primeiro)
        result.sort(key=lambda x: x.get("spend", 0), reverse=True)

        summary = {
            "total_campaigns": len(sales_campaigns),
            "total_spend": round(total_spend, 2),
            "total_revenue": round(total_revenue, 2),
            "total_purchases": total_purchases,
            "avg_roas": round(total_revenue / total_spend, 2) if total_spend > 0 else 0,
            "avg_cpa": round(total_spend / total_purchases, 2) if total_purchases > 0 else 0,
            "avg_cpm": round((total_spend / total_impressions) * 1000, 2) if total_impressions > 0 else 0,
            "avg_ctr": round((total_clicks / total_impressions) * 100, 2) if total_impressions > 0 else 0,
            "total_impressions": total_impressions,
            "total_clicks": total_clicks,
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

        cache_key = f"multi_insights_{camp_type}_{ids_param}_{camp_status}_{date_from}_{date_to}"
        if not force:
            cached = get_cached(cache_key)
            if cached:
                return jsonify(cached)

        if ids_param == "all":
            # Buscar campanhas do tipo selecionado (multi-conta)
            sales_camps = _fetch_type_campaigns(
                camp_type, "id,name,objective", _camp_status_filter(camp_status)
            )
            sales_map = {c["id"]: c for c in sales_camps}
            target_ids = list(sales_map.keys())
        else:
            # IDs específicos: aceitar direto sem validar status
            target_ids = [i.strip() for i in ids_param.split(",") if i.strip()]
            # Buscar nomes das campanhas de todas as contas relevantes
            sales_map = {}
            for acc in _get_accounts_for_type(camp_type):
                try:
                    rows = meta_get_all_pages(
                        f"{acc}/campaigns",
                        {"fields": "id,name,objective", "effective_status": '["ACTIVE","PAUSED"]'}
                    )
                    for c in rows:
                        c["_account_id"] = acc
                        sales_map[c["id"]] = c
                except Exception as e:
                    print(f"[MULTI-ACCT] Falha multi-insights {acc}: {e}")

        if not target_ids:
            return jsonify({"ok": True, "campaigns": []})

        # Monta lista de campanhas taggueadas para agrupar queries por conta
        tagged_camps = []
        for cid in target_ids:
            c = sales_map.get(cid, {"id": cid})
            # Garante que tem _account_id (fallback para conta principal se desconhecido)
            if not c.get("_account_id"):
                c = dict(c)
                c["_account_id"] = ACCOUNT_ID
            c.setdefault("id", cid)
            tagged_camps.append(c)

        # Insights diarios: agrupa por conta e faz 1 query por conta
        rows = _fetch_insights_for_tagged_campaigns(
            tagged_camps,
            base_params={
                "fields": INSIGHT_FIELDS_DAILY_CAMP,
                "time_range": json.dumps({"since": date_from, "until": date_to}),
                "time_increment": 1,
                "level": "campaign",
                "limit": 500,
            },
            extra_filters=[{"field": "impressions", "operator": "GREATER_THAN", "value": 0}]
        )

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
            # Sobrescreve purchases com seguidores atribuidos do dia (crescimento)
            if crescimento_attr:
                attr_val = crescimento_attr.get((cid, parsed["date"]), 0)
                parsed["purchases"] = int(round(attr_val))
                if parsed.get("spend", 0) > 0 and parsed["purchases"] > 0:
                    parsed["cpa"] = round(parsed["spend"] / parsed["purchases"], 2)
                else:
                    parsed["cpa"] = 0
            by_camp[cid]["daily"].append(parsed)

        # Ordenar daily por data
        result = []
        for cid in target_ids:
            entry = by_camp.get(cid, {"id": cid, "name": sales_map.get(cid, {}).get("name", cid), "daily": []})
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

        for a in ads:
            conv = a.get("purchases", 0)
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
    view_content = 0
    add_to_cart = 0
    initiate_checkout = 0
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
        view_content += m.get("view_content", 0)
        add_to_cart += m.get("add_to_cart", 0)
        initiate_checkout += m.get("initiate_checkout", 0)

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

    return {
        "spend": round(spend, 2), "impressions": impressions, "clicks": clicks,
        "reach": max_reach, "frequency": round(frequency, 2),
        "purchases": purchases, "revenue": round(revenue, 2), "roas": round(roas, 2),
        "cpa": round(cpa, 2), "ctr": round(ctr, 2), "cpm": round(cpm, 2),
        "link_clicks": link_clicks, "cost_per_link_click": round(cost_per_link_click, 2),
        "lpv": lpv, "view_content": view_content, "add_to_cart": add_to_cart,
        "initiate_checkout": initiate_checkout,
        "rate_click_lpv": round(rate_click_lpv, 2),
        "rate_lpv_ic": round(rate_lpv_ic, 2),
        "rate_ic_purchase": round(rate_ic_purchase, 2),
        "rate_click_purchase": round(rate_click_purchase, 2),
        "cost_per_ic": round(cost_per_ic, 2),
        "cost_per_lpv": round(cost_per_lpv, 2),
    }


def _fetch_creatives_for_campaigns(sales_campaigns, date_from, date_to, warnings=None):
    """Busca criativos de campanhas com métricas avançadas.

    Otimização: primeiro verifica se a campanha tem impressões no período
    antes de buscar ads e insights detalhados.
    """
    if warnings is None:
        warnings = []
    insight_fields = INSIGHT_FIELDS_AD
    ads_by_campaign = {}
    daily_by_ad = {}

    # Pré-filtrar: buscar quais campanhas têm impressões no período (1 call batch)
    camp_ids = [c["id"] for c in sales_campaigns]
    camps_with_data = set()
    try:
        check_rows = meta_get_all_pages(
            f"{ACCOUNT_ID}/insights",
            {
                "fields": "campaign_id,impressions",
                "time_range": json.dumps({"since": date_from, "until": date_to}),
                "level": "campaign",
                "filtering": json.dumps([
                    {"field": "campaign.id", "operator": "IN", "value": camp_ids},
                    {"field": "impressions", "operator": "GREATER_THAN", "value": 0},
                ]),
                "limit": 500,
            }
        )
        camps_with_data = {r.get("campaign_id") for r in check_rows}
        print(f"[OPT] {len(camps_with_data)}/{len(sales_campaigns)} campanhas com impressões no período")
    except Exception as e:
        print(f"[WARN] Pré-filtro falhou, buscando todas: {e}")
        camps_with_data = set(camp_ids)

    for camp in sales_campaigns:
        # Pular campanhas sem impressões
        if camp["id"] not in camps_with_data:
            continue

        # Call 1: ads da campanha
        try:
            ads = meta_get_all_pages(
                f"{camp['id']}/ads",
                {
                    "fields": "id,name,status,created_time,creative{id,name,thumbnail_url}",
                    "limit": 200,
                }
            )
        except Exception as e:
            warnings.append({
                "campaign_id": camp["id"],
                "campaign_name": camp.get("name", ""),
                "step": "fetch_ads",
                "error": str(e),
            })
            print(f"[ERROR] ads falhou para {camp.get('name')}: {e}")
            continue

        if not ads:
            continue

        # Call 2: insights diários com filtro impressions > 0
        try:
            daily_rows = meta_get_all_pages(
                f"{camp['id']}/insights",
                {
                    "fields": insight_fields,
                    "time_range": json.dumps({"since": date_from, "until": date_to}),
                    "level": "ad",
                    "time_increment": 1,
                    "filtering": json.dumps([{"field": "impressions", "operator": "GREATER_THAN", "value": 0}]),
                    "limit": 500,
                }
            )
        except Exception as e:
            warnings.append({
                "campaign_id": camp["id"],
                "campaign_name": camp.get("name", ""),
                "step": "fetch_insights",
                "error": str(e),
            })
            print(f"[ERROR] insights falhou para {camp.get('name')}: {e}")
            daily_rows = []

        # Agrupar daily rows por ad_id
        insights_map = {}
        for row in daily_rows:
            ad_id = row.get("ad_id")
            if ad_id not in daily_by_ad:
                daily_by_ad[ad_id] = []
            daily_by_ad[ad_id].append(row)

        # Agregar total por ad
        for ad in ads:
            ad_id = ad["id"]
            if ad_id in daily_by_ad:
                insights_map[ad_id] = _aggregate_daily_total(daily_by_ad[ad_id])

        empty_metrics = {
            "spend": 0, "impressions": 0, "clicks": 0, "reach": 0,
            "purchases": 0, "revenue": 0, "roas": 0, "cpa": 0,
            "ctr": 0, "cpm": 0, "cpp": 0, "link_clicks": 0,
            "cost_per_link_click": 0,
        }

        camp_ads = []
        for ad in ads:
            creative = ad.get("creative", {})
            ad_metrics = insights_map.get(ad["id"])
            if not ad_metrics or ad_metrics.get("impressions", 0) == 0:
                continue  # Pular ads sem impressões no período
            metrics = {**empty_metrics, **ad_metrics}

            created = ad.get("created_time", "")
            days_active = 0
            dt_end = datetime.strptime(date_to, "%Y-%m-%d")
            if created:
                try:
                    days_active = (dt_end - datetime.fromisoformat(created[:10])).days
                except Exception:
                    pass

            entry = {
                "campaign_id": camp["id"],
                "campaign_name": camp.get("name", ""),
                "ad_id": ad["id"],
                "ad_name": ad.get("name", ""),
                "ad_status": ad.get("status", ""),
                "creative_id": creative.get("id", ""),
                "creative_name": creative.get("name", ""),
                "thumbnail_url": creative.get("thumbnail_url", ""),
                "days_active": days_active,
                **metrics,
            }
            camp_ads.append(entry)

        ads_by_campaign[camp["id"]] = {"name": camp.get("name", ""), "ads": camp_ads}

    # Calcular métricas avançadas (3d, 7d, velocity, trend, etc) usando dados diários já cacheados
    _compute_advanced_metrics(ads_by_campaign, daily_by_ad, date_from, date_to)

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

        cache_key = f"daily_summary_{camp_type}_{camp_status}_{date_from}_{date_to}"
        if not force:
            cached = get_cached(cache_key)
            if cached:
                return jsonify(cached)

        # 1. Buscar campanhas do tipo selecionado (multi-conta)
        sales_campaigns = _fetch_type_campaigns(
            camp_type, "id,name,objective", _camp_status_filter(camp_status)
        )

        if not sales_campaigns:
            return jsonify({"ok": True, "data": []})

        # 2. Buscar insights diarios agrupados por conta
        rows = _fetch_insights_for_tagged_campaigns(
            sales_campaigns,
            base_params={
                "fields": INSIGHT_FIELDS_DAILY,
                "time_range": json.dumps({"since": date_from, "until": date_to}),
                "time_increment": 1,
                "level": "campaign",
                "limit": 500,
            },
            extra_filters=[{"field": "impressions", "operator": "GREATER_THAN", "value": 0}]
        )

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
                    "lpv": 0, "initiate_checkout": 0,
                }
            by_date[d]["spend"] += parsed.get("spend", 0)
            by_date[d]["revenue"] += parsed.get("revenue", 0)
            by_date[d]["purchases"] += parsed.get("purchases", 0)
            by_date[d]["impressions"] += parsed.get("impressions", 0)
            by_date[d]["clicks"] += parsed.get("clicks", 0)
            by_date[d]["link_clicks"] += parsed.get("link_clicks", 0)
            by_date[d]["lpv"] += parsed.get("lpv", 0)
            by_date[d]["initiate_checkout"] += parsed.get("initiate_checkout", 0)

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

        cache_key = f"all_creatives_{camp_type}_{camp_status}_{date_from}_{date_to}"
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
        if not warnings:
            set_cached(cache_key, response, ttl_hours=_cache_ttl_for_range(date_from, date_to))
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
        camp_type = _camp_type_from_request()
        g.camp_type = camp_type

        blocked = _enforce_range_for_role(date_from, date_to)
        if blocked:
            return blocked
        campaign_id = request.args.get("campaign_id", "")

        cache_key = f"breakdowns_{camp_type}_{campaign_id or 'all'}_{date_from}_{date_to}"
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

        ins_fields = "spend,impressions,clicks,actions,action_values,purchase_roas,website_purchase_roas"

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

        # 0. Buscar totais gerais para calcular ticket médio
        _enforce_rate_limit()
        totals_data = meta_get_all_pages(endpoint, {
            **base_params,
            "fields": "spend,actions,action_values",
        })
        total_spend = sum(float(r.get("spend", 0)) for r in totals_data)
        total_conv = 0
        total_revenue = 0
        for r in totals_data:
            c, rev, _ = extract_purchase(r)
            total_conv += c
            total_revenue += rev
        ticket_medio = total_revenue / total_conv if total_conv > 0 else 0

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
            "fields": "spend,impressions,clicks,actions,action_values,purchase_roas",
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
        age_result = []
        for row in age_data:
            conv, revenue, roas = extract_purchase(row)
            spend = float(row.get("spend", 0))
            roas, revenue = calc_roas_fallback(conv, revenue, roas, spend)
            age_result.append({
                "age": row.get("age", "?"),
                "spend": round(spend, 2),
                "impressions": int(row.get("impressions", 0)),
                "clicks": int(row.get("clicks", 0)),
                "conversions": conv,
                "revenue": round(revenue, 2),
                "roas": roas,
                "cpa": round(spend / conv, 2) if conv > 0 else 0,
            })

        # Processar sexo
        gender_result = []
        gender_labels = {"male": "Masculino", "female": "Feminino", "unknown": "Desconhecido"}
        for row in gender_data:
            conv, revenue, roas = extract_purchase(row)
            spend = float(row.get("spend", 0))
            roas, revenue = calc_roas_fallback(conv, revenue, roas, spend)
            gender_result.append({
                "gender": gender_labels.get(row.get("gender", ""), row.get("gender", "?")),
                "spend": round(spend, 2),
                "impressions": int(row.get("impressions", 0)),
                "clicks": int(row.get("clicks", 0)),
                "conversions": conv,
                "revenue": round(revenue, 2),
                "roas": roas,
                "cpa": round(spend / conv, 2) if conv > 0 else 0,
            })

        # Processar dia da semana
        weekdays = {0: "Segunda", 1: "Terca", 2: "Quarta", 3: "Quinta", 4: "Sexta", 5: "Sabado", 6: "Domingo"}
        weekday_totals = {i: {"spend": 0, "impressions": 0, "clicks": 0, "conversions": 0, "revenue": 0, "days": 0} for i in range(7)}

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
            weekday_totals[wd]["days"] += 1

        weekday_result = []
        for i in range(7):
            t = weekday_totals[i]
            days_count = max(t["days"], 1)
            rev = t["revenue"]
            if rev == 0 and t["conversions"] > 0:
                rev = t["conversions"] * ticket_medio
            weekday_result.append({
                "day": weekdays[i],
                "day_num": i,
                "spend": round(t["spend"], 2),
                "spend_avg": round(t["spend"] / days_count, 2),
                "conversions": t["conversions"],
                "conv_avg": round(t["conversions"] / days_count, 1),
                "revenue": round(rev, 2),
                "roas": round(rev / t["spend"], 2) if t["spend"] > 0 else 0,
                "impressions": t["impressions"],
                "clicks": t["clicks"],
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
    return jsonify({"ok": True, "blocks": blocks[:60]})


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
        meta["label"] = extra.get("label") or meta.get("name") or acc_id
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
    if not acc_id.startswith("act_"):
        acc_id = "act_" + acc_id.lstrip("act_").strip()
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
            "label": label or acc_id,
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
                # Tudo que pode conter follow: actions, unique_actions, conversions, cost_per_*
                "fields": "campaign_id,campaign_name,spend,impressions,actions,unique_actions,conversions,conversion_values,cost_per_action_type,cost_per_unique_action_type,cost_per_conversion",
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
                "actions_sample": (top.get("actions") or [])[:50],
                "unique_actions_sample": (top.get("unique_actions") or [])[:50],
                "conversions_sample": (top.get("conversions") or [])[:50],
                "cost_per_action_type_sample": (top.get("cost_per_action_type") or [])[:50],
            }

        return jsonify({
            "ok": True,
            "total": len(filtered),
            "period": f"{dt_from} -> {dt_to}",
            "total_spend": round(total_spend, 2),
            "current_follow_types": FOLLOW_TYPES,
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
            if _is_meteoricos_campaign(name):
                auto_type = CAMP_TYPE_METEORICOS
            elif _is_comercial_campaign(name):
                auto_type = CAMP_TYPE_COMERCIAL
            elif _is_crescimento_campaign(name):
                auto_type = CAMP_TYPE_CRESCIMENTO
            elif c.get("objective") == "OUTCOME_SALES":
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

def _scheduled_refresh():
    """Pre-carrega campanhas e criativos em etapas com intervalos para não sobrecarregar a API.

    Cobre os ranges mais usados (7d, 14d, 30d, 60d) para que o cache sirva
    qualquer seleção de periodo no dashboard sem precisar bater na API.
    """
    from datetime import datetime, timedelta

    refresh_scheduler_lock("daily_scheduler")
    now_br = _now_br()
    dt_to = (now_br - timedelta(days=1)).strftime("%Y-%m-%d")
    # Ranges pre-carregados: (dias, label). 30d e o range principal usado no /criativos
    preload_ranges = [1, 7, 14, 30, 60]
    ranges = [(d, (now_br - timedelta(days=d)).strftime("%Y-%m-%d")) for d in preload_ranges]

    print(f"[SCHEDULER] Iniciando atualizacao automatica — ranges: {preload_ranges} dias, ate {dt_to}")
    clear_expired()

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
                    client.get(f"/api/dashboard/campaigns?camp_type={ct}&date_from={dt_from}&date_to={dt_to}&camp_status=all&force=true")
                    time.sleep(5)
            print("[SCHEDULER] Campanhas OK")
        except Exception as e:
            print(f"[SCHEDULER] Erro campanhas: {e}")

        # Pausa de 30 min entre etapas
        print("[SCHEDULER] Aguardando 30 min antes da proxima etapa...")
        time.sleep(1800)

        # ── ETAPA 2 (2:30): Resumo diario (todos os ranges, ambos tipos) ──
        try:
            print("[SCHEDULER] Etapa 2/5: Carregando resumo diario...")
            for days, dt_from in ranges:
                for ct in VALID_CAMP_TYPES:
                    print(f"[SCHEDULER]   Resumo diario {ct} {days}d")
                    client.get(f"/api/dashboard/daily-summary?camp_type={ct}&date_from={dt_from}&date_to={dt_to}&camp_status=all")
                    time.sleep(5)
            print("[SCHEDULER] Resumo diario OK")
        except Exception as e:
            print(f"[SCHEDULER] Erro resumo diario: {e}")

        # Pausa
        print("[SCHEDULER] Aguardando 30 min...")
        time.sleep(1800)

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
                    client.get(f"/api/dashboard/campaigns/{camp['id']}/creatives?camp_type={ct}&date_from={dt_from_30}&date_to={dt_to}")
                    time.sleep(10)
                except Exception as e:
                    print(f"[SCHEDULER]   Erro: {e}")
                    time.sleep(30)

            print("[SCHEDULER] Criativos OK")
        except Exception as e:
            print(f"[SCHEDULER] Erro criativos: {e}")

        # Pausa
        print("[SCHEDULER] Aguardando 30 min...")
        time.sleep(1800)

        # ── ETAPA 4 (3:30): Breakdowns demograficos (todos os ranges, ambos tipos) ──
        try:
            print("[SCHEDULER] Etapa 4/5: Carregando breakdowns...")
            for days, dt_from in ranges:
                for ct in VALID_CAMP_TYPES:
                    print(f"[SCHEDULER]   Breakdowns {ct} {days}d")
                    client.get(f"/api/dashboard/breakdowns?camp_type={ct}&date_from={dt_from}&date_to={dt_to}")
                    time.sleep(10)
            print("[SCHEDULER] Breakdowns OK")
        except Exception as e:
            print(f"[SCHEDULER] Erro breakdowns: {e}")

        print("[SCHEDULER] Aguardando 30 min...")
        time.sleep(1800)

        # ── ETAPA 5 (4:00): Todos criativos consolidado (todos os ranges, ambos tipos) ──
        try:
            print("[SCHEDULER] Etapa 5/5: Carregando todos criativos consolidados...")
            for days, dt_from in ranges:
                for ct in VALID_CAMP_TYPES:
                    print(f"[SCHEDULER]   All-creatives {ct} {days}d")
                    client.get(f"/api/dashboard/all-creatives?camp_type={ct}&date_from={dt_from}&date_to={dt_to}&camp_status=active")
                    time.sleep(30)
            print("[SCHEDULER] Todos criativos OK")
        except Exception as e:
            print(f"[SCHEDULER] Erro todos criativos: {e}")

    print(f"[SCHEDULER] Atualizacao completa! Tudo cacheado ate {datetime.now().strftime('%H:%M')}")


def _refresh_recent_loop():
    """Thread separada que revalida o cache dos ranges recentes ao longo do dia.

    Ciclo de 30min para garantir que 1d (TTL=36min) nunca expire entre refreshes.
    Ranges maiores (7d/14d/30d) so sao refreshados a cada 4 ciclos (~2h) pois
    seus TTLs sao maiores (3h/8h/20h).

    Historico do problema: TTL 1d=30min com ciclo 2h deixava o cache vazio por
    ~1h30 entre refreshes — usuario abria de manha e dados nao estavam cacheados."""
    now_br = _now_br
    # Primeira execucao 10s apos boot para popular o cache imediatamente
    time.sleep(10)
    iteration = 0
    while True:
        try:
            refresh_scheduler_lock("refresh_recent")
            dt_to = (now_br() - timedelta(days=1)).strftime("%Y-%m-%d")
            # 1d em todo ciclo; 7d/14d/30d a cada 4 ciclos (~2h)
            days_to_refresh = [1] if iteration % 4 != 0 else [1, 7, 14, 30]
            print(f"[REFRESH] Ciclo {iteration}: ranges {days_to_refresh}d — {datetime.now().strftime('%H:%M')}")
            with app.test_client() as client:
                with client.session_transaction() as sess:
                    sess["logged_in"] = True
                    sess["username"] = SUPER_ADMIN_EMAIL
                    sess["role"] = "super_admin"
                for days in days_to_refresh:
                    dt_from = (now_br() - timedelta(days=days)).strftime("%Y-%m-%d")
                    for ct in VALID_CAMP_TYPES:
                        try:
                            # Chaves de cache correspondentes (matching backend logic).
                            # Pula requisicao se cache ainda tem >40% do TTL original.
                            k_camp = f"campaigns_{ct}_all_{dt_from}_{dt_to}"
                            k_daily = f"daily_summary_{ct}_all_{dt_from}_{dt_to}"
                            k_creat = f"all_creatives_{ct}_active_{dt_from}_{dt_to}"
                            base = f"camp_type={ct}&date_from={dt_from}&date_to={dt_to}&force=true"

                            refreshed_any = False
                            if should_refresh(k_camp):
                                client.get(f"/api/dashboard/campaigns?{base}&camp_status=all")
                                refreshed_any = True; time.sleep(3)
                            if should_refresh(k_daily):
                                client.get(f"/api/dashboard/daily-summary?{base}&camp_status=all")
                                refreshed_any = True; time.sleep(3)
                            if should_refresh(k_creat):
                                client.get(f"/api/dashboard/all-creatives?{base}&camp_status=active")
                                refreshed_any = True; time.sleep(5)
                            if refreshed_any:
                                refresh_scheduler_lock("refresh_recent")
                        except Exception as e:
                            print(f"[REFRESH] Erro {days}d/{ct}: {e}")
            print(f"[REFRESH] Cache atualizado em {datetime.now().strftime('%H:%M')}")
        except Exception as e:
            print(f"[REFRESH] Erro no loop: {e}")
        iteration += 1
        # Aguarda 30min, com heartbeat a cada 5min para manter o lock
        for _ in range(6):  # 6 * 5min = 30min
            time.sleep(300)
            try: refresh_scheduler_lock("refresh_recent")
            except Exception: pass


# ── Bootstrap dos schedulers (roda tanto em `python` quanto em `gunicorn`) ───
# Em producao o gunicorn importa o modulo e nao executa o bloco __main__.
# O lock em SQLite garante que so um worker de fato rode o scheduler.
if try_acquire_scheduler_lock("daily_scheduler"):
    start_scheduler(_scheduled_refresh)
    print("[BOOT] Scheduler diario (2:00) iniciado neste worker")
else:
    print("[BOOT] Outro worker ja esta rodando o scheduler diario")

if try_acquire_scheduler_lock("refresh_recent"):
    threading.Thread(target=_refresh_recent_loop, daemon=True).start()
    print("[BOOT] Loop de refresh recente (1d/7d a cada 2h) iniciado neste worker")
else:
    print("[BOOT] Outro worker ja esta rodando o loop de refresh recente")


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
