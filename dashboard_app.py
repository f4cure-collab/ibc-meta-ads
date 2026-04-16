"""
IBC Eventos - Dashboard de Performance Meta Ads
Servidor Flask na porta 5001 (separado do app.py principal).
"""

import os
import json
import math
import time
import functools
import requests
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from werkzeug.middleware.proxy_fix import ProxyFix
from dotenv import load_dotenv
from cache_manager import get_cached, set_cached, clear_cache, cache_stats, start_scheduler, clear_expired
from event_grouper import group_campaigns_by_event

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

SUPER_ADMIN_EMAIL = "f4cure@gmail.com"  # Admin principal — invisível e intocável
ADMIN_DEFAULT_PASS = os.getenv("ADMIN_PASSWORD", "ibc!facure@1010")
USERS_FILE = os.path.join(os.path.dirname(__file__), "users.json")


def _load_users():
    """Carrega usuarios do arquivo JSON."""
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE, "r") as f:
            users = json.load(f)
    else:
        users = {}
    # Super admin sempre existe e nunca é sobrescrito
    users[SUPER_ADMIN_EMAIL] = {
        "password": ADMIN_DEFAULT_PASS,
        "role": "super_admin",
        "must_reset": False
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
    """Exige login e role != viewer. Usado em endpoints que o perfil
    Visualizador nao deve poder acessar (campanhas, projecao, breakdowns)."""
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
        session["must_reset"] = user.get("must_reset", False)
        # Registra timestamp de ultimo acesso no users.json (se existir no arquivo).
        # O super admin e injetado em _load_users e nao persiste — nesse caso ignoramos.
        try:
            if os.path.exists(USERS_FILE):
                with open(USERS_FILE, "r") as f:
                    persisted = json.load(f)
                if username in persisted:
                    persisted[username]["last_login"] = datetime.now().isoformat(timespec="seconds")
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
    session.clear()
    return redirect(url_for("login_page"))


# ── Pages ──────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return redirect(url_for("login_page"))


@app.route("/dashboard")
@login_required
def dashboard():
    return render_template(
        "dashboard.html",
        username=session.get("username"),
        role=session.get("role", "viewer"),
    )


# ── Meta API helpers ───────────────────────────────────────────────────

# ── Rate Limit Protection ──────────────────────────────────────────────
_rate_usage = {"call_count": 0, "total_cputime": 0, "total_time": 0, "last_check": 0}
_MIN_DELAY = 1  # Minimo 1 segundo entre chamadas
_last_call_time = 0


def _enforce_rate_limit():
    """Garante delay minimo entre chamadas e pausa se proximo do limite."""
    global _last_call_time
    now = time.time()
    elapsed = now - _last_call_time
    if elapsed < _MIN_DELAY:
        time.sleep(_MIN_DELAY - elapsed)

    # Se uso > 75%, desacelerar
    if _rate_usage["call_count"] > 75:
        wait = 30
        print(f"[RATE LIMIT] Uso em {_rate_usage['call_count']}% — pausando {wait}s para seguranca")
        time.sleep(wait)
    elif _rate_usage["call_count"] > 50:
        print(f"[RATE LIMIT] Uso em {_rate_usage['call_count']}% — desacelerando")
        time.sleep(3)

    _last_call_time = time.time()


def _update_rate_from_headers(resp):
    """Atualiza info de rate limit dos headers da resposta."""
    import json as _json
    usage = resp.headers.get("x-business-use-case-usage", "")
    if usage:
        try:
            data = _json.loads(usage)
            for acct, usages in data.items():
                for u in usages:
                    _rate_usage["call_count"] = u.get("call_count", 0)
                    _rate_usage["total_cputime"] = u.get("total_cputime", 0)
                    _rate_usage["total_time"] = u.get("total_time", 0)
                    _rate_usage["last_check"] = time.time()
        except Exception:
            pass


def get_dashboard_rate_info():
    """Retorna info de rate limit para exibir no dashboard."""
    return dict(_rate_usage)


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


def extract_purchase_value(actions_or_values, field="value"):
    """Extrai valor de purchase evitando duplicação.

    Estratégia: pega o PRIMEIRO match da ordem PURCHASE_TYPES que tiver valor > 0.
    Se nenhum tiver valor > 0, retorna 0.
    """
    if not actions_or_values:
        return 0.0
    # Procurar na ordem de prioridade dos tipos
    for ptype in PURCHASE_TYPES:
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
        if item.get("action_type") in PURCHASE_TYPES:
            try:
                return float(item.get(field, 0) or 0)
            except Exception:
                return 0.0
    return 0.0


def extract_purchase_count(actions):
    """Extrai contagem de purchases evitando duplicação."""
    if not actions:
        return 0
    for ptype in PURCHASE_TYPES:
        for item in actions:
            if item.get("action_type") == ptype:
                try:
                    v = int(float(item.get("value", 0) or 0))
                except Exception:
                    v = 0
                if v > 0:
                    return v
    for item in actions:
        if item.get("action_type") in PURCHASE_TYPES:
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


def parse_insights(row):
    """Transforma uma linha de insights da API em dict limpo.

    IMPORTANTE:
    - Usa 'inline_link_clicks' como fonte principal de link_clicks (campo oficial da Meta).
      Fallback para 'actions[link_click]' se não estiver presente.
    - Usa 'purchase_roas' oficial quando disponível; fallback para revenue/spend.
    - Deduplica purchase types (evita contar 2x).
    """
    spend = _safe_float(row.get("spend"))
    impressions = _safe_int(row.get("impressions"))
    clicks = _safe_int(row.get("clicks"))
    reach = _safe_int(row.get("reach"))

    actions = row.get("actions", []) or []
    action_values = row.get("action_values", []) or []
    purchase_roas_list = row.get("purchase_roas", []) or []

    purchases = extract_purchase_count(actions)
    revenue = extract_purchase_value(action_values)

    # ROAS: usar purchase_roas oficial como fonte primária
    roas = 0
    if purchase_roas_list:
        for pr in purchase_roas_list:
            if pr.get("action_type") in PURCHASE_TYPES:
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
    """Retorna o filtro effective_status para a Meta API."""
    if camp_status == "paused":
        return '["PAUSED"]'
    elif camp_status == "all":
        return '["ACTIVE","PAUSED"]'
    return '["ACTIVE"]'


def _yesterday():
    """Retorna data de ontem — nunca usar dados do dia atual (incompletos)."""
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


def _default_date_from():
    return (datetime.now() - timedelta(days=31)).strftime("%Y-%m-%d")


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
    "actions,action_values,purchase_roas,date_start"
)


# ── API Endpoints ──────────────────────────────────────────────────────

@app.route("/api/dashboard/campaigns")
@not_viewer_required
def api_campaigns():
    """Lista campanhas de vendas com métricas agregadas."""
    try:
        date_from = request.args.get("date_from", _default_date_from())
        date_to = request.args.get("date_to", _yesterday())
        camp_status = request.args.get("camp_status", "active")
        force = request.args.get("force", "false") == "true"

        cache_key = f"campaigns_{camp_status}_{date_from}_{date_to}"
        if not force:
            cached = get_cached(cache_key)
            if cached:
                return jsonify(cached)

        # 1. Buscar campanhas de VENDAS
        campaigns = meta_get_all_pages(
            f"{ACCOUNT_ID}/campaigns",
            {
                "fields": "id,name,status,objective,daily_budget,lifetime_budget,start_time,created_time",
                "effective_status": _camp_status_filter(camp_status),
            }
        )
        sales_campaigns = [c for c in campaigns if c.get("objective") == "OUTCOME_SALES"]

        if not sales_campaigns:
            return jsonify({"ok": True, "data": [], "summary": {}})

        # 2. Buscar insights para cada campanha (só com impressões > 0)
        campaign_ids = [c["id"] for c in sales_campaigns]
        insights_params = {
            "fields": INSIGHT_FIELDS_CAMPAIGN,
            "time_range": json.dumps({"since": date_from, "until": date_to}),
            "level": "campaign",
            "filtering": json.dumps([
                {"field": "campaign.id", "operator": "IN", "value": campaign_ids},
                {"field": "impressions", "operator": "GREATER_THAN", "value": 0},
            ]),
            "limit": 500,
        }

        insights_data = meta_get_all_pages(
            f"{ACCOUNT_ID}/insights",
            insights_params
        )

        insights_map = {}
        for row in insights_data:
            insights_map[row.get("campaign_id")] = parse_insights(row)

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
        set_cached(cache_key, response, ttl_hours=24)
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
        ids_param = request.args.get("ids", "all")
        camp_status = request.args.get("camp_status", "all")
        force = request.args.get("force", "false") == "true"

        cache_key = f"multi_insights_{ids_param}_{camp_status}_{date_from}_{date_to}"
        if not force:
            cached = get_cached(cache_key)
            if cached:
                return jsonify(cached)

        if ids_param == "all":
            # Buscar campanhas de vendas
            all_camps = meta_get_all_pages(
                f"{ACCOUNT_ID}/campaigns",
                {"fields": "id,name,objective", "effective_status": _camp_status_filter(camp_status)}
            )
            sales_camps = [c for c in all_camps if c.get("objective") == "OUTCOME_SALES"]
            sales_map = {c["id"]: c for c in sales_camps}
            target_ids = list(sales_map.keys())
        else:
            # IDs específicos: aceitar direto sem validar status
            target_ids = [i.strip() for i in ids_param.split(",") if i.strip()]
            # Buscar nomes das campanhas
            all_camps = meta_get_all_pages(
                f"{ACCOUNT_ID}/campaigns",
                {"fields": "id,name,objective", "effective_status": '["ACTIVE","PAUSED"]'}
            )
            sales_map = {c["id"]: c for c in all_camps}

        if not target_ids:
            return jsonify({"ok": True, "campaigns": []})

        # 1 chamada batch usando filtering por campaign.id IN
        rows = meta_get_all_pages(
            f"{ACCOUNT_ID}/insights",
            {
                "fields": INSIGHT_FIELDS_DAILY_CAMP,
                "time_range": json.dumps({"since": date_from, "until": date_to}),
                "time_increment": 1,
                "level": "campaign",
                "filtering": json.dumps([
                    {"field": "campaign.id", "operator": "IN", "value": target_ids},
                    {"field": "impressions", "operator": "GREATER_THAN", "value": 0},
                ]),
                "limit": 500,
            }
        )

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
            by_camp[cid]["daily"].append(parsed)

        # Ordenar daily por data
        result = []
        for cid in target_ids:
            entry = by_camp.get(cid, {"id": cid, "name": sales_map.get(cid, {}).get("name", cid), "daily": []})
            entry["daily"].sort(key=lambda x: x["date"])
            result.append(entry)

        # Buscar reach/frequency agregados reais (1 chamada para todas as campanhas selecionadas)
        agg_totals = {"reach": 0, "frequency": 0}
        try:
            agg_rows = meta_get_all_pages(
                f"{ACCOUNT_ID}/insights",
                {
                    "fields": "reach,frequency,impressions",
                    "time_range": json.dumps({"since": date_from, "until": date_to}),
                    "level": "account",
                    "filtering": json.dumps([{"field": "campaign.id", "operator": "IN", "value": target_ids}]),
                }
            )
            if agg_rows:
                agg_totals["reach"] = int(agg_rows[0].get("reach", 0))
                agg_totals["frequency"] = round(float(agg_rows[0].get("frequency", 0)), 2)
        except Exception as e:
            print(f"[WARN] reach/frequency agregados falhou: {e}")

        response = {"ok": True, "campaigns": result, "aggregated": agg_totals}
        set_cached(cache_key, response, ttl_hours=24)
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
    """Insights diários agregados de TODAS as campanhas de vendas (somatório)."""
    try:
        date_from = request.args.get("date_from", _default_date_from())
        date_to = request.args.get("date_to", _yesterday())
        camp_status = request.args.get("camp_status", "active")
        force = request.args.get("force", "false") == "true"

        cache_key = f"daily_summary_{camp_status}_{date_from}_{date_to}"
        if not force:
            cached = get_cached(cache_key)
            if cached:
                return jsonify(cached)

        # 1. Buscar campanhas de vendas
        campaigns = meta_get_all_pages(
            f"{ACCOUNT_ID}/campaigns",
            {
                "fields": "id,name,objective",
                "effective_status": _camp_status_filter(camp_status),
            }
        )
        sales_campaigns = [c for c in campaigns if c.get("objective") == "OUTCOME_SALES"]

        if not sales_campaigns:
            return jsonify({"ok": True, "data": []})

        # 2. Buscar insights di&aacute;rios filtrados pelos IDs das campanhas de vendas
        camp_ids = [c["id"] for c in sales_campaigns]
        rows = meta_get_all_pages(
            f"{ACCOUNT_ID}/insights",
            {
                "fields": INSIGHT_FIELDS_DAILY,
                "time_range": json.dumps({"since": date_from, "until": date_to}),
                "time_increment": 1,
                "level": "campaign",
                "filtering": json.dumps([
                    {"field": "campaign.id", "operator": "IN", "value": camp_ids},
                    {"field": "impressions", "operator": "GREATER_THAN", "value": 0},
                ]),
                "limit": 500,
            }
        )

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
        set_cached(cache_key, response, ttl_hours=24)
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
        campaign_id = request.args.get("campaign_id", "")  # 1 ID ou vários separados por vírgula
        force = request.args.get("force", "false") == "true"

        cache_key = f"cumulative_reach_{campaign_id or 'all'}_{date_from}_{date_to}"
        if not force:
            cached = get_cached(cache_key)
            if cached:
                return jsonify(cached)

        # Suporta: 1 ID, múltiplos IDs (vírgula), ou vazio (todas de vendas)
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
            campaigns = meta_get_all_pages(
                f"{ACCOUNT_ID}/campaigns",
                {"fields": "id,objective", "effective_status": _camp_status_filter(camp_status)}
            )
            camp_ids = [c["id"] for c in campaigns if c.get("objective") == "OUTCOME_SALES"]
            if not camp_ids:
                return jsonify({"ok": True, "data": []})
            endpoint = f"{ACCOUNT_ID}/insights"
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
        set_cached(cache_key, response, ttl_hours=24)
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

        cache_key = f"ad_insights_{ad_id}_{date_from}_{date_to}"
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
        set_cached(cache_key, response, ttl_hours=24)
        return jsonify(response)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/dashboard/all-creatives")
@login_required
def api_all_creatives():
    """Busca criativos de TODAS as campanhas de vendas com métricas avançadas."""
    try:
        date_from = request.args.get("date_from", _default_date_from())
        date_to = request.args.get("date_to", _yesterday())
        camp_status = request.args.get("camp_status", "active")
        force = request.args.get("force", "false") == "true"

        cache_key = f"all_creatives_{camp_status}_{date_from}_{date_to}"
        if not force:
            cached = get_cached(cache_key)
            if cached:
                return jsonify(cached)

        campaigns = meta_get_all_pages(
            f"{ACCOUNT_ID}/campaigns",
            {
                "fields": "id,name,objective",
                "effective_status": _camp_status_filter(camp_status),
            }
        )
        sales_campaigns = [c for c in campaigns if c.get("objective") == "OUTCOME_SALES"]

        warnings = []
        result = _fetch_creatives_for_campaigns(sales_campaigns, date_from, date_to, warnings)
        response = {"ok": True, "data": result, "warnings": warnings}
        if not warnings:
            set_cached(cache_key, response, ttl_hours=24)
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
        campaign_id = request.args.get("campaign_id", "")

        cache_key = f"breakdowns_{campaign_id or 'all'}_{date_from}_{date_to}"
        cached = get_cached(cache_key)
        if cached:
            return jsonify(cached)

        # Determinar endpoint: campanha especifica ou conta toda (só vendas)
        if campaign_id:
            endpoint = campaign_id + "/insights"
        else:
            endpoint = ACCOUNT_ID + "/insights"

        base_params = {
            "time_range": json.dumps({"since": date_from, "until": date_to}),
            "filtering": json.dumps([{"field": "campaign.objective", "operator": "IN", "value": ["OUTCOME_SALES"]}]) if not campaign_id else None,
        }
        # Limpar None
        base_params = {k: v for k, v in base_params.items() if v is not None}

        ins_fields = "spend,impressions,clicks,actions,action_values,purchase_roas,website_purchase_roas"

        def extract_purchase(row):
            conv = 0
            revenue = 0
            roas = 0
            for a in (row.get("actions") or []):
                if a.get("action_type") in PURCHASE_TYPES:
                    conv = int(a.get("value", 0))
                    break
            for a in (row.get("action_values") or []):
                if a.get("action_type") in PURCHASE_TYPES:
                    revenue = float(a.get("value", 0))
                    break
            for a in (row.get("purchase_roas") or []):
                if a.get("action_type") in PURCHASE_TYPES:
                    roas = float(a.get("value", 0))
                    break
            if roas == 0:
                for a in (row.get("website_purchase_roas") or []):
                    if a.get("action_type") in PURCHASE_TYPES:
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
        set_cached(cache_key, response, ttl_hours=24)
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
        result = subprocess.run(["git", "fetch", "origin", "master"], capture_output=True, text=True, timeout=30,
                                cwd=os.path.dirname(__file__))
        local = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True,
                               cwd=os.path.dirname(__file__)).stdout.strip()
        remote = subprocess.run(["git", "rev-parse", "origin/master"], capture_output=True, text=True,
                                cwd=os.path.dirname(__file__)).stdout.strip()
        has_update = local != remote
        return jsonify({
            "ok": True,
            "has_update": has_update,
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
        cwd = os.path.dirname(__file__)
        subprocess.run(["git", "stash"], capture_output=True, text=True, cwd=cwd, timeout=15)
        pull = subprocess.run(["git", "pull", "origin", "master"], capture_output=True, text=True, cwd=cwd, timeout=60)
        pip = subprocess.run(["pip", "install", "-r", "requirements.txt"], capture_output=True, text=True, cwd=cwd, timeout=120)
        return jsonify({
            "ok": True,
            "message": "Atualizado! Reinicie o servico para aplicar.",
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
            last_applied = datetime.fromtimestamp(os.path.getmtime(fetch_head)).isoformat(timespec="seconds")

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
    result = []
    for email, u in users.items():
        # Super admin é invisível para admins secundários
        if u.get("role") == "super_admin" and not is_super:
            continue
        result.append({
            "email": email,
            "role": u.get("role", "viewer"),
            "must_reset": u.get("must_reset", False),
            "last_login": u.get("last_login", ""),
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
    users[email] = {"password": password, "role": role, "must_reset": True}
    _save_users(users)
    return jsonify({"ok": True})


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

    dt_to = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    # Ranges pre-carregados: (dias, label). 30d e o range principal usado no /criativos
    preload_ranges = [7, 14, 30, 60]
    ranges = [(d, (datetime.now() - timedelta(days=d)).strftime("%Y-%m-%d")) for d in preload_ranges]

    print(f"[SCHEDULER] Iniciando atualizacao automatica — ranges: {preload_ranges} dias, ate {dt_to}")
    clear_expired()

    with app.test_client() as client:
        # Simular login para acessar endpoints protegidos
        with client.session_transaction() as sess:
            sess["logged_in"] = True
            sess["username"] = SUPER_ADMIN_EMAIL
            sess["role"] = "super_admin"

        # ── ETAPA 1 (2:00): Campanhas (todos os ranges) ──
        try:
            print("[SCHEDULER] Etapa 1/5: Carregando campanhas (4 ranges)...")
            for days, dt_from in ranges:
                print(f"[SCHEDULER]   Campanhas {days}d ({dt_from} a {dt_to})")
                client.get(f"/api/dashboard/campaigns?date_from={dt_from}&date_to={dt_to}&camp_status=active&force=true")
                time.sleep(5)
            print("[SCHEDULER] Campanhas OK")
        except Exception as e:
            print(f"[SCHEDULER] Erro campanhas: {e}")

        # Pausa de 30 min entre etapas
        print("[SCHEDULER] Aguardando 30 min antes da proxima etapa...")
        time.sleep(1800)

        # ── ETAPA 2 (2:30): Resumo diario (todos os ranges) ──
        try:
            print("[SCHEDULER] Etapa 2/5: Carregando resumo diario (4 ranges)...")
            for days, dt_from in ranges:
                print(f"[SCHEDULER]   Resumo diario {days}d")
                client.get(f"/api/dashboard/daily-summary?date_from={dt_from}&date_to={dt_to}&camp_status=active")
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
            dt_from_30 = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            sales_camps = []
            try:
                data = meta_get(f"{ACCOUNT_ID}/campaigns", {
                    "fields": "id,name,objective",
                    "effective_status": '["ACTIVE"]',
                    "limit": "200"
                })
                sales_camps = [c for c in data.get("data", []) if c.get("objective") in PURCHASE_TYPES or c.get("objective") == "OUTCOME_SALES"]
            except Exception as e:
                print(f"[SCHEDULER] Erro ao buscar campanhas: {e}")

            for i, camp in enumerate(sales_camps):
                try:
                    print(f"[SCHEDULER]   Criativos campanha {i+1}/{len(sales_camps)}: {camp.get('name', camp['id'])}")
                    client.get(f"/api/dashboard/campaigns/{camp['id']}/creatives?date_from={dt_from_30}&date_to={dt_to}")
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

        # ── ETAPA 4 (3:30): Breakdowns demograficos (todos os ranges) ──
        try:
            print("[SCHEDULER] Etapa 4/5: Carregando breakdowns (4 ranges)...")
            for days, dt_from in ranges:
                print(f"[SCHEDULER]   Breakdowns {days}d")
                client.get(f"/api/dashboard/breakdowns?date_from={dt_from}&date_to={dt_to}")
                time.sleep(10)
            print("[SCHEDULER] Breakdowns OK")
        except Exception as e:
            print(f"[SCHEDULER] Erro breakdowns: {e}")

        print("[SCHEDULER] Aguardando 30 min...")
        time.sleep(1800)

        # ── ETAPA 5 (4:00): Todos criativos consolidado (todos os ranges) ──
        try:
            print("[SCHEDULER] Etapa 5/5: Carregando todos criativos consolidados (4 ranges)...")
            for days, dt_from in ranges:
                print(f"[SCHEDULER]   All-creatives {days}d")
                client.get(f"/api/dashboard/all-creatives?date_from={dt_from}&date_to={dt_to}&camp_status=active")
                time.sleep(30)
            print("[SCHEDULER] Todos criativos OK")
        except Exception as e:
            print(f"[SCHEDULER] Erro todos criativos: {e}")

    print(f"[SCHEDULER] Atualizacao completa! Tudo cacheado ate {datetime.now().strftime('%H:%M')}")


# ── Run ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  IBC Dashboard de Performance - http://localhost:5001")
    print(f"  Login: {SUPER_ADMIN_EMAIL}")
    print("=" * 60)

    # Iniciar scheduler para atualização automática às 2h
    start_scheduler(_scheduled_refresh)
    print("  Scheduler ativo: atualizacao automatica as 2:00")
    print("  Cache TTL: 20 horas")
    print("=" * 60)

    app.run(host="0.0.0.0", port=5001, debug=True)
