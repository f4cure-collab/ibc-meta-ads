"""Concorrentes — modulo isolado pra mapear anuncios da Facebook Ads Library
de paginas concorrentes via Apify.

Tudo que envolve essa feature mora aqui:
- Cliente Apify (run-sync)
- Cache via cache_manager (chaves competitors_ads_v1_*)
- Lista de concorrentes em competitors_list.json
- Endpoints super_admin only

Pra remover a feature: deletar competitors.py + templates/competitors.html +
competitors_list.json + tirar as 2 linhas de blueprint do dashboard_app.py.
"""

import os
import re
import json
import time
import threading
from urllib.parse import urlparse
from flask import Blueprint, jsonify, request, render_template, session, redirect

import requests
from cache_manager import get_cached, set_cached

competitors_bp = Blueprint("competitors", __name__)

_COMPETITORS_FILE = os.path.join(os.path.dirname(__file__), "competitors_list.json")
_LIST_LOCK = threading.Lock()

# Apify actor: configuravel via env var pra trocar sem deploy se preciso.
# curious_coder/facebook-ads-library-scraper e popular e estavel.
_APIFY_ACTOR = os.getenv("APIFY_ACTOR", "curious_coder~facebook-ads-library-scraper")
_APIFY_TIMEOUT_SEC = 180  # 3min — Apify roda em ate 1-2min normalmente
_APIFY_COUNTRY = "BR"
_APIFY_MAX_PER_RUN = 50  # limita ads por concorrente pra controlar custo

_SUPER_ADMIN_EMAIL = "f4cure@gmail.com"

# Token Apify guardado no banco local do servidor (cache_manager).
# Repositorio GitHub e publico — token NUNCA vai pro git, so existe
# no banco local SQLite. Usuario configura via formulario na pagina.
_APIFY_TOKEN_CACHE_KEY = "apify_token_v1"
_APIFY_TOKEN_TTL_HOURS = 24 * 365 * 10  # 10 anos efetivo (so muda quando user atualiza)


def _is_super_admin():
    return (
        session.get("logged_in")
        and session.get("username") == _SUPER_ADMIN_EMAIL
    )


def _get_apify_token():
    """Retorna token Apify. Prioridade: env var > banco local."""
    env_token = (os.getenv("APIFY_TOKEN") or "").strip()
    if env_token:
        return env_token
    cached = get_cached(_APIFY_TOKEN_CACHE_KEY)
    if isinstance(cached, dict):
        return (cached.get("token") or "").strip()
    if isinstance(cached, str):
        return cached.strip()
    return ""


def _save_apify_token(token):
    """Salva token no banco local (sobrevive deploys; nunca vai pro git)."""
    set_cached(
        _APIFY_TOKEN_CACHE_KEY,
        {"token": token, "saved_at": time.strftime("%Y-%m-%d %H:%M:%S")},
        ttl_hours=_APIFY_TOKEN_TTL_HOURS,
    )


def _token_source():
    """De onde vem o token atual: 'env' | 'db' | 'none'."""
    if (os.getenv("APIFY_TOKEN") or "").strip():
        return "env"
    if _get_apify_token():
        return "db"
    return "none"


def _slugify(s):
    s = (s or "").lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")[:40] or "concorrente"


def _load_competitors():
    if not os.path.exists(_COMPETITORS_FILE):
        return []
    try:
        with open(_COMPETITORS_FILE, "r", encoding="utf-8") as f:
            return json.load(f) or []
    except Exception:
        return []


def _save_competitors(comps):
    with _LIST_LOCK:
        with open(_COMPETITORS_FILE, "w", encoding="utf-8") as f:
            json.dump(comps, f, ensure_ascii=False, indent=2)


def _extract_page_handle(page_url):
    """Tira o handle/nome da pagina da URL: https://www.facebook.com/KairosTreinamentos
    -> KairosTreinamentos."""
    try:
        path = urlparse(page_url).path or ""
        parts = [p for p in path.split("/") if p]
        return parts[0] if parts else ""
    except Exception:
        return ""


def _run_apify_sync(page_url):
    """Chama o ator Apify sincronamente e retorna a lista de items.
    Levanta excecao em caso de erro ou timeout."""
    token = _get_apify_token()
    if not token:
        raise RuntimeError("Token Apify nao configurado. Cole o token na pagina /concorrentes (campo 'Token Apify').")

    api_url = f"https://api.apify.com/v2/acts/{_APIFY_ACTOR}/run-sync-get-dataset-items"
    payload = {
        "urls": [{"url": page_url}],
        "count": _APIFY_MAX_PER_RUN,
        "scrapePageAds.activeStatus": "all",
        "scrapePageAds.country": _APIFY_COUNTRY,
    }
    r = requests.post(
        api_url,
        params={"token": token, "timeout": _APIFY_TIMEOUT_SEC},
        json=payload,
        timeout=_APIFY_TIMEOUT_SEC + 10,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"Apify HTTP {r.status_code}: {r.text[:300]}")
    try:
        return r.json() or []
    except ValueError:
        raise RuntimeError("Apify retornou resposta nao-JSON")


def _normalize_ad(a):
    """Reduz o JSON pesado do Apify ao essencial pro frontend."""
    if not isinstance(a, dict):
        return None
    snap = a.get("snapshot") or {}
    body = snap.get("body") or {}
    images = snap.get("images") or []
    videos = snap.get("videos") or []
    cards = snap.get("cards") or []

    # Texto do anuncio (varios campos possiveis)
    body_text = ""
    if isinstance(body, dict):
        body_text = body.get("text") or ""
        if not body_text and isinstance(body.get("markup"), dict):
            body_text = body["markup"].get("__html", "") or ""
    if not body_text and snap.get("caption"):
        body_text = snap.get("caption", "")

    # Imagem (varios fallbacks)
    image_url = None
    if images and isinstance(images[0], dict):
        image_url = (
            images[0].get("original_image_url")
            or images[0].get("resized_image_url")
            or images[0].get("watermarked_resized_image_url")
        )
    if not image_url and cards and isinstance(cards[0], dict):
        image_url = cards[0].get("original_image_url") or cards[0].get("resized_image_url")

    # Video (URL + thumb)
    video_url = None
    video_thumb = None
    if videos and isinstance(videos[0], dict):
        video_url = (
            videos[0].get("video_hd_url")
            or videos[0].get("video_sd_url")
            or videos[0].get("video_url")
        )
        video_thumb = videos[0].get("video_preview_image_url")

    # Archive ID (varias casing variations)
    archive_id = (
        a.get("ad_archive_id")
        or a.get("adArchiveID")
        or a.get("adArchiveId")
        or a.get("id")
    )

    # Library URL: SEMPRE construir do archive_id (mais confiavel que a URL
    # que o Apify retorna). Inclui params pra abrir filtrando esse anuncio.
    library_url = (
        f"https://www.facebook.com/ads/library/?id={archive_id}"
        if archive_id
        else None
    )

    # Datas: tenta varios campos. Apify retorna timestamp Unix em alguns
    # casos, string formatada em outros.
    from datetime import datetime as _dt

    def _ts_to_str(ts):
        try:
            return _dt.fromtimestamp(float(ts)).strftime("%d/%m/%Y")
        except Exception:
            return None

    start_ts = a.get("start_date") or a.get("startDate")
    end_ts = a.get("end_date") or a.get("endDate")

    start_str = (
        a.get("start_date_string")
        or a.get("startDateString")
        or a.get("started_running_on_string")
        or (
            _ts_to_str(start_ts)
            if isinstance(start_ts, (int, float))
            else (start_ts if isinstance(start_ts, str) else None)
        )
    )
    end_str = (
        a.get("end_date_string")
        or a.get("endDateString")
        or (
            _ts_to_str(end_ts)
            if isinstance(end_ts, (int, float))
            else (end_ts if isinstance(end_ts, str) else None)
        )
    )

    # Dias ativos (calculado do timestamp se disponivel)
    days_active = None
    try:
        if isinstance(start_ts, (int, float)):
            start_dt = _dt.fromtimestamp(float(start_ts))
            if isinstance(end_ts, (int, float)):
                end_dt = _dt.fromtimestamp(float(end_ts))
            else:
                end_dt = _dt.now()
            days_active = max(0, (end_dt - start_dt).days)
    except Exception:
        pass

    # Tipo de midia
    if videos:
        media_type = "video"
    elif len(images) > 1 or len(cards) > 1:
        media_type = "carousel"
    elif images or cards:
        media_type = "image"
    else:
        media_type = "unknown"

    # CTA + link de destino
    cta_text = snap.get("cta_text") or snap.get("ctaText")
    cta_type = snap.get("cta_type") or snap.get("ctaType")
    link_url = (
        snap.get("link_url")
        or snap.get("linkUrl")
        or snap.get("link")
        or (cards[0].get("link_url") if cards and isinstance(cards[0], dict) else None)
    )
    link_caption = (
        snap.get("caption")
        or snap.get("link_description")
        or (cards[0].get("link_description") if cards and isinstance(cards[0], dict) else None)
    )

    return {
        "id": archive_id,
        "page_name": a.get("page_name") or snap.get("page_name") or "",
        "is_active": a.get("is_active"),
        "start_date": start_str,
        "end_date": end_str,
        "days_active": days_active,
        "platforms": a.get("publisher_platform") or a.get("publisherPlatform") or [],
        "media_type": media_type,
        "body": (body_text or "")[:1500],
        "title": (snap.get("title") or "")[:200],
        "cta_text": cta_text,
        "cta_type": cta_type,
        "link_url": link_url,
        "link_caption": link_caption,
        "image_url": image_url,
        "video_url": video_url,
        "video_thumb": video_thumb,
        "library_url": library_url,
    }


# ── Endpoints ────────────────────────────────────────────────────────


@competitors_bp.route("/concorrentes")
def page():
    if not _is_super_admin():
        return redirect("/login")
    return render_template("competitors.html", username=session.get("username"))


@competitors_bp.route("/api/competitors/token-status")
def api_token_status():
    """Retorna se ha token configurado e de onde vem (sem expor o valor)."""
    if not _is_super_admin():
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    return jsonify({
        "ok": True,
        "configured": bool(_get_apify_token()),
        "source": _token_source(),
    })


@competitors_bp.route("/api/competitors/set-token", methods=["POST"])
def api_set_token():
    """Salva o token Apify no banco local (server-only). Super_admin only."""
    if not _is_super_admin():
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    body = request.get_json(silent=True) or {}
    token = (body.get("token") or "").strip()
    if not token:
        return jsonify({"ok": False, "error": "Token vazio"}), 400
    if not token.startswith("apify_api_"):
        return jsonify({"ok": False, "error": "Token deve comecar com apify_api_"}), 400
    if len(token) < 30:
        return jsonify({"ok": False, "error": "Token muito curto — confere se copiou inteiro"}), 400
    _save_apify_token(token)
    return jsonify({"ok": True})


@competitors_bp.route("/api/competitors/list")
def api_list():
    if not _is_super_admin():
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    comps = _load_competitors()
    out = []
    for c in comps:
        cached = get_cached(f"competitors_ads_v1_{c['id']}")
        ads = (cached or {}).get("ads") if isinstance(cached, dict) else None
        out.append({
            "id": c["id"],
            "name": c.get("name", ""),
            "url": c.get("url", ""),
            "ads_count": len(ads) if ads else 0,
            "refreshed_at": (cached or {}).get("refreshed_at") if isinstance(cached, dict) else None,
        })
    return jsonify({"ok": True, "competitors": out})


@competitors_bp.route("/api/competitors/add", methods=["POST"])
def api_add():
    if not _is_super_admin():
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    body = request.get_json(silent=True) or {}
    name = (body.get("name") or "").strip()
    url = (body.get("url") or "").strip()
    if not name or not url:
        return jsonify({"ok": False, "error": "name e url obrigatorios"}), 400
    if "facebook.com" not in url:
        return jsonify({"ok": False, "error": "URL precisa ser do facebook.com"}), 400

    comps = _load_competitors()
    handle = _extract_page_handle(url) or _slugify(name)
    cid = _slugify(handle)
    if any(c["id"] == cid for c in comps):
        return jsonify({"ok": False, "error": "Concorrente ja cadastrado"}), 400
    comps.append({"id": cid, "name": name, "url": url})
    _save_competitors(comps)
    return jsonify({"ok": True, "id": cid})


@competitors_bp.route("/api/competitors/remove", methods=["POST"])
def api_remove():
    if not _is_super_admin():
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    body = request.get_json(silent=True) or {}
    cid = (body.get("id") or "").strip()
    comps = [c for c in _load_competitors() if c.get("id") != cid]
    _save_competitors(comps)
    # Limpa cache do removido (best-effort)
    try:
        from cache_manager import _get_db
        conn = _get_db()
        try:
            conn.execute("DELETE FROM api_cache WHERE cache_key = ?", (f"competitors_ads_v1_{cid}",))
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass
    return jsonify({"ok": True})


@competitors_bp.route("/api/competitors/refresh", methods=["POST"])
def api_refresh():
    if not _is_super_admin():
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    body = request.get_json(silent=True) or {}
    cid = (body.get("id") or "").strip()
    comps = _load_competitors()
    target = next((c for c in comps if c.get("id") == cid), None)
    if not target:
        return jsonify({"ok": False, "error": "Concorrente nao encontrado"}), 404

    try:
        raw_ads = _run_apify_sync(target["url"])
        normalized = []
        for a in raw_ads:
            n = _normalize_ad(a)
            if n and n.get("id"):
                normalized.append(n)
        payload = {
            "ads": normalized,
            "refreshed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "raw_count": len(raw_ads),
        }
        # TTL longo (30d) — refresh e SEMPRE manual via botao
        set_cached(f"competitors_ads_v1_{cid}", payload, ttl_hours=720)
        return jsonify({
            "ok": True,
            "count": len(normalized),
            "refreshed_at": payload["refreshed_at"],
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)[:500]}), 500


@competitors_bp.route("/api/competitors/ads/<cid>")
def api_ads(cid):
    if not _is_super_admin():
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    cached = get_cached(f"competitors_ads_v1_{cid}")
    if not cached or not isinstance(cached, dict):
        return jsonify({"ok": True, "ads": [], "refreshed_at": None})
    return jsonify({
        "ok": True,
        "ads": cached.get("ads", []),
        "refreshed_at": cached.get("refreshed_at"),
    })
