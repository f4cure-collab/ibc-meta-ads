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


def _is_super_admin():
    return (
        session.get("logged_in")
        and session.get("username") == _SUPER_ADMIN_EMAIL
    )


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
    token = os.getenv("APIFY_TOKEN", "")
    if not token:
        raise RuntimeError("APIFY_TOKEN nao configurado nas variaveis de ambiente.")

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

    body_text = ""
    if isinstance(body, dict):
        body_text = body.get("text") or body.get("markup", {}).get("__html", "") or ""
    if not body_text and snap.get("caption"):
        body_text = snap.get("caption", "")

    image_url = None
    if images and isinstance(images[0], dict):
        image_url = (
            images[0].get("original_image_url")
            or images[0].get("resized_image_url")
            or images[0].get("watermarked_resized_image_url")
        )
    if not image_url and cards and isinstance(cards[0], dict):
        image_url = cards[0].get("original_image_url") or cards[0].get("resized_image_url")

    video_url = None
    video_thumb = None
    if videos and isinstance(videos[0], dict):
        video_url = videos[0].get("video_hd_url") or videos[0].get("video_sd_url")
        video_thumb = videos[0].get("video_preview_image_url")

    archive_id = a.get("ad_archive_id") or a.get("adArchiveID") or a.get("id")
    library_url = a.get("url") or (
        f"https://www.facebook.com/ads/library/?id={archive_id}" if archive_id else None
    )

    return {
        "id": archive_id,
        "page_name": a.get("page_name") or snap.get("page_name") or "",
        "is_active": a.get("is_active"),
        "start_date": a.get("start_date_string") or a.get("startDate"),
        "end_date": a.get("end_date_string") or a.get("endDate"),
        "platforms": a.get("publisher_platform") or a.get("publisherPlatform") or [],
        "body": (body_text or "")[:1500],
        "title": (snap.get("title") or "")[:200],
        "cta_text": snap.get("cta_text") or snap.get("ctaText"),
        "cta_type": snap.get("cta_type") or snap.get("ctaType"),
        "link_url": snap.get("link_url") or snap.get("linkUrl"),
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
