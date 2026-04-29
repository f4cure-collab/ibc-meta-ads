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
_APIFY_MAX_ACTIVE = 500     # ads ATIVOS: tras tudo (limite alto)
_APIFY_MAX_WITH_PAUSED = 50  # incluindo pausados: limita pra controlar custo

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


def _is_logged_in():
    """Qualquer usuario autenticado. Pra endpoints read-only de
    concorrentes (ver lista, ver ads cacheados)."""
    return bool(session.get("logged_in"))


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


def _is_ads_library_url(url):
    """Detecta se a URL ja eh da Facebook Ads Library."""
    return bool(url) and "facebook.com/ads/library" in url.lower()


def _extract_page_id_from_url(url):
    """Se a URL ja for Ads Library com view_all_page_id, extrai direto
    sem precisar fazer GET no FB. Retorna page_id ou None."""
    if not url:
        return None
    try:
        from urllib.parse import parse_qs
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        for key in ("view_all_page_id", "viewAllPageId"):
            if key in qs and qs[key]:
                v = qs[key][0]
                if v.isdigit() and 5 < len(v) < 20:
                    return v
    except Exception:
        pass
    return None


def _extract_page_id(page_url):
    """Faz GET na pagina FB publica e tenta extrair o page_id numerico
    do HTML. Retorna None se nao achar (pagina privada, FB exigindo
    login, ou padrao mudou). Usado pra montar URL especifica da Ads
    Library com search_type=page&view_all_page_id=..."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        }
        r = requests.get(page_url, headers=headers, timeout=20, allow_redirects=True)
        if r.status_code != 200:
            return None
        text = r.text
        patterns = [
            r'"pageID":"(\d{6,20})"',
            r'"page_id":"(\d{6,20})"',
            r'fb:page_id"\s+content="(\d{6,20})"',
            r'"entity_id":"(\d{6,20})"',
            r'al:android:url"\s+content="fb://page/(\d{6,20})"',
            r'profile_id=(\d{6,20})',
            r'"identifier":"(\d{6,20})","__typename":"Page"',
        ]
        for pat in patterns:
            m = re.search(pat, text)
            if m:
                return m.group(1)
    except Exception as e:
        print(f"[CONCORRENTES] page_id extract falhou {page_url}: {e}")
    return None


def _build_ads_library_url(page_id, country="BR"):
    """Monta URL da Ads Library filtrando estritamente pela pagina."""
    return (
        "https://www.facebook.com/ads/library/"
        f"?active_status=all&ad_type=all&country={country}"
        f"&search_type=page&view_all_page_id={page_id}"
    )


# Palavras genericas que NAO devem ser usadas pra match (ja que
# muitas paginas nao-relacionadas as tem)
_FILTER_STOPWORDS = {
    "coaching", "treinamentos", "treinamento", "training", "academy",
    "official", "oficial", "the", "and", "para", "store", "shop",
    "br", "brasil", "mentor", "mentoria", "coach", "method", "metodo",
    "curso", "cursos", "online", "digital",
}


def _filter_ads_to_competitor(raw_ads, competitor_name):
    """Filtra resultados pra so manter ads cuja page_name bate com o nome
    do concorrente. Match e por substring OU por keywords significativas
    (palavras com 3+ chars excluindo termos genericos).

    Usar APENAS quando nao tem page_id confirmado — quando page_id existe,
    a URL ja filtra estritamente no Apify e este filtro vira ruido."""
    if not raw_ads:
        return [], 0
    if not competitor_name:
        return raw_ads, 0

    name_norm = re.sub(r"\s+", " ", competitor_name.lower().strip())
    keywords = [
        w for w in name_norm.split()
        if len(w) >= 3 and w not in _FILTER_STOPWORDS
    ]

    matching = []
    rejected = 0
    for a in raw_ads:
        if not isinstance(a, dict):
            continue
        ad_page_name = (
            a.get("page_name")
            or (a.get("snapshot") or {}).get("page_name")
            or ""
        )
        ad_page_name_norm = re.sub(r"\s+", " ", ad_page_name.lower().strip())

        is_match = False
        if not ad_page_name_norm:
            # Sem nome de pagina — mantem (nao da pra rejeitar com confianca)
            is_match = True
        elif name_norm in ad_page_name_norm or ad_page_name_norm in name_norm:
            is_match = True
        elif keywords and any(kw in ad_page_name_norm for kw in keywords):
            is_match = True

        if is_match:
            matching.append(a)
        else:
            rejected += 1
    return matching, rejected


def _run_apify_sync(page_url, include_paused=False):
    """Chama o ator Apify sincronamente e retorna a lista de items.
    Levanta excecao em caso de erro ou timeout.

    include_paused=False (padrao): so traz ads ATIVOS, ate 500 por
        concorrente. Custo mais previsivel — anuncios pausados
        antigos costumam ser ruido.
    include_paused=True: traz ativos + pausados, limitado a 50 (pra
        nao explodir o custo do Apify nem o tempo de scraping)."""
    token = _get_apify_token()
    if not token:
        raise RuntimeError("Token Apify nao configurado. Cole o token em /admin (secao 'Token Apify' na aba Sistema).")

    api_url = f"https://api.apify.com/v2/acts/{_APIFY_ACTOR}/run-sync-get-dataset-items"
    payload = {
        "urls": [{"url": page_url}],
        "count": _APIFY_MAX_WITH_PAUSED if include_paused else _APIFY_MAX_ACTIVE,
        "scrapePageAds.activeStatus": "all" if include_paused else "active",
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


def _has_placeholder(text):
    """Detecta placeholders dinamicos tipo {{product.name}}."""
    if not text:
        return False
    return "{{product." in text or "{{ product." in text


_VIDEO_KEY_HINTS = (
    "video_hd_url", "video_sd_url", "video_url", "videoUrl",
    "video_high_quality_url", "video_low_quality_url",
)
_THUMB_KEY_HINTS = ("video_preview_image_url", "video_thumbnail_url")


def _recursive_find_video(node, depth=0, max_depth=8):
    """Busca recursiva por URL de video em qualquer parte do JSON.
    Retorna (video_url, thumb_url). Util pra estruturas onde video
    nao esta em snap.videos nem em dynamic_versions."""
    if depth > max_depth or node is None:
        return None, None
    if isinstance(node, dict):
        # Tenta keys diretas primeiro
        for k in _VIDEO_KEY_HINTS:
            v = node.get(k)
            if isinstance(v, str) and v.startswith("http"):
                # Procura thumb no mesmo nivel
                thumb = None
                for tk in _THUMB_KEY_HINTS:
                    tv = node.get(tk)
                    if isinstance(tv, str) and tv.startswith("http"):
                        thumb = tv
                        break
                return v, thumb
        # Recursivo
        for k, v in node.items():
            # Pula thumbnails pra nao confundir com video
            kl = k.lower()
            if "thumb" in kl or "image" in kl or "icon" in kl or "profile" in kl:
                continue
            r = _recursive_find_video(v, depth + 1, max_depth)
            if r[0]:
                return r
    elif isinstance(node, list):
        for item in node:
            r = _recursive_find_video(item, depth + 1, max_depth)
            if r[0]:
                return r
    return None, None


def _extract_from_dynamic_versions(snap):
    """Procura em dynamic_versions por uma versao com conteudo REAL
    (sem placeholder). Anuncios DCO (Dynamic Creative Optimization) tem
    o template no snap principal e as variacoes renderizadas em
    dynamic_versions/cards. Retorna dict ou None."""
    if not isinstance(snap, dict):
        return None
    versions = (
        snap.get("dynamic_versions")
        or snap.get("dynamicVersions")
        or snap.get("body_versions")
        or snap.get("cards")  # alguns ads colocam variacoes em cards
        or []
    )
    for v in versions:
        if not isinstance(v, dict):
            continue
        # Texto do body
        v_body = ""
        v_body_obj = v.get("body")
        if isinstance(v_body_obj, dict):
            v_body = v_body_obj.get("text") or ""
        elif isinstance(v_body_obj, str):
            v_body = v_body_obj
        if not v_body:
            v_body = v.get("body_text") or v.get("text") or ""
        # Pula versoes que ainda tem placeholder
        if not v_body or _has_placeholder(v_body):
            continue
        out = {"body": v_body}
        v_title = v.get("title")
        if v_title and not _has_placeholder(v_title):
            out["title"] = v_title
        v_videos = v.get("videos") or []
        if v_videos and isinstance(v_videos[0], dict):
            out["video_url"] = (
                v_videos[0].get("video_hd_url")
                or v_videos[0].get("video_sd_url")
            )
            out["video_thumb"] = v_videos[0].get("video_preview_image_url")
        v_images = v.get("images") or []
        if v_images and isinstance(v_images[0], dict):
            out["image_url"] = (
                v_images[0].get("original_image_url")
                or v_images[0].get("resized_image_url")
                or v_images[0].get("watermarked_resized_image_url")
            )
        # Tambem tenta original_image_url / resized_image_url no proprio card
        if "image_url" not in out:
            img = v.get("original_image_url") or v.get("resized_image_url")
            if img:
                out["image_url"] = img
        v_link = v.get("link_url") or v.get("linkUrl")
        if v_link:
            out["link_url"] = v_link
        return out
    return None


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

    # Title do anuncio
    snap_title = snap.get("title") or ""

    # DCO (Dynamic Creative Optimization): se body principal tem placeholder,
    # mergulha em dynamic_versions/cards pra achar uma versao renderizada.
    main_has_placeholder = _has_placeholder(body_text) or _has_placeholder(snap_title)

    # Imagem (varios fallbacks)
    image_url = None
    if images and isinstance(images[0], dict):
        image_url = (
            images[0].get("original_image_url")
            or images[0].get("resized_image_url")
            or images[0].get("watermarked_resized_image_url")
        )
    if not image_url and cards:
        for card in cards:
            if isinstance(card, dict):
                image_url = (
                    card.get("original_image_url")
                    or card.get("resized_image_url")
                    or card.get("watermarked_resized_image_url")
                )
                if image_url:
                    break
    # Fallbacks pra anuncios dinamicos (DPA / catalogo)
    if not image_url:
        extra = snap.get("extra_images") or snap.get("extraImages") or []
        if extra and isinstance(extra[0], dict):
            image_url = (
                extra[0].get("original_image_url")
                or extra[0].get("resized_image_url")
            )
    if not image_url:
        dyn = snap.get("dynamic_versions") or snap.get("dynamicVersions") or []
        if dyn and isinstance(dyn[0], dict):
            image_url = dyn[0].get("image_url") or dyn[0].get("imageUrl")
    # Ultimo recurso: thumb da pagina
    if not image_url:
        image_url = snap.get("page_profile_picture_url") or snap.get("pageProfilePictureUrl")

    # Video (URL + thumb) — busca em multiplos lugares
    video_url = None
    video_thumb = None

    def _video_from_obj(o):
        if not isinstance(o, dict):
            return None, None
        u = (
            o.get("video_hd_url")
            or o.get("video_sd_url")
            or o.get("video_url")
            or o.get("videoUrl")
            or o.get("video_high_quality_url")
            or o.get("video_low_quality_url")
        )
        t = o.get("video_preview_image_url") or o.get("video_thumbnail_url")
        return u, t

    # 1) snap.videos (principal)
    if videos and isinstance(videos[0], dict):
        video_url, video_thumb = _video_from_obj(videos[0])

    # 2) snap.extra_videos
    if not video_url:
        extra_v = snap.get("extra_videos") or snap.get("extraVideos") or []
        if extra_v and isinstance(extra_v[0], dict):
            v_url, v_thumb = _video_from_obj(extra_v[0])
            video_url = video_url or v_url
            video_thumb = video_thumb or v_thumb

    # 3) cards (para carrosseis com video)
    if not video_url and cards:
        for card in cards:
            if isinstance(card, dict):
                cv = card.get("videos") or []
                if cv and isinstance(cv[0], dict):
                    v_url, v_thumb = _video_from_obj(cv[0])
                    if v_url:
                        video_url = v_url
                        video_thumb = video_thumb or v_thumb
                        break
                # video direto no card
                v_url, v_thumb = _video_from_obj(card)
                if v_url:
                    video_url = v_url
                    video_thumb = video_thumb or v_thumb
                    break

    # DCO override: se o body principal tem placeholder, busca em
    # dynamic_versions/cards uma versao renderizada e SOBRESCREVE os
    # campos. Isso resolve anuncios que apareciam como '{{product.name}}'
    # quando na verdade tem texto e video reais.
    dco_override = None
    if main_has_placeholder:
        dco_override = _extract_from_dynamic_versions(snap)
        if dco_override:
            if dco_override.get("body"):
                body_text = dco_override["body"]
            if dco_override.get("title"):
                snap_title = dco_override["title"]
            if dco_override.get("video_url") and not video_url:
                video_url = dco_override["video_url"]
            if dco_override.get("video_thumb") and not video_thumb:
                video_thumb = dco_override["video_thumb"]
            if dco_override.get("image_url") and not image_url:
                image_url = dco_override["image_url"]

    # Ultimo recurso: busca recursiva por URL de video em qualquer lugar
    # do snapshot. Pega o primeiro link http que seja .mp4 ou contenha
    # 'video' em chave video_*. Limita profundidade pra evitar loops.
    if not video_url:
        video_url, found_thumb = _recursive_find_video(snap)
        if video_url and not video_thumb:
            video_thumb = found_thumb

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

    # Tipo de midia — detecta DPA (anuncio dinamico de catalogo) primeiro
    display_format = (
        a.get("display_format")
        or a.get("displayFormat")
        or snap.get("display_format")
        or ""
    ).upper()
    # Apos o DCO override (acima), body_text e snap_title podem ja estar
    # com conteudo real. So marca DPA se AINDA tiver placeholder e nao tem
    # midia real — caso contrario o anuncio e tratado como video/imagem.
    is_dpa = (
        display_format == "DPA"
        or (
            display_format in ("DCO", "DYNAMIC")
            and not video_url and not image_url
        )
        or (
            (_has_placeholder(body_text) or _has_placeholder(snap_title))
            and not video_url and not image_url
        )
    )
    if is_dpa:
        media_type = "dpa"
    elif videos or video_url:
        media_type = "video"
    elif len(images) > 1 or len(cards) > 1:
        media_type = "carousel"
    elif images or cards or image_url:
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
        "title": (snap_title or "")[:200],
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
    if not _is_logged_in():
        return redirect("/login")
    return render_template(
        "competitors.html",
        username=session.get("username"),
        is_admin=_is_super_admin(),
    )


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
    if not _is_logged_in():
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
        return jsonify({"ok": False, "error": "URL precisa ser do facebook.com (pagina ou Ads Library)"}), 400

    comps = _load_competitors()
    # Pra cid: usa handle da URL se for pagina; senao, slug do nome.
    handle = _extract_page_handle(url)
    cid = _slugify(handle) if handle and not _is_ads_library_url(url) else _slugify(name)
    if any(c["id"] == cid for c in comps):
        return jsonify({"ok": False, "error": "Concorrente ja cadastrado com esse nome/handle"}), 400
    entry = {"id": cid, "name": name, "url": url}
    # Se URL ja tem view_all_page_id, salva direto pra economizar 1 fetch
    page_id_from_url = _extract_page_id_from_url(url)
    if page_id_from_url:
        entry["page_id"] = page_id_from_url
    comps.append(entry)
    _save_competitors(comps)
    return jsonify({"ok": True, "id": cid})


@competitors_bp.route("/api/competitors/edit", methods=["POST"])
def api_edit():
    """Edita URL e/ou nome de um concorrente. Limpa page_id cacheado e
    cache de ads pra forcar re-extracao no proximo refresh."""
    if not _is_super_admin():
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    body = request.get_json(silent=True) or {}
    cid = (body.get("id") or "").strip()
    new_url = (body.get("url") or "").strip()
    new_name = (body.get("name") or "").strip()
    if not cid or (not new_url and not new_name):
        return jsonify({"ok": False, "error": "id + url ou name obrigatorio"}), 400
    if new_url and "facebook.com" not in new_url:
        return jsonify({"ok": False, "error": "URL precisa ser do facebook.com (pagina ou Ads Library)"}), 400
    comps = _load_competitors()
    found = False
    for c in comps:
        if c.get("id") == cid:
            if new_url:
                c["url"] = new_url
                # Reseta page_id pra forcar re-extracao
                c.pop("page_id", None)
                # Se URL ja tem view_all_page_id, salva direto
                page_id_from_url = _extract_page_id_from_url(new_url)
                if page_id_from_url:
                    c["page_id"] = page_id_from_url
            if new_name:
                c["name"] = new_name
            found = True
            break
    if not found:
        return jsonify({"ok": False, "error": "Concorrente nao encontrado"}), 404
    _save_competitors(comps)
    # Limpa cache dos ads antigos
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
    include_paused = bool(body.get("include_paused"))
    comps = _load_competitors()
    target = next((c for c in comps if c.get("id") == cid), None)
    if not target:
        return jsonify({"ok": False, "error": "Concorrente nao encontrado"}), 404

    try:
        url = target["url"]
        page_id = (target.get("page_id") or "").strip()

        # Estrategia em 3 passos pra obter page_id:
        # 1. Cache (target.page_id)
        # 2. Extrai do query string se URL ja for Ads Library
        # 3. GET no HTML da pagina FB (mais lento, pode falhar)
        if not page_id:
            page_id = _extract_page_id_from_url(url) or ""
        if not page_id and not _is_ads_library_url(url):
            page_id = _extract_page_id(url) or ""

        if page_id and page_id != target.get("page_id"):
            # Salva pro proximo refresh nao precisar extrair de novo
            comps2 = _load_competitors()
            for c in comps2:
                if c.get("id") == cid:
                    c["page_id"] = page_id
            _save_competitors(comps2)

        # URL pro Apify: usa as-is se ja for Ads Library; senao monta com
        # view_all_page_id quando temos page_id; senao usa URL bruta.
        if _is_ads_library_url(url):
            scrape_url = url
        elif page_id:
            scrape_url = _build_ads_library_url(page_id)
        else:
            scrape_url = url

        raw_ads = _run_apify_sync(scrape_url, include_paused=include_paused)

        # Quando page_id foi extraido, a URL pro Apify ja contem
        # view_all_page_id (search_type=page) — Apify retorna apenas
        # ads dessa pagina. Confia no resultado, sem filtro.
        # Quando page_id nao foi extraido, usamos a URL da pagina como
        # fallback — Apify pode fazer keyword search e retornar ads de
        # outras paginas. Filtro por nome serve como rede de seguranca.
        if page_id:
            matching = raw_ads
            rejected = 0
        else:
            matching, rejected = _filter_ads_to_competitor(
                raw_ads, target.get("name")
            )

        normalized = []
        for a in matching:
            n = _normalize_ad(a)
            if n and n.get("id"):
                normalized.append(n)
        payload = {
            "ads": normalized,
            "refreshed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "raw_count": len(raw_ads),
            "rejected_count": rejected,
            "page_id": page_id,
            "included_paused": include_paused,
        }
        # TTL longo (30d) — refresh e SEMPRE manual via botao
        set_cached(f"competitors_ads_v1_{cid}", payload, ttl_hours=720)
        return jsonify({
            "ok": True,
            "count": len(normalized),
            "raw_count": len(raw_ads),
            "rejected": rejected,
            "page_id": page_id or None,
            "included_paused": include_paused,
            "refreshed_at": payload["refreshed_at"],
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)[:500]}), 500


@competitors_bp.route("/api/competitors/ads/<cid>")
def api_ads(cid):
    if not _is_logged_in():
        return jsonify({"ok": False, "error": "Acesso negado"}), 403
    cached = get_cached(f"competitors_ads_v1_{cid}")
    if not cached or not isinstance(cached, dict):
        return jsonify({"ok": True, "ads": [], "refreshed_at": None})
    return jsonify({
        "ok": True,
        "ads": cached.get("ads", []),
        "refreshed_at": cached.get("refreshed_at"),
    })
