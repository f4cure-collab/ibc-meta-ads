"""
Módulo de integração com a Meta Marketing API.
Gerencia upload de vídeos, leitura de anúncios e duplicação de criativos.
"""

import os
import time
import requests
import json
from dotenv import load_dotenv

load_dotenv()

API_VERSION = "v21.0"
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"

# ── Rate Limiting ──────────────────────────────────────────────────
MAX_DUPLICATIONS_PER_BATCH = 10  # Máximo de duplicações por vez
API_DELAY_SECONDS = 2  # Delay entre chamadas de escrita (create/update)
_rate_limit_info = {"call_count": 0, "total_cputime": 0, "total_time": 0, "tier": "unknown"}


def get_rate_limit_info():
    """Retorna info atual do rate limit."""
    return dict(_rate_limit_info)


def _update_rate_limit(resp):
    """Atualiza info de rate limit a partir dos headers da resposta."""
    usage = resp.headers.get("x-business-use-case-usage", "")
    if usage:
        try:
            data = json.loads(usage)
            for account_id, usages in data.items():
                for u in usages:
                    _rate_limit_info["call_count"] = u.get("call_count", 0)
                    _rate_limit_info["total_cputime"] = u.get("total_cputime", 0)
                    _rate_limit_info["total_time"] = u.get("total_time", 0)
                    _rate_limit_info["tier"] = u.get("ads_api_access_tier", "unknown")
        except (json.JSONDecodeError, KeyError):
            pass

    # Se próximo do limite (>80%), esperar antes de continuar
    if _rate_limit_info["call_count"] > 80:
        wait = max(60, _rate_limit_info.get("estimated_time_to_regain_access", 60))
        print(f"[RATE LIMIT] Uso em {_rate_limit_info['call_count']}%, aguardando {wait}s...")
        time.sleep(wait)
    elif _rate_limit_info["call_count"] > 50:
        print(f"[RATE LIMIT] Uso em {_rate_limit_info['call_count']}%, desacelerando...")
        time.sleep(5)


def _check_response(resp):
    """Verifica resposta da API e levanta erro com mensagem legível."""
    _update_rate_limit(resp)

    if not resp.text:
        raise Exception(f"Meta API retornou resposta vazia (HTTP {resp.status_code})")
    try:
        data = resp.json()
    except Exception:
        raise Exception(f"Meta API retornou resposta inválida: {resp.text[:200]}")
    if "error" in data:
        err = data["error"]
        msg = err.get("message", "")
        detail = err.get("error_user_msg", "")
        title = err.get("error_user_title", "")
        full = f"Meta API: {msg}"
        if detail:
            full += f" | {detail}"
        if title:
            full += f" ({title})"
        print(f"[META API ERROR] {full}")
        print(f"[META API ERROR] Full response: {data}")
        raise Exception(full)
    return data


def get_token():
    return os.getenv("META_ACCESS_TOKEN", "")


def get_ad_account_id():
    return os.getenv("META_AD_ACCOUNT_ID", "")


# ── Campanhas e Adsets ──────────────────────────────────────────────

def list_campaigns():
    """Lista campanhas ativas da conta."""
    url = f"{BASE_URL}/{get_ad_account_id()}/campaigns"
    params = {
        "access_token": get_token(),
        "fields": "id,name,status,objective",
        "effective_status": '["ACTIVE"]',
        "limit": 500,
    }
    resp = requests.get(url, params=params)
    data = resp.json()
    if "error" in data:
        raise Exception(data["error"].get("message", str(data["error"])))
    return data.get("data", [])


def list_adsets(campaign_id):
    """Lista adsets ativos de uma campanha."""
    url = f"{BASE_URL}/{campaign_id}/adsets"
    params = {
        "access_token": get_token(),
        "fields": "id,name,status",
        "effective_status": '["ACTIVE"]',
        "limit": 100,
    }
    resp = requests.get(url, params=params)
    data = _check_response(resp)
    return data.get("data", [])


def list_ads(adset_id):
    """Lista anúncios ativos de um adset."""
    url = f"{BASE_URL}/{adset_id}/ads"
    params = {
        "access_token": get_token(),
        "fields": "id,name,status,creative{id,name}",
        "effective_status": '["ACTIVE"]',
        "limit": 100,
    }
    resp = requests.get(url, params=params)
    data = _check_response(resp)
    return data.get("data", [])


# ── Leitura de Criativo ─────────────────────────────────────────────

def get_creative_details(creative_id):
    """Lê todos os campos relevantes de um criativo (template)."""
    url = f"{BASE_URL}/{creative_id}"
    params = {
        "access_token": get_token(),
        "fields": (
            "id,name,body,title,link_url,call_to_action_type,"
            "object_story_spec,asset_feed_spec,url_tags,"
            "image_hash,image_url,video_id,"
            "object_story_id,effective_object_story_id"
        ),
    }
    resp = requests.get(url, params=params)
    return _check_response(resp)


# ── Upload de Vídeo ─────────────────────────────────────────────────

def upload_video(video_path, title=None):
    """Faz upload de um vídeo para a conta de anúncios. Retorna o video_id."""
    file_size = os.path.getsize(video_path)

    # Vídeos > 50MB usam upload resumable
    if file_size > 50 * 1024 * 1024:
        return _upload_video_resumable(video_path, title)

    url = f"{BASE_URL}/{get_ad_account_id()}/advideos"
    data = {"access_token": get_token()}
    if title:
        data["title"] = title

    with open(video_path, "rb") as f:
        files = {"source": (os.path.basename(video_path), f, "video/mp4")}
        resp = requests.post(url, data=data, files=files, timeout=600)

    result = _check_response(resp)
    video_id = result.get("id")

    _wait_video_ready(video_id)
    return video_id


def _upload_video_resumable(video_path, title=None):
    """Upload resumable para vídeos grandes (>50MB)."""
    file_size = os.path.getsize(video_path)
    print(f"[DEBUG] Upload resumable: {video_path} ({file_size / 1024 / 1024:.1f} MB)")

    # 1. Iniciar sessão de upload
    url = f"{BASE_URL}/{get_ad_account_id()}/advideos"
    start_data = {
        "access_token": get_token(),
        "upload_phase": "start",
        "file_size": str(file_size),
    }
    resp = requests.post(url, data=start_data, timeout=60)
    start_result = _check_response(resp)

    upload_session_id = start_result["upload_session_id"]
    video_id = start_result.get("video_id")

    print(f"[DEBUG] Sessão de upload: {upload_session_id}, video_id: {video_id}")

    # 2. Enviar chunks
    chunk_size = 20 * 1024 * 1024  # 20MB por chunk
    offset = 0

    with open(video_path, "rb") as f:
        while offset < file_size:
            chunk = f.read(chunk_size)
            transfer_data = {
                "access_token": get_token(),
                "upload_phase": "transfer",
                "upload_session_id": upload_session_id,
                "start_offset": str(offset),
            }
            files = {"video_file_chunk": ("chunk", chunk, "application/octet-stream")}
            resp = requests.post(url, data=transfer_data, files=files, timeout=300)
            result = _check_response(resp)

            offset = int(result.get("start_offset", file_size))
            print(f"[DEBUG] Upload chunk: {offset}/{file_size} ({offset * 100 // file_size}%)")

    # 3. Finalizar upload
    finish_data = {
        "access_token": get_token(),
        "upload_phase": "finish",
        "upload_session_id": upload_session_id,
    }
    if title:
        finish_data["title"] = title

    resp = requests.post(url, data=finish_data, timeout=60)
    finish_result = _check_response(resp)

    final_video_id = finish_result.get("id") or video_id
    print(f"[DEBUG] Upload finalizado: {final_video_id}")

    _wait_video_ready(final_video_id)
    return final_video_id


def upload_video_by_url(video_url, title=None):
    """Faz upload de vídeo via URL pública."""
    url = f"{BASE_URL}/{get_ad_account_id()}/advideos"
    data = {
        "access_token": get_token(),
        "file_url": video_url,
    }
    if title:
        data["title"] = title

    resp = requests.post(url, data=data)
    result = _check_response(resp)
    video_id = result.get("id")

    _wait_video_ready(video_id)
    return video_id


def _wait_video_ready(video_id, timeout=300):
    """Aguarda o vídeo ser processado pela Meta (até 5 min)."""
    url = f"{BASE_URL}/{video_id}"
    params = {"access_token": get_token(), "fields": "status"}
    start = time.time()

    while time.time() - start < timeout:
        resp = requests.get(url, params=params)
        result = _check_response(resp)
        status = result.get("status", {})
        video_status = status.get("video_status", "")

        if video_status == "ready":
            return True
        if video_status == "error":
            raise Exception(f"Erro no processamento do vídeo {video_id}: {status}")

        time.sleep(5)

    raise TimeoutError(f"Vídeo {video_id} não ficou pronto em {timeout}s")


# ── Upload de Imagem ────────────────────────────────────────────────

def upload_image(image_path):
    """Faz upload de uma imagem para a conta de anúncios. Retorna o image_hash."""
    url = f"{BASE_URL}/{get_ad_account_id()}/adimages"
    data = {"access_token": get_token()}

    with open(image_path, "rb") as f:
        files = {"filename": (os.path.basename(image_path), f)}
        resp = requests.post(url, data=data, files=files, timeout=600)

    result = _check_response(resp)
    images = result.get("images", {})
    for key, val in images.items():
        return val.get("hash")
    raise Exception("Falha ao obter hash da imagem enviada")


def upload_image_by_url(image_url):
    """Faz upload de imagem via URL pública. Baixa e envia."""
    resp = requests.get(image_url)
    resp.raise_for_status()

    tmp_path = os.path.join(os.path.dirname(__file__), "uploads", "_tmp_img")
    os.makedirs(os.path.dirname(tmp_path), exist_ok=True)
    with open(tmp_path, "wb") as f:
        f.write(resp.content)
    try:
        return upload_image(tmp_path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


# ── Duplicação de Criativo ──────────────────────────────────────────

import json as _json
import copy


def duplicate_ad_with_new_media(
    ad_id, feed_media_id, feed_media_type="video",
    reels_media_id=None, reels_media_type=None, new_name=None
):
    """
    Duplica um anúncio criando um novo ad do zero no mesmo adset,
    com um novo criativo que troca apenas a mídia.
    Inclui delay entre operações para respeitar rate limits.
    """
    # Delay entre operações de escrita
    time.sleep(API_DELAY_SECONDS)

    # 1. Ler dados do anúncio original
    ad_url = f"{BASE_URL}/{ad_id}"
    ad_params = {
        "access_token": get_token(),
        "fields": "name,adset_id,status,creative{id}",
    }
    ad_resp = requests.get(ad_url, params=ad_params)
    ad_data = _check_response(ad_resp)

    adset_id = ad_data["adset_id"]
    original_creative_id = ad_data["creative"]["id"]

    print(f"[DEBUG] Ad original: {ad_id}, adset: {adset_id}, creative: {original_creative_id}")

    # 2. Criar novo criativo com mídia trocada
    new_creative_id = _copy_creative_with_new_media(
        original_creative_id, feed_media_id, feed_media_type,
        reels_media_id, reels_media_type, new_name
    )

    print(f"[DEBUG] Novo criativo criado: {new_creative_id}")

    # 3. Criar novo ad no mesmo adset
    ad_name = new_name or f"{ad_data.get('name', 'Ad')} - Novo"
    new_ad = _create_ad(adset_id, new_creative_id, ad_name)

    print(f"[DEBUG] Novo ad criado: {new_ad.get('id')}")
    return new_ad


def duplicate_ad_with_new_video(ad_id, video_id_feed, video_id_reels=None, new_name=None):
    return duplicate_ad_with_new_media(
        ad_id, video_id_feed, "video",
        video_id_reels, "video" if video_id_reels else None,
        new_name
    )


def _copy_creative_with_new_media(
    source_creative_id, feed_media_id, feed_media_type,
    reels_media_id=None, reels_media_type=None, name=None
):
    """
    Lê o criativo original, copia TODOS os campos (asset_feed_spec,
    object_story_spec, url_tags) e troca os vídeos/imagens.
    Mapeia feed (_Q) e reels (_V) nos placements corretos.
    """
    original = get_creative_details(source_creative_id)
    oss = copy.deepcopy(original.get("object_story_spec", {}))
    afs = copy.deepcopy(original.get("asset_feed_spec"))

    print(f"[DEBUG] Criativo {source_creative_id}: has_oss={bool(oss)}, has_afs={bool(afs)}")

    url = f"{BASE_URL}/{get_ad_account_id()}/adcreatives"
    payload = {
        "access_token": get_token(),
        "name": name or "Novo Criativo",
    }

    if afs:
        # ── Criativo com asset_feed_spec (múltiplos placements) ──
        # Identificar reels labels via customization_rules (priority 1 = reels geralmente)
        reels_labels = set()
        media_label_key = None  # "video_label" ou "image_label"
        if "asset_customization_rules" in afs:
            for rule in afs["asset_customization_rules"]:
                spec = rule.get("customization_spec", {})
                positions = (
                    spec.get("facebook_positions", []) +
                    spec.get("instagram_positions", [])
                )
                is_reels_rule = any(
                    p in ["reels", "story", "ig_search", "profile_reels",
                           "facebook_reels", "facebook_reels_overlay"]
                    for p in positions
                )
                # Detectar qual key de mídia é usada nas rules
                for key in ["video_label", "image_label"]:
                    if key in rule:
                        media_label_key = key
                        if is_reels_rule:
                            reels_labels.add(rule[key]["name"])

        # Detectar mídia existente no template (videos ou images)
        has_videos = "videos" in afs and len(afs.get("videos", [])) > 0
        has_images = "images" in afs and len(afs.get("images", [])) > 0
        all_new_are_video = feed_media_type == "video"
        all_new_are_image = feed_media_type == "image" and (not reels_media_id or reels_media_type == "image")

        print(f"[DEBUG] Reels labels: {reels_labels}, media_label_key: {media_label_key}")
        print(f"[DEBUG] Template has_videos={has_videos}, has_images={has_images}, new_are_video={all_new_are_video}")

        # Processar mídia existente (vídeos ou imagens do template)
        source_entries = afs.get("videos", []) or afs.get("images", [])

        if all_new_are_video:
            # ── Novos são vídeos ──
            new_videos = []
            for entry in source_entries:
                labels = [l.get("name", "") for l in entry.get("adlabels", [])]
                is_reels = any(l in reels_labels for l in labels)

                vid_entry = {"video_id": reels_media_id if (is_reels and reels_media_id) else feed_media_id}
                if "adlabels" in entry:
                    vid_entry["adlabels"] = entry["adlabels"]
                new_videos.append(vid_entry)

            afs.pop("videos", None)
            afs.pop("images", None)
            afs["videos"] = new_videos

            # Converter image_label -> video_label nas rules
            if "asset_customization_rules" in afs:
                for rule in afs["asset_customization_rules"]:
                    if "image_label" in rule:
                        rule["video_label"] = rule.pop("image_label")

        elif all_new_are_image:
            # ── Novos são imagens ──
            new_images = []
            for entry in source_entries:
                labels = [l.get("name", "") for l in entry.get("adlabels", [])]
                is_reels = any(l in reels_labels for l in labels)

                img_entry = {"hash": reels_media_id if (is_reels and reels_media_id) else feed_media_id}
                if "adlabels" in entry:
                    img_entry["adlabels"] = entry["adlabels"]
                new_images.append(img_entry)

            afs.pop("videos", None)
            afs.pop("images", None)
            afs["images"] = new_images

            # Converter video_label -> image_label nas rules
            if "asset_customization_rules" in afs:
                for rule in afs["asset_customization_rules"]:
                    if "video_label" in rule:
                        rule["image_label"] = rule.pop("video_label")

        payload["asset_feed_spec"] = _json.dumps(afs)
        payload["object_story_spec"] = _json.dumps(oss)

    elif "video_data" in oss:
        # ── Criativo simples com video_data ──
        if media_type == "video":
            oss["video_data"]["video_id"] = media_id
            oss["video_data"].pop("image_url", None)
            oss["video_data"].pop("image_hash", None)
        else:
            vd = oss.pop("video_data")
            oss["link_data"] = {
                "link": vd.get("call_to_action", {}).get("value", {}).get("link", ""),
                "image_hash": media_id,
                "message": vd.get("message", ""),
            }
            if vd.get("call_to_action"):
                oss["link_data"]["call_to_action"] = vd["call_to_action"]
        payload["object_story_spec"] = _json.dumps(oss)

    elif "link_data" in oss:
        # ── Criativo simples com link_data ──
        if media_type == "video":
            oss["link_data"]["video_id"] = media_id
            oss["link_data"].pop("image_hash", None)
            oss["link_data"].pop("image_url", None)
        else:
            oss["link_data"]["image_hash"] = media_id
            oss["link_data"].pop("video_id", None)
            oss["link_data"].pop("image_url", None)
        payload["object_story_spec"] = _json.dumps(oss)

    # Copiar url_tags
    if original.get("url_tags"):
        payload["url_tags"] = original["url_tags"]

    print(f"[DEBUG] Criando criativo com keys: {[k for k in payload if k != 'access_token']}")

    resp = requests.post(url, data=payload)
    return _check_response(resp)["id"]


def _create_ad(adset_id, creative_id, name):
    """Cria um novo anúncio dentro de um adset."""
    url = f"{BASE_URL}/{get_ad_account_id()}/ads"
    data = {
        "access_token": get_token(),
        "name": name,
        "adset_id": adset_id,
        "creative": _json.dumps({"creative_id": creative_id}),
        "status": "PAUSED",
    }
    resp = requests.post(url, data=data)
    return _check_response(resp)


# ── Duplicação em Lote ──────────────────────────────────────────────

def batch_duplicate_from_list(ad_id, video_list):
    """Duplica um anúncio para cada par de vídeos da lista (CSV)."""
    results = []

    for i, video_info in enumerate(video_list):
        try:
            name = video_info.get("name", f"Variação {i + 1}")

            feed_source = video_info["feed"]
            if feed_source.startswith(("http://", "https://")):
                vid_feed = upload_video_by_url(feed_source, title=f"{name} - Feed")
            else:
                vid_feed = upload_video(feed_source, title=f"{name} - Feed")

            vid_reels = None
            reels_source = video_info.get("reels")
            if reels_source:
                if reels_source.startswith(("http://", "https://")):
                    vid_reels = upload_video_by_url(reels_source, title=f"{name} - Reels")
                else:
                    vid_reels = upload_video(reels_source, title=f"{name} - Reels")

            new_ad = duplicate_ad_with_new_video(ad_id, vid_feed, vid_reels, name)

            results.append({
                "name": name, "status": "success",
                "ad_id": new_ad.get("id"),
            })
        except Exception as e:
            results.append({
                "name": video_info.get("name", f"Variação {i + 1}"),
                "status": "error", "error": str(e),
            })

    return results
