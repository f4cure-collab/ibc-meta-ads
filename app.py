"""
Meta Ads Creative Tester - Dashboard Web
Duplica anúncios trocando apenas o vídeo, mantendo todas as configurações.
"""

import os
import csv
import io
import json
import zipfile
import unicodedata
import re
from flask import Flask, render_template, request, jsonify
from dotenv import load_dotenv
from meta_api import (
    list_campaigns,
    list_adsets,
    list_ads,
    get_creative_details,
    upload_video,
    upload_video_by_url,
    upload_image,
    upload_image_by_url,
    duplicate_ad_with_new_media,
    batch_duplicate_from_list,
    get_rate_limit_info,
    MAX_DUPLICATIONS_PER_BATCH,
)

load_dotenv()

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 500 * 1024 * 1024  # 500MB max upload

UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)


@app.route("/")
def index():
    return render_template("index.html")


# ── API: Listar estrutura da conta ──────────────────────────────────

@app.route("/api/campaigns")
def api_campaigns():
    try:
        campaigns = list_campaigns()
        return jsonify({"ok": True, "data": campaigns})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/adsets/<campaign_id>")
def api_adsets(campaign_id):
    try:
        adsets = list_adsets(campaign_id)
        return jsonify({"ok": True, "data": adsets})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/ads/<adset_id>")
def api_ads(adset_id):
    try:
        ads = list_ads(adset_id)
        return jsonify({"ok": True, "data": ads})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/creative/<creative_id>")
def api_creative(creative_id):
    try:
        creative = get_creative_details(creative_id)
        return jsonify({"ok": True, "data": creative})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


# ── API: Extrair ZIP ────────────────────────────────────────────────

def _sanitize_name(s):
    """Remove acentos, troca espaços por _ e remove caracteres especiais."""
    s = unicodedata.normalize('NFD', s)
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    s = s.replace(' ', '_')
    s = re.sub(r'[^a-zA-Z0-9_\-]', '', s)
    s = re.sub(r'_+', '_', s)
    return s


def _get_media_type(filename):
    ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
    return 'video' if ext in ('mp4', 'mov', 'avi', 'webm', 'mkv') else 'image'


@app.route("/api/extract-zip", methods=["POST"])
def api_extract_zip():
    """Extrai um ZIP com pastas de criativos e retorna os pares encontrados."""
    try:
        zip_file = request.files.get("zip_file")
        if not zip_file:
            return jsonify({"ok": False, "error": "Arquivo ZIP é obrigatório"}), 400

        # Salvar ZIP temporariamente
        zip_path = os.path.join(UPLOAD_DIR, "temp_upload.zip")
        zip_file.save(zip_path)

        # Extrair
        extract_dir = os.path.join(UPLOAD_DIR, "zip_extract")
        if os.path.exists(extract_dir):
            import shutil
            shutil.rmtree(extract_dir)

        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(extract_dir)

        os.remove(zip_path)

        # Encontrar todos os arquivos de mídia
        media_files = []
        for root, dirs, files in os.walk(extract_dir):
            for f in files:
                ext = f.rsplit('.', 1)[-1].lower() if '.' in f else ''
                if ext in ('mp4', 'mov', 'avi', 'webm', 'mkv', 'png', 'jpg', 'jpeg', 'webp', 'gif'):
                    full_path = os.path.join(root, f)
                    media_files.append((f, full_path))

        # Agrupar por nome base (sem extensão, sem _V/_Q)
        groups = {}
        for filename, filepath in media_files:
            base = filename.rsplit('.', 1)[0]
            sanitized = _sanitize_name(base)
            parts = sanitized.split('_')
            suffix = parts[-1].upper() if len(parts) > 1 else ''

            if suffix == 'Q':
                creative_name = '_'.join(parts[:-1])
                slot = 'feed'
            elif suffix == 'V':
                creative_name = '_'.join(parts[:-1])
                slot = 'reels'
            else:
                creative_name = sanitized
                slot = 'feed'

            if creative_name not in groups:
                groups[creative_name] = {"name": creative_name}

            media_type = _get_media_type(filename)
            if slot == 'feed':
                groups[creative_name]["feedPath"] = filepath
                groups[creative_name]["feedType"] = media_type
            else:
                groups[creative_name]["reelsPath"] = filepath
                groups[creative_name]["reelsType"] = media_type

        # Converter para lista ordenada
        result = sorted(groups.values(), key=lambda x: x["name"])

        return jsonify({"ok": True, "data": result})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


# ── API: Duplicar anúncio (manual) ──────────────────────────────────

@app.route("/api/duplicate", methods=["POST"])
def api_duplicate():
    """
    Duplica um anúncio com nova mídia (vídeo ou imagem).
    Aceita arquivos ou URLs, para feed e reels.
    """
    try:
        ad_id = request.form.get("ad_id")
        name = request.form.get("name", "")

        if not ad_id:
            return jsonify({"ok": False, "error": "ad_id é obrigatório"}), 400

        # Feed (obrigatório)
        feed_type = request.form.get("feed_media_type", "video")
        feed_id = _handle_media_input("media_feed", "media_feed_url", feed_type, f"{name} - Feed")
        if not feed_id:
            return jsonify({"ok": False, "error": "Mídia feed é obrigatória"}), 400

        # Reels (opcional)
        reels_type = request.form.get("reels_media_type", "video")
        reels_id = _handle_media_input("media_reels", "media_reels_url", reels_type, f"{name} - Reels")

        # Duplicar
        result = duplicate_ad_with_new_media(
            ad_id=ad_id,
            feed_media_id=feed_id,
            feed_media_type=feed_type,
            reels_media_id=reels_id,
            reels_media_type=reels_type if reels_id else None,
            new_name=name,
        )

        return jsonify({"ok": True, "data": result})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


def _handle_media_input(file_field, url_field, media_type, title):
    """Processa input de mídia (vídeo ou imagem, arquivo ou URL ou path do servidor)."""
    is_image = media_type == "image"

    # Tentar arquivo upload primeiro
    file = request.files.get(file_field)
    if file and file.filename:
        filepath = os.path.join(UPLOAD_DIR, file.filename)
        file.save(filepath)
        try:
            if is_image:
                return upload_image(filepath)
            return upload_video(filepath, title=title)
        finally:
            if os.path.exists(filepath):
                os.remove(filepath)

    # Tentar path do servidor (ZIP extraído)
    server_path = request.form.get(file_field + "_path")
    if server_path and os.path.exists(server_path):
        if is_image:
            return upload_image(server_path)
        return upload_video(server_path, title=title)

    # Tentar URL
    url = request.form.get(url_field)
    if url:
        if is_image:
            return upload_image_by_url(url)
        return upload_video_by_url(url, title=title)

    return None


# ── Helpers de métricas ──────────────────────────────────────────────

PURCHASE_TYPES = [
    "purchase", "offsite_conversion.fb_pixel_purchase",
    "onsite_conversion.purchase",
]
SALES_OBJECTIVES = ["OUTCOME_SALES"]


def _extract_metrics(ins):
    """Extrai métricas de um objeto insights."""
    conversions = 0
    revenue = 0.0
    cpa = 0.0
    # Pegar apenas o primeiro match para evitar duplicatas
    # (purchase e offsite_conversion.fb_pixel_purchase contam a mesma coisa)
    for action in ins.get("actions", []):
        if action.get("action_type") in PURCHASE_TYPES:
            conversions = int(action.get("value", 0))
            break
    for av in ins.get("action_values", []):
        if av.get("action_type") in PURCHASE_TYPES:
            revenue = float(av.get("value", 0))
            break
    for entry in ins.get("cost_per_action_type", []):
        if entry.get("action_type") in PURCHASE_TYPES:
            cpa = float(entry.get("value", 0))
            break

    spend = float(ins.get("spend", 0))

    # Usar purchase_roas oficial da API se disponível
    roas = 0
    for pr in ins.get("purchase_roas", []):
        if pr.get("action_type") in PURCHASE_TYPES:
            roas = round(float(pr.get("value", 0)), 2)
            break
    # Fallback: calcular manualmente
    if roas == 0 and spend > 0 and revenue > 0:
        roas = round(revenue / spend, 2)

    return {
        "spend": spend,
        "impressions": int(ins.get("impressions", 0)),
        "reach": int(ins.get("reach", 0)),
        "clicks": int(ins.get("clicks", 0)),
        "ctr": round(float(ins.get("ctr", 0)), 2),
        "cpm": round(float(ins.get("cpm", 0)), 2),
        "cpc": round(float(ins.get("cpc", 0) or 0), 2),
        "frequency": round(float(ins.get("frequency", 0)), 2),
        "conversions": conversions,
        "revenue": round(revenue, 2),
        "cpa": round(cpa, 2),
        "roas": roas,
    }


def _add_confidence(ad):
    """
    Adiciona nível de confiança e score composto a um ad.
    Leva em conta gasto, conversões e ROAS.
    - alto: gasto significativo (>R$100) e 3+ conversões
    - medio: gasto moderado (>R$30) e 2+ conversões
    - tendencia: pouco gasto ou apenas 1 conversão — promissor mas sem garantia
    - baixo: sem conversões ou ROAS muito baixo
    """
    spend = ad.get("spend", 0)
    conv = ad.get("conversions", 0)
    roas = ad.get("roas", 0)
    revenue = ad.get("revenue", 0)

    # Score composto: ROAS * min(conversões, 10) * log(gasto+1)
    # Isso penaliza criativos com ROAS alto mas pouco volume
    import math
    volume_factor = min(conv, 10)  # Cap em 10 para não distorcer
    spend_factor = math.log(max(spend, 1) + 1)  # Log do gasto para suavizar
    ad["score"] = round(roas * volume_factor * spend_factor, 2)

    velocity = ad.get("velocity", 0)
    days = ad.get("days_active", 0)

    if conv >= 5 and spend >= 1000:
        ad["confidence"] = "alto"
    elif conv >= 3 and spend >= 500:
        ad["confidence"] = "medio"
    elif conv >= 1 and (days <= 7 or velocity >= -20):
        # Tendência só para criativos novos OU que não estão perdendo escala
        ad["confidence"] = "tendencia"
    elif conv >= 1 and velocity < -20 and days > 7:
        # Tem conversão mas está perdendo escala — em declínio
        ad["confidence"] = "declinando"
    else:
        ad["confidence"] = "baixo"


def _get_effective_status():
    """Retorna filtro de status baseado no parâmetro da request."""
    status = request.args.get("status", "active")
    if status == "with_data":
        return '["ACTIVE","PAUSED","CAMPAIGN_PAUSED","ADSET_PAUSED"]'
    return '["ACTIVE"]'


def _get_sales_campaign_ids():
    """Retorna IDs das campanhas de vendas ativas."""
    from meta_api import get_token, get_ad_account_id, BASE_URL, _check_response
    import requests as req

    url = f"{BASE_URL}/{get_ad_account_id()}/campaigns"
    params = {
        "access_token": get_token(),
        "fields": "id,objective",
        "effective_status": '["ACTIVE"]',
        "limit": 500,
    }
    resp = req.get(url, params=params)
    data = _check_response(resp)
    return [c["id"] for c in data.get("data", []) if c.get("objective") in SALES_OBJECTIVES]


def _get_ads_for_campaigns(campaign_ids, fields, effective_status):
    """Busca ads de campanhas específicas."""
    from meta_api import get_token, BASE_URL, _check_response
    import requests as req

    all_ads = []
    for cid in campaign_ids:
        url = f"{BASE_URL}/{cid}/ads"
        params = {
            "access_token": get_token(),
            "fields": fields,
            "effective_status": effective_status,
            "limit": 200,
        }
        resp = req.get(url, params=params)
        data = _check_response(resp)
        all_ads.extend(data.get("data", []))

    return all_ads


# ── API: Dashboard de campanhas (só vendas com dados) ───────────────

@app.route("/api/dashboard")
def api_dashboard():
    try:
        from meta_api import get_token, get_ad_account_id, BASE_URL, _check_response
        import requests as req
        from datetime import datetime, timedelta
        import math

        date_from = request.args.get("since", "")
        date_to = request.args.get("until", "")
        dt_to = datetime.strptime(date_to, "%Y-%m-%d")
        recent_7d = (dt_to - timedelta(days=7)).strftime("%Y-%m-%d")
        recent_3d = (dt_to - timedelta(days=3)).strftime("%Y-%m-%d")

        url = f"{BASE_URL}/{get_ad_account_id()}/campaigns"
        insight_fields = "spend,impressions,reach,clicks,ctr,cpm,cpc,actions,action_values,cost_per_action_type,purchase_roas,frequency"

        # Período total
        params = {
            "access_token": get_token(),
            "fields": f"id,name,status,objective,created_time,insights.time_range({{'since':'{date_from}','until':'{date_to}'}}){{{insight_fields}}}",
            "effective_status": _get_effective_status(),
            "limit": 200,
        }
        resp = req.get(url, params=params)
        data = _check_response(resp)
        campaigns = data.get("data", [])

        # Últimos 7 dias
        params_7d = {
            "access_token": get_token(),
            "fields": f"id,insights.time_range({{'since':'{recent_7d}','until':'{date_to}'}}){{{insight_fields}}}",
            "effective_status": _get_effective_status(),
            "limit": 200,
        }
        resp_7d = req.get(url, params=params_7d)
        data_7d = _check_response(resp_7d)
        map_7d = {}
        for c in data_7d.get("data", []):
            ins = (c.get("insights", {}).get("data", [{}]) or [{}])[0]
            map_7d[c["id"]] = _extract_metrics(ins)

        # Últimos 3 dias
        params_3d = {
            "access_token": get_token(),
            "fields": f"id,insights.time_range({{'since':'{recent_3d}','until':'{date_to}'}}){{{insight_fields}}}",
            "effective_status": _get_effective_status(),
            "limit": 200,
        }
        resp_3d = req.get(url, params=params_3d)
        data_3d = _check_response(resp_3d)
        map_3d = {}
        for c in data_3d.get("data", []):
            ins = (c.get("insights", {}).get("data", [{}]) or [{}])[0]
            map_3d[c["id"]] = _extract_metrics(ins)

        result = []
        for camp in campaigns:
            obj = camp.get("objective", "")
            if obj not in SALES_OBJECTIVES:
                continue

            insights = camp.get("insights", {}).get("data", [])
            if not insights:
                continue

            m = _extract_metrics(insights[0])
            if m["spend"] == 0:
                continue

            m["id"] = camp["id"]
            m["name"] = camp["name"]
            m["objective"] = obj

            # Dados 7d
            r7 = map_7d.get(camp["id"], {})
            m["spend_7d"] = r7.get("spend", 0)
            m["revenue_7d"] = r7.get("revenue", 0)
            m["roas_7d"] = r7.get("roas", 0)
            m["conv_7d"] = r7.get("conversions", 0)
            m["cpa_7d"] = r7.get("cpa", 0)

            # Dados 3d
            r3 = map_3d.get(camp["id"], {})
            m["spend_3d"] = r3.get("spend", 0)
            m["revenue_3d"] = r3.get("revenue", 0)
            m["roas_3d"] = r3.get("roas", 0)
            m["conv_3d"] = r3.get("conversions", 0)

            # Período anterior (total - 7d)
            spend_prev = m["spend"] - m["spend_7d"]
            revenue_prev = m["revenue"] - m["revenue_7d"]
            roas_prev = round(revenue_prev / spend_prev, 2) if spend_prev > 0 else 0
            m["roas_prev"] = roas_prev

            # Tendência ROAS: comparar 7d vs anterior
            if roas_prev > 0:
                roas_change = round(((m["roas_7d"] - roas_prev) / roas_prev) * 100)
            else:
                roas_change = 0
            m["roas_trend"] = roas_change

            # Health: baseado no ROAS atual e 7d, não na tendência
            roas_min = min(m["roas"], m["roas_7d"])
            if roas_min >= 1:
                m["health"] = "saudavel"
            elif roas_min >= 0.9:
                m["health"] = "estavel"
            elif roas_min >= 0.7:
                m["health"] = "atencao"
            else:
                m["health"] = "critico"

            # Score: prioriza volume (topo de funil) com ROAS como multiplicador
            # Volume = conversões (sem cap) — quanto mais vendas, melhor
            # Escala = gasto (proporcional) — campanha que gasta mais está escalando
            # ROAS = multiplicador suave — acima de 0.9 é positivo, abaixo penaliza
            # Tendência = boost se melhorando, penalidade se piorando
            trend_mult = 1.15 if roas_change > 10 else (0.85 if roas_change < -20 else 1.0)
            roas_mult = max(m["roas"], 0.3)  # ROAS como multiplicador (mínimo 0.3 para não zerar)
            volume = m["conversions"]  # Sem cap — volume é primordial
            escala = math.sqrt(m["spend"])  # Raiz quadrada do gasto para suavizar diferenças extremas
            m["score"] = round(volume * escala * roas_mult * trend_mult, 2)

            result.append(m)

        result.sort(key=lambda x: x["score"], reverse=True)

        # Adicionar rank
        for i, m in enumerate(result):
            m["rank"] = i + 1

        return jsonify({"ok": True, "data": result})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


# ── API: Top criativos por campanha ─────────────────────────────────

@app.route("/api/top-creatives")
def api_top_creatives():
    """Top 5 criativos de cada campanha, ranqueados por volume + ROAS + tendência."""
    try:
        from meta_api import get_token, get_ad_account_id, BASE_URL, _check_response
        import requests as req
        from datetime import datetime, timedelta

        date_from = request.args.get("since", "")
        date_to = request.args.get("until", "")

        # Calcular períodos recentes para tendência
        dt_to = datetime.strptime(date_to, "%Y-%m-%d")
        recent_3d_from = (dt_to - timedelta(days=3)).strftime("%Y-%m-%d")
        recent_7d_from = (dt_to - timedelta(days=7)).strftime("%Y-%m-%d")

        # Buscar IDs das campanhas de vendas
        sales_ids = _get_sales_campaign_ids()
        if not sales_ids:
            return jsonify({"ok": True, "data": [], "all_ads": []})

        eff_status = _get_effective_status()

        # Buscar ads do período total (por campanha de vendas)
        total_fields = (
            "id,name,status,created_time,campaign{id,name,objective},"
            "insights.time_range({'since':'" + date_from + "','until':'" + date_to + "'}){"
            "spend,impressions,reach,clicks,ctr,cpm,"
            "actions,action_values,cost_per_action_type,purchase_roas,frequency"
            "}"
        )
        ads_list = _get_ads_for_campaigns(sales_ids, total_fields, eff_status)
        ads_total = {ad["id"]: ad for ad in ads_list}

        # Buscar dados dos últimos 3 dias
        insight_fields = "spend,actions,action_values,purchase_roas"
        fields_3d = f"id,insights.time_range({{'since':'{recent_3d_from}','until':'{date_to}'}}){{{insight_fields}}}"
        ads_3d = _get_ads_for_campaigns(sales_ids, fields_3d, eff_status)
        map_3d = {}
        for ad in ads_3d:
            ins = (ad.get("insights", {}).get("data", [{}]) or [{}])[0]
            map_3d[ad["id"]] = _extract_metrics(ins)

        # Buscar dados dos últimos 7 dias
        fields_7d = f"id,insights.time_range({{'since':'{recent_7d_from}','until':'{date_to}'}}){{{insight_fields}}}"
        ads_7d = _get_ads_for_campaigns(sales_ids, fields_7d, eff_status)
        map_7d = {}
        for ad in ads_7d:
            ins = (ad.get("insights", {}).get("data", [{}]) or [{}])[0]
            map_7d[ad["id"]] = _extract_metrics(ins)

        # Agrupar por campanha
        campaigns = {}
        for ad_id, ad in ads_total.items():
            camp = ad.get("campaign", {})
            if camp.get("objective", "") not in SALES_OBJECTIVES:
                continue

            insights = ad.get("insights", {}).get("data", [])
            if not insights:
                continue

            metrics = _extract_metrics(insights[0])
            if metrics["spend"] == 0:
                continue

            metrics["ad_id"] = ad["id"]
            metrics["ad_name"] = ad["name"]
            metrics["campaign_id"] = camp.get("id", "")
            metrics["campaign_name"] = camp.get("name", "")

            # Dias ativo
            created = ad.get("created_time", "")
            if created:
                metrics["days_active"] = (datetime.now() - datetime.fromisoformat(created[:10])).days
            else:
                metrics["days_active"] = 0

            # Dados dos últimos 3d e 7d
            r3 = map_3d.get(ad_id, {})
            r7 = map_7d.get(ad_id, {})
            total_spend = metrics["spend"]
            total_days = max((dt_to - datetime.strptime(date_from, "%Y-%m-%d")).days, 1)
            daily_avg = total_spend / total_days

            # 3 dias
            spend_3d = r3.get("spend", 0)
            conv_3d = r3.get("conversions", 0)
            roas_3d = r3.get("roas", 0)
            daily_3d = spend_3d / 3 if spend_3d > 0 else 0

            # 7 dias
            spend_7d = r7.get("spend", 0)
            conv_7d = r7.get("conversions", 0)
            roas_7d = r7.get("roas", 0)
            daily_7d = spend_7d / 7 if spend_7d > 0 else 0

            metrics["spend_3d"] = round(spend_3d, 2)
            metrics["conv_3d"] = conv_3d
            metrics["roas_3d"] = roas_3d
            metrics["spend_7d"] = round(spend_7d, 2)
            metrics["conv_7d"] = conv_7d
            metrics["roas_7d"] = roas_7d

            metrics["recent_spend"] = round(spend_3d, 2)
            metrics["recent_conversions"] = conv_3d

            cid = camp.get("id", "")
            if cid not in campaigns:
                campaigns[cid] = {"name": camp.get("name", ""), "ads": []}
            campaigns[cid]["ads"].append(metrics)

        # Calcular velocidade relativa (participação no gasto da campanha)
        import math
        total_period_days = max((dt_to - datetime.strptime(date_from, "%Y-%m-%d")).days, 1)

        for cid, cdata in campaigns.items():
            ads = cdata["ads"]
            camp_spend_total = sum(a["spend"] for a in ads)
            camp_spend_3d = sum(a["recent_spend"] for a in ads)
            camp_spend_7d = sum(a.get("spend_7d", 0) for a in ads)

            # Calcular gasto diário médio da campanha no período total
            camp_daily_avg = camp_spend_total / total_period_days if total_period_days > 0 else 0

            for a in ads:
                days = a.get("days_active", 0)

                # Participação: gasto do criativo / gasto da campanha no mesmo período
                # Para criativos novos, usar apenas os dias que ele existiu
                if days > 0 and days < total_period_days:
                    # Criativo mais novo que o período: comparar com gasto da campanha
                    # estimado para os dias que o criativo existiu
                    camp_spend_in_ad_period = camp_daily_avg * days
                    share_total = (a["spend"] / camp_spend_in_ad_period * 100) if camp_spend_in_ad_period > 0 else 0
                else:
                    share_total = (a["spend"] / camp_spend_total * 100) if camp_spend_total > 0 else 0

                share_3d = (a["recent_spend"] / camp_spend_3d * 100) if camp_spend_3d > 0 else 0
                share_7d = (a.get("spend_7d", 0) / camp_spend_7d * 100) if camp_spend_7d > 0 else 0

                a["share_total"] = round(share_total, 1)
                a["share_3d"] = round(share_3d, 1)
                a["share_7d"] = round(share_7d, 1)

                # Velocidade adaptativa baseada na idade do criativo
                if days <= 2:
                    share_change = 0
                    a["velocity_note"] = "novo"
                elif days <= 9:
                    # Criativo novo (3-9 dias): comparar share 3d vs share total
                    if share_total > 0:
                        share_change = round(((share_3d - share_total) / share_total) * 100)
                        share_change = max(min(share_change, 100), -100)
                    else:
                        share_change = 0
                    a["velocity_note"] = "recente"
                else:
                    # Criativo maduro (10+ dias): comparação normal
                    if share_total > 0:
                        share_change = round(((share_3d - share_total) / share_total) * 100)
                    else:
                        share_change = 0
                    a["velocity_note"] = "maduro"

                a["velocity"] = share_change

                if days <= 3:
                    a["trend"] = "estavel"  # Muito novo para avaliar
                elif share_change >= 30:
                    a["trend"] = "escalando"
                elif share_change <= -30:
                    a["trend"] = "caindo"
                else:
                    a["trend"] = "estavel"

                _add_confidence(a)

                trend_boost = 1.3 if a["trend"] == "escalando" else (0.7 if a["trend"] == "caindo" else 1.0)
                a["score"] = round(
                    a["conversions"] * a["roas"] * math.log(max(a["spend"], 1) + 1) * trend_boost, 2
                )

        # Calcular médias por campanha
        for cid, cdata in campaigns.items():
            ads = cdata["ads"]
            total_spend = sum(a["spend"] for a in ads)
            total_conv = sum(a["conversions"] for a in ads)
            total_revenue = sum(a["revenue"] for a in ads)
            camp_roas = round(total_revenue / total_spend, 2) if total_spend > 0 else 0
            camp_cpa = round(total_spend / total_conv, 2) if total_conv > 0 else 0
            cdata["camp_roas"] = camp_roas
            cdata["camp_cpa"] = camp_cpa
            # Adicionar a cada ad
            for ad in ads:
                ad["camp_roas"] = camp_roas
                ad["camp_cpa"] = camp_cpa

        # Top 5 por score em cada campanha + todos os ads para piores
        result = []
        all_ads = []
        for cid, cdata in campaigns.items():
            sorted_ads = sorted(cdata["ads"], key=lambda x: x["score"], reverse=True)[:5]
            result.append({
                "campaign_id": cid,
                "campaign_name": cdata["name"],
                "camp_roas": cdata["camp_roas"],
                "camp_cpa": cdata["camp_cpa"],
                "top_ads": sorted_ads,
            })
            for ad in cdata["ads"]:
                ad["campaign_name"] = cdata["name"]
                all_ads.append(ad)

        result.sort(key=lambda x: x["campaign_name"])
        return jsonify({"ok": True, "data": result, "all_ads": all_ads})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


# ── API: Destaques recentes (criativos novos que performaram) ───────

@app.route("/api/highlights")
def api_highlights():
    """Criativos criados nos últimos 7 dias com bom desempenho."""
    try:
        from meta_api import get_token, get_ad_account_id, BASE_URL, _check_response
        import requests as req
        from datetime import datetime, timedelta

        date_to = request.args.get("until", datetime.now().strftime("%Y-%m-%d"))
        date_from = request.args.get("since", (datetime.now() - timedelta(days=17)).strftime("%Y-%m-%d"))
        recent_cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00")

        sales_ids = _get_sales_campaign_ids()
        if not sales_ids:
            return jsonify({"ok": True, "data": []})

        fields = (
            "id,name,status,created_time,campaign{id,name,objective},"
            "insights.time_range({'since':'" + date_from + "','until':'" + date_to + "'}){"
            "spend,impressions,clicks,ctr,"
            "actions,action_values,cost_per_action_type,purchase_roas"
            "}"
        )
        eff_status = _get_effective_status()

        # Buscar TODOS os ads da campanha (para calcular participação real)
        all_fields = (
            "id,name,created_time,campaign{id,name},"
            "insights.time_range({'since':'" + date_from + "','until':'" + date_to + "'}){"
            "spend,actions,action_values,purchase_roas"
            "}"
        )
        all_ads = _get_ads_for_campaigns(sales_ids, all_fields, eff_status)

        # Buscar dados 3d de TODOS
        recent_3d = (datetime.strptime(date_to, "%Y-%m-%d") - timedelta(days=3)).strftime("%Y-%m-%d")
        fields_3d = f"id,insights.time_range({{'since':'{recent_3d}','until':'{date_to}'}}){{spend}}"
        ads_3d = _get_ads_for_campaigns(sales_ids, fields_3d, eff_status)
        map_3d = {}
        for a in ads_3d:
            ins = (a.get("insights", {}).get("data", [{}]) or [{}])[0]
            map_3d[a["id"]] = float(ins.get("spend", 0))

        # Gasto total de cada campanha (todos os ads)
        camp_spend_total = {}
        camp_spend_3d = {}
        for ad in all_ads:
            cname = ad.get("campaign", {}).get("name", "")
            ins = (ad.get("insights", {}).get("data", [{}]) or [{}])[0]
            spend = float(ins.get("spend", 0))
            camp_spend_total[cname] = camp_spend_total.get(cname, 0) + spend
            camp_spend_3d[cname] = camp_spend_3d.get(cname, 0) + map_3d.get(ad["id"], 0)

        # Filtrar só os recentes com conversões
        result = []
        for ad in all_ads:
            created = ad.get("created_time", "")
            if created < recent_cutoff:
                continue

            ins_list = ad.get("insights", {}).get("data", [])
            if not ins_list:
                continue

            metrics = _extract_metrics(ins_list[0])
            if metrics["spend"] == 0 or metrics["conversions"] == 0:
                continue

            cname = ad.get("campaign", {}).get("name", "")
            metrics["ad_id"] = ad["id"]
            metrics["ad_name"] = ad["name"]
            metrics["campaign_name"] = cname
            metrics["created_time"] = created[:10]

            days_active = max((datetime.now() - datetime.fromisoformat(created[:10])).days, 1)
            metrics["days_active"] = days_active

            # Participação real (sobre toda a campanha)
            ct = camp_spend_total.get(cname, 0)
            c3 = camp_spend_3d.get(cname, 0)
            ad_spend_3d = map_3d.get(ad["id"], 0)

            share_total = round((metrics["spend"] / ct * 100) if ct > 0 else 0, 1)
            share_3d = round((ad_spend_3d / c3 * 100) if c3 > 0 else 0, 1)
            metrics["share_total"] = share_total
            metrics["share_3d"] = share_3d

            # Velocidade relativa
            if days_active <= 3:
                metrics["velocity"] = 0
                metrics["velocity_note"] = "novo"
                metrics["trend"] = "estavel"
            elif share_total > 0:
                vel = round(((share_3d - share_total) / share_total) * 100)
                if days_active <= 7:
                    vel = max(min(vel, 100), -100)
                    metrics["velocity_note"] = "recente"
                else:
                    metrics["velocity_note"] = "maduro"
                metrics["velocity"] = vel
                metrics["trend"] = "escalando" if vel >= 30 else ("caindo" if vel <= -30 else "estavel")
            else:
                metrics["velocity"] = 0
                metrics["velocity_note"] = "novo"
                metrics["trend"] = "estavel"

            metrics["_diffPct"] = metrics["velocity"]

            _add_confidence(metrics)
            result.append(metrics)

        result.sort(key=lambda x: x["score"], reverse=True)
        return jsonify({"ok": True, "data": result})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


# ── API: Criativos antigos sem resultado (sugestão de pausa) ────────

@app.route("/api/stale-creatives")
def api_stale_creatives():
    """Criativos ativos há mais de 15 dias sem conversões."""
    try:
        from meta_api import get_token, get_ad_account_id, BASE_URL, _check_response
        import requests as req
        from datetime import datetime, timedelta

        date_to = request.args.get("until", datetime.now().strftime("%Y-%m-%d"))
        date_from = request.args.get("since", (datetime.now() - timedelta(days=17)).strftime("%Y-%m-%d"))
        old_cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00")

        sales_ids = _get_sales_campaign_ids()
        if not sales_ids:
            return jsonify({"ok": True, "data": []})

        fields = (
            "id,name,status,created_time,campaign{id,name,objective},"
            "insights.time_range({'since':'" + date_from + "','until':'" + date_to + "'}){"
            "spend,impressions,clicks,actions,action_values,cost_per_action_type,purchase_roas"
            "}"
        )
        ads = _get_ads_for_campaigns(sales_ids, fields, _get_effective_status())

        # Buscar dados 3d e 7d para tendência
        dt_to = datetime.strptime(date_to, "%Y-%m-%d")
        recent_3d = (dt_to - timedelta(days=3)).strftime("%Y-%m-%d")
        recent_7d = (dt_to - timedelta(days=7)).strftime("%Y-%m-%d")
        eff = _get_effective_status()
        short_fields = "spend,actions,action_values,purchase_roas"

        fields_3d = "id,insights.time_range({'since':'" + recent_3d + "','until':'" + date_to + "'}){" + short_fields + "}"
        ads_3d = _get_ads_for_campaigns(sales_ids, fields_3d, eff)
        map_3d = {}
        for a3 in ads_3d:
            ins3 = (a3.get("insights", {}).get("data", [{}]) or [{}])[0]
            m3 = _extract_metrics(ins3)
            map_3d[a3["id"]] = {"spend": m3["spend"], "conv": m3["conversions"], "roas": m3["roas"]}

        fields_7d = "id,insights.time_range({'since':'" + recent_7d + "','until':'" + date_to + "'}){" + short_fields + "}"
        ads_7d = _get_ads_for_campaigns(sales_ids, fields_7d, eff)
        map_7d = {}
        for a7 in ads_7d:
            ins7 = (a7.get("insights", {}).get("data", [{}]) or [{}])[0]
            m7 = _extract_metrics(ins7)
            map_7d[a7["id"]] = {"spend": m7["spend"], "conv": m7["conversions"], "roas": m7["roas"]}

        # Calcular gasto total por campanha para participação
        camp_spend = {}
        camp_spend_3d = {}
        camp_spend_7d = {}
        for ad in ads:
            cname = ad.get("campaign", {}).get("name", "")
            ins_list = ad.get("insights", {}).get("data", [])
            s = float(ins_list[0].get("spend", 0)) if ins_list else 0
            camp_spend[cname] = camp_spend.get(cname, 0) + s
            camp_spend_3d[cname] = camp_spend_3d.get(cname, 0) + map_3d.get(ad["id"], {}).get("spend", 0)
            camp_spend_7d[cname] = camp_spend_7d.get(cname, 0) + map_7d.get(ad["id"], {}).get("spend", 0)

        result = []
        for ad in ads:
            camp = ad.get("campaign", {})
            if camp.get("objective", "") not in SALES_OBJECTIVES:
                continue

            created = ad.get("created_time", "")
            if created > old_cutoff:
                continue  # Muito novo, pular

            insights = ad.get("insights", {}).get("data", [])
            metrics = _extract_metrics(insights[0]) if insights else _extract_metrics({})
            days_active = (datetime.now() - datetime.fromisoformat(created[:10])).days if created else 0

            r3 = map_3d.get(ad["id"], {"spend": 0, "conv": 0, "roas": 0})
            r7 = map_7d.get(ad["id"], {"spend": 0, "conv": 0, "roas": 0})
            cname = camp.get("name", "")
            ct = camp_spend.get(cname, 0)
            c3 = camp_spend_3d.get(cname, 0)
            c7 = camp_spend_7d.get(cname, 0)

            share = round((metrics["spend"] / ct * 100), 1) if ct > 0 else 0
            share_3d = round((r3["spend"] / c3 * 100), 1) if c3 > 0 else 0
            share_7d = round((r7["spend"] / c7 * 100), 1) if c7 > 0 else 0

            should_suggest = False
            reason = ""

            if metrics["conversions"] == 0 and metrics["spend"] > 0:
                # Caso 1: Sem conversões com gasto
                should_suggest = True
                reason = "SEM VENDAS"
            elif metrics["conversions"] == 0 and metrics["spend"] == 0 and metrics["impressions"] == 0:
                # Caso 2: Meta não está investindo nada (sem impressão)
                should_suggest = True
                reason = "SEM INVESTIMENTO"
            elif metrics["conversions"] <= 1 and days_active >= 10 and share_3d < share * 0.5:
                # Caso 3: Poucas conversões e perdendo participação (sem tendência de melhora)
                should_suggest = True
                reason = "SEM TENDENCIA"
            elif metrics["spend"] > 0 and share_3d < 0.5 and metrics["conversions"] <= 1 and days_active >= 7:
                # Caso 4: Participação insignificante (<0.5%) e sem vendas relevantes
                should_suggest = True
                reason = "IGNORADO"

            if should_suggest:
                metrics["ad_id"] = ad["id"]
                metrics["ad_name"] = ad["name"]
                metrics["campaign_name"] = cname
                metrics["created_time"] = created[:10]
                metrics["days_active"] = days_active
                metrics["reason"] = reason
                metrics["share"] = share
                metrics["share_3d"] = share_3d
                metrics["share_7d"] = share_7d
                metrics["spend_3d"] = round(r3["spend"], 2)
                metrics["conv_3d"] = r3["conv"]
                metrics["roas_3d"] = r3["roas"]
                metrics["spend_7d"] = round(r7["spend"], 2)
                metrics["conv_7d"] = r7["conv"]
                metrics["roas_7d"] = r7["roas"]
                result.append(metrics)

        result.sort(key=lambda x: x["spend"], reverse=True)
        return jsonify({"ok": True, "data": result})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


# ── API: Rate Limit ─────────────────────────────────────────────────

@app.route("/api/rate-limit")
def api_rate_limit():
    return jsonify({"ok": True, "data": get_rate_limit_info(), "max_batch": MAX_DUPLICATIONS_PER_BATCH})


# ── API: Verificar conexão ──────────────────────────────────────────

@app.route("/api/check-connection")
def api_check_connection():
    """Verifica se o token e account ID estão configurados e funcionando."""
    token = os.getenv("META_ACCESS_TOKEN", "")
    account_id = os.getenv("META_AD_ACCOUNT_ID", "")

    if not token or not account_id:
        return jsonify({
            "ok": False,
            "error": "META_ACCESS_TOKEN ou META_AD_ACCOUNT_ID não configurados no .env",
        })

    try:
        campaigns = list_campaigns()
        return jsonify({
            "ok": True,
            "message": f"Conectado! {len(campaigns)} campanha(s) encontrada(s).",
            "account_id": account_id,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": f"Erro na API: {str(e)}"})


# ── API: Pausar anúncios ────────────────────────────────────────────

@app.route("/api/pause-ads", methods=["POST"])
def api_pause_ads():
    """Pausa uma lista de anúncios pelo ID."""
    try:
        from meta_api import get_token, BASE_URL, _check_response
        import requests as req

        data = request.get_json()
        ad_ids = data.get("ad_ids", [])

        if not ad_ids:
            return jsonify({"ok": False, "error": "Nenhum anúncio selecionado"}), 400

        results = []
        for ad_id in ad_ids:
            try:
                url = f"{BASE_URL}/{ad_id}"
                resp = req.post(url, data={
                    "access_token": get_token(),
                    "status": "PAUSED",
                })
                _check_response(resp)
                results.append({"id": ad_id, "status": "paused"})
            except Exception as e:
                results.append({"id": ad_id, "status": "error", "error": str(e)})

        paused = len([r for r in results if r["status"] == "paused"])
        return jsonify({"ok": True, "data": results, "paused": paused, "total": len(ad_ids)})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


# ── API: Token info ─────────────────────────────────────────────────

@app.route("/api/token-info")
def api_token_info():
    """Retorna informações sobre o token (expiração)."""
    try:
        from meta_api import get_token, BASE_URL
        import requests as req

        url = f"https://graph.facebook.com/v21.0/debug_token"
        params = {
            "input_token": get_token(),
            "access_token": get_token(),
        }
        resp = req.get(url, params=params)
        data = resp.json().get("data", {})

        expires_at = data.get("expires_at", 0)
        if expires_at:
            from datetime import datetime
            expires_date = datetime.fromtimestamp(expires_at)
            days_left = (expires_date - datetime.now()).days
            return jsonify({
                "ok": True,
                "expires_at": expires_date.strftime("%Y-%m-%d"),
                "days_left": days_left,
            })
        return jsonify({"ok": True, "expires_at": None, "days_left": -1})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


if __name__ == "__main__":
    app.run(debug=True, port=5000)
