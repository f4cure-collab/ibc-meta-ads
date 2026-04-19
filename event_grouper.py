"""
Agrupa campanhas Meta Ads por evento.

Lógica:
1. Extrai tipo de evento (SPK, DSP, AIE, Perpétuo) e cidade do nome da campanha
2. Agrupa campanhas com mesmo tipo+cidade
3. Se houver gap > 60 dias entre campanhas do mesmo tipo+cidade, separa em eventos distintos
4. RMKT é agrupado como "Remarketing Geral" (cobre todos os eventos)
"""

import re
from datetime import datetime, timedelta

# Mapeamento de siglas de cidade para nome completo
CITY_MAP = {
    "BH": "Belo Horizonte",
    "RJ": "Rio de Janeiro",
    "POA": "Porto Alegre",
    "BSB": "Brasília",
    "GYN": "Goiânia",
    "GOIANIA": "Goiânia",
    "FLORIPA": "Florianópolis",
    "FLORIANOPOLIS": "Florianópolis",
    "SJC": "São José dos Campos",
    "SAOJOSEDOSCAMPOS": "São José dos Campos",
    "SJRP": "São José do Rio Preto",
    "JOAOPESSOA": "João Pessoa",
    "JOAO_PESSOA": "João Pessoa",
    "JUIZDEFORA": "Juiz de Fora",
    "JUIZ_DE_FORA": "Juiz de Fora",
    "SAOLUIS": "São Luís",
    "SAO_LUIS": "São Luís",
    "SP": "São Paulo",
    "SAOPAULO": "São Paulo",
    "SAO_PAULO": "São Paulo",
    "UBERLANDIA": "Uberlândia",
    "LONDRINA": "Londrina",
    "CAMPINAS": "Campinas",
    "CURITIBA": "Curitiba",
    "RECIFE": "Recife",
    "SANTOS": "Santos",
    "SALVADOR": "Salvador",
    "ARACAJU": "Aracaju",
    "IMPERATRIZ": "Imperatriz",
    "MANAUS": "Manaus",
    "VITORIA": "Vitória",
    "GRAMADO": "Gramado",
    "FOZ": "Foz do Iguaçu",
    "ORLANDO": "Orlando",
    "BOSTON": "Boston",
    "BELEM": "Belém",
    "FORTALEZA": "Fortaleza",
    "NATAL": "Natal",
    "MACEIO": "Maceió",
    "TERESINA": "Teresina",
    "CUIABA": "Cuiabá",
    "CAMPOGRANDE": "Campo Grande",
    "CAMPO_GRANDE": "Campo Grande",
    "PORTOVELHO": "Porto Velho",
    "PORTO_VELHO": "Porto Velho",
    "RIOBRANCO": "Rio Branco",
    "RIO_BRANCO": "Rio Branco",
    "PALMAS": "Palmas",
    "MACAPA": "Macapá",
    "BOAVISTA": "Boa Vista",
    "BOA_VISTA": "Boa Vista",
    "SAOJOAOBOAVISTA": "São João da Boa Vista",
    "RIBEIRAOPRETO": "Ribeirão Preto",
    "RIBEIRAO_PRETO": "Ribeirão Preto",
    "RIBEIRAO": "Ribeirão Preto",
    "RMKT": "Remarketing",
}

EVENT_TYPE_MAP = {
    "SPK": "Speaker",
    "DSP": "Desperte seu Poder",
    "AIE": "Alto Impacto Empresarial",
    "METEORICO": "Meteorico",
}

# Produtos comerciais (highticket). Usados para agrupar campanhas de leads comerciais.
COMERCIAL_PRODUCT_MAP = {
    "MTR": "Master Trainer",
    "PSC": "Professional & Self Coaching",
    "OHIO": "Ohio",
    "CSI": "Constelacao Sistemica",
    "PNL": "PNL",
}

# Normalizar variantes de city_key para forma canônica
CITY_KEY_NORMALIZE = {
    "JOAO_PESSOA": "JOAOPESSOA",
    "JUIZ_DE_FORA": "JUIZDEFORA",
    "SAO_LUIS": "SAOLUIS",
    "SP": "SAOPAULO",
    "SAO_PAULO": "SAOPAULO",
    "FLORIANOPOLIS": "FLORIPA",
    "GOIANIA": "GYN",
    "SAOJOSEDOSCAMPOS": "SJC",
}


def _parse_campaign_name(name):
    """Extrai tipo de evento e cidade do nome da campanha.

    Retorna (event_type, city_key, city_name) ou None se não conseguir parsear.
    """
    name_upper = name.upper().replace("Á", "A").replace("É", "E").replace("Ú", "U").replace("Ã", "A").replace("Ó", "O")
    tokens_split = set(name_upper.replace("-", "_").replace(".", "_").split("_"))

    # Comercial: agrupa por produto (MTR, PSC, OHIO, CSI, PNL) em vez de evento+cidade
    for prod_key, prod_name in COMERCIAL_PRODUCT_MAP.items():
        if prod_key in tokens_split:
            # Reutiliza o contrato (event_type, city_key, city_name).
            # event_type = codigo do produto, city_name = nome completo.
            return (prod_key, prod_key, prod_name)

    # Meteoricos: qualquer campanha com token METEORICO e agrupada por CIDADE apenas.
    # Ignora DSP/SPK/AIE no nome (todo meteorico e um so tipo de evento).
    if "METEORICO" in tokens_split or "METEORICOS" in tokens_split:
        best_city = None
        best_key = None
        for key, full in CITY_MAP.items():
            if key == "RMKT":
                continue
            if key in tokens_split:
                if best_key is None or len(key) > len(best_key):
                    best_key = key
                    best_city = full
        if not best_city:
            for key, full in CITY_MAP.items():
                if key == "RMKT" or len(key) <= 3:
                    continue
                if key in name_upper:
                    if best_key is None or len(key) > len(best_key):
                        best_key = key
                        best_city = full
        if best_city:
            best_key = CITY_KEY_NORMALIZE.get(best_key, best_key)
            best_city = CITY_MAP.get(best_key, best_city)
            return ("METEORICO", best_key, best_city)
        # METEORICO sem cidade reconhecida -> nao identificada (cai em Outros)
        return None

    # Detectar RMKT
    if "_RMKT" in name_upper:
        return ("RMKT", "GERAL", "Geral")

    # Perpetuo: tratar como SPK (é Speaker, não um tipo separado)
    if "PERPETUO" in name_upper or "PERPÉTUO" in name_upper:
        for key, full in CITY_MAP.items():
            if key in name_upper:
                return ("SPK", key, full)
        return ("SPK", "GERAL", "Geral")

    # Detectar tipo: SPK, DSP, AIE
    event_type = None
    for et in ["SPK", "DSP", "AIE"]:
        if f"_{et}_" in name_upper or f"_{et}" == name_upper[-len(f"_{et}"):] or name_upper.startswith(f"{et}_"):
            event_type = et
            break
        # Também checar com VENDAS_ prefix
        if f"VENDAS_{et}_" in name_upper:
            event_type = et
            break

    if not event_type:
        # Tentar detectar pelo contexto mais amplo
        for et in ["SPK", "DSP", "AIE"]:
            if et in name_upper:
                event_type = et
                break

    # Nomes especiais: LF_BOSTON_VENDAS, LF_ORLANDO_VENDAS etc
    if not event_type and ("VENDAS" in name_upper or "VENDA" in name_upper):
        # Tentar detectar cidade mesmo sem tipo explícito → assume DSP
        for key in sorted(CITY_MAP.keys(), key=len, reverse=True):
            if key == "RMKT":
                continue
            if key in name_upper:
                event_type = "DSP"
                break

    if not event_type:
        return None

    # Detectar cidade: match exato por token (evita falsos positivos como
    # "SP" dentro de "SPK"). Se nao achar por token, cai num fallback de
    # substring pra cobrir chaves longas que podem estar sem separadores.
    tokens = set(name_upper.replace("-", "_").split("_"))
    best_city = None
    best_key = None
    for key, full in CITY_MAP.items():
        if key == "RMKT":
            continue
        if key in tokens:
            if best_key is None or len(key) > len(best_key):
                best_key = key
                best_city = full
    # Fallback: chaves longas grudadas no nome (ex: "SPKFLORIPAV1")
    if not best_city:
        for key, full in CITY_MAP.items():
            if key == "RMKT" or len(key) <= 3:
                # chaves curtas (SP, RJ, BH, POA, BSB, GYN, SJC, SJRP, FOZ)
                # nao participam do fallback pra nao colidir com SPK, AIE etc
                continue
            if key in name_upper:
                if best_key is None or len(key) > len(best_key):
                    best_key = key
                    best_city = full

    if not best_city:
        return None

    # Normalizar a key para forma canônica
    best_key = CITY_KEY_NORMALIZE.get(best_key, best_key)
    # Atualizar o nome da cidade com a key normalizada
    best_city = CITY_MAP.get(best_key, best_city)

    return (event_type, best_key, best_city)


def group_campaigns_by_event(campaigns, gap_days=60):
    """Agrupa campanhas por evento.

    Args:
        campaigns: lista de dicts com pelo menos 'id', 'name', 'start_time' ou 'created_time'
        gap_days: se duas campanhas do mesmo tipo+cidade têm gap > N dias, são eventos diferentes

    Returns:
        lista de eventos, cada um com:
        {
            "event_id": "DSP_BH_2026-01",
            "event_name": "Desperte seu Poder — Belo Horizonte",
            "event_type": "DSP",
            "city": "Belo Horizonte",
            "campaigns": [lista de campaign dicts],
            "date_range": "15/01 — 28/02",
        }
    """
    # 1. Parsear cada campanha
    parsed = []
    unmatched = []
    for c in campaigns:
        result = _parse_campaign_name(c.get("name", ""))
        start = c.get("start_time", "") or c.get("created_time", "")
        start_date = None
        if start:
            try:
                start_date = datetime.fromisoformat(start[:10])
            except Exception:
                pass

        if result:
            event_type, city_key, city_name = result
            parsed.append({
                "campaign": c,
                "event_type": event_type,
                "city_key": city_key,
                "city_name": city_name,
                "start_date": start_date,
            })
        else:
            unmatched.append(c)

    # 2. Agrupar por tipo+cidade
    groups = {}
    for p in parsed:
        key = f"{p['event_type']}_{p['city_key']}"
        if key not in groups:
            groups[key] = []
        groups[key].append(p)

    # 3. Ordenar cada grupo por data e separar por gap temporal
    events = []
    min_valid_date = datetime(2020, 1, 1)
    for key, items in groups.items():
        # Campanhas sem data válida vão para o final (não criam gaps)
        items.sort(key=lambda x: x["start_date"] if x["start_date"] and x["start_date"] > min_valid_date else datetime.max)

        # Comerciais (MTR, PSC, OHIO, CSI, PNL) sao produtos perpetuos — nao
        # separar por gap temporal, sempre 1 unico grupo por produto.
        is_comercial_group = items[0]["event_type"] in COMERCIAL_PRODUCT_MAP
        if is_comercial_group:
            sub_events = [items]
        else:
            # Separar em sub-eventos por gap (ignorar itens sem data válida)
            sub_events = []
            current_group = [items[0]]
            for i in range(1, len(items)):
                prev_date = items[i - 1]["start_date"]
                curr_date = items[i]["start_date"]
                prev_valid = prev_date and prev_date > min_valid_date
                curr_valid = curr_date and curr_date > min_valid_date
                if prev_valid and curr_valid and (curr_date - prev_date).days > gap_days:
                    sub_events.append(current_group)
                    current_group = []
                current_group.append(items[i])
            sub_events.append(current_group)

        for idx, group in enumerate(sub_events):
            if not group:
                continue
            et = group[0]["event_type"]
            city = group[0]["city_name"]
            city_key = group[0]["city_key"]

            # Calcular range de datas (ignorar datas inválidas tipo epoch 0)
            dates = [g["start_date"] for g in group if g["start_date"] and g["start_date"] > min_valid_date]
            if dates:
                min_date = min(dates)
                max_date = max(dates)
                date_range = f"{min_date.strftime('%d/%m')} — {max_date.strftime('%d/%m/%Y')}"
                period_id = min_date.strftime("%Y-%m")
            else:
                date_range = ""
                period_id = "unknown"

            event_type_name = EVENT_TYPE_MAP.get(et, et)

            # RMKT não tem cidade
            if et == "RMKT":
                event_name = "Remarketing Geral"
                event_id = "RMKT"
            elif et in COMERCIAL_PRODUCT_MAP:
                # Comercial: event_name e o proprio nome do produto (ex: "Master Trainer")
                event_type_name = COMERCIAL_PRODUCT_MAP[et]
                suffix = f" ({idx + 1})" if len(sub_events) > 1 else ""
                event_name = f"{event_type_name}{suffix}"
                event_id = f"{et}_{period_id}"
            elif et == "METEORICO":
                # Meteoricos: agrupa todas as campanhas da cidade em 1 evento
                # (nome: "Meteorico — Porto Velho")
                suffix = f" ({idx + 1})" if len(sub_events) > 1 else ""
                event_name = f"Meteorico — {city}{suffix}"
                event_id = f"METEORICO_{city_key}_{period_id}"
            else:
                suffix = f" ({idx + 1})" if len(sub_events) > 1 else ""
                event_name = f"{event_type_name} — {city}{suffix}"
                event_id = f"{et}_{city_key}_{period_id}"

            events.append({
                "event_id": event_id,
                "event_name": event_name,
                "event_type": et,
                "event_type_name": event_type_name,
                "city": city,
                "city_key": city_key,
                "campaign_ids": [g["campaign"]["id"] for g in group],
                "campaign_count": len(group),
                "campaigns": [g["campaign"] for g in group],
                "date_range": date_range,
            })

    # Campanhas não reconhecidas vão num grupo "Outros"
    if unmatched:
        events.append({
            "event_id": "OUTROS",
            "event_name": "Outros",
            "event_type": "OUTROS",
            "event_type_name": "Outros",
            "city": "",
            "city_key": "",
            "campaign_ids": [c["id"] for c in unmatched],
            "campaign_count": len(unmatched),
            "campaigns": unmatched,
            "date_range": "",
        })

    # Ordenar por gasto total (se tiver campo spend)
    events.sort(key=lambda e: sum(c.get("spend", 0) for c in e["campaigns"]), reverse=True)

    return events
