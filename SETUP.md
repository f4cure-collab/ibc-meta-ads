# Guia de Configuração - Meta Ads Testador de Criativos

## Pré-requisitos

- Python 3.9+
- Conta de anúncios no Meta Ads

---

## Passo 1: Criar App no Meta for Developers

1. Acesse **https://developers.facebook.com/**
2. Clique em **"Meus Apps"** > **"Criar App"**
3. Escolha **"Outro"** como tipo de app
4. Escolha **"Empresa"** como tipo
5. Dê um nome (ex: "Testador Criativos") e clique **Criar**

## Passo 2: Adicionar o produto Marketing API

1. No painel do app, vá em **"Adicionar Produto"**
2. Encontre **"Marketing API"** e clique **"Configurar"**

## Passo 3: Gerar Access Token

### Opção A: Token de teste (expira em ~1h, bom para testar)

1. Acesse **https://developers.facebook.com/tools/explorer/**
2. Selecione seu App no dropdown
3. Clique em **"Generate Access Token"**
4. Marque as permissões:
   - `ads_management`
   - `ads_read`
   - `business_management`
5. Clique **"Generate"** e copie o token

### Opção B: Token de longa duração (recomendado para uso contínuo)

1. No painel do app, vá em **Configurações** > **Básico**
2. Copie o **App ID** e **App Secret**
3. Gere um token de curta duração (Opção A acima)
4. Troque por um de longa duração fazendo esta requisição no navegador:

```
https://graph.facebook.com/v21.0/oauth/access_token?
  grant_type=fb_exchange_token&
  client_id=SEU_APP_ID&
  client_secret=SEU_APP_SECRET&
  fb_exchange_token=TOKEN_CURTO
```

5. O token retornado dura ~60 dias

## Passo 4: Encontrar o Ad Account ID

1. Acesse o **Meta Business Suite** (business.facebook.com)
2. Vá em **Configurações** > **Contas** > **Contas de anúncios**
3. O ID estará listado (formato numérico, ex: `123456789`)
4. Para o `.env`, adicione o prefixo `act_`: `act_123456789`

## Passo 5: Configurar o arquivo .env

1. Copie o arquivo de exemplo:
   ```bash
   cp .env.example .env
   ```

2. Edite o `.env` com seus dados:
   ```
   META_APP_ID=seu_app_id
   META_APP_SECRET=seu_app_secret
   META_ACCESS_TOKEN=seu_token_aqui
   META_AD_ACCOUNT_ID=act_123456789
   ```

## Passo 6: Instalar e rodar

```bash
pip install -r requirements.txt
python app.py
```

Acesse **http://localhost:5000** no navegador.

---

## Como usar

### Duplicar manualmente
1. Selecione Campanha > Adset > Anúncio template
2. Veja as configurações que serão copiadas (legenda, URL, CTA, etc)
3. Faça upload do novo vídeo Feed (1:1) e opcionalmente Reels (9:16)
4. Clique "Duplicar Anúncio"
5. O novo anúncio é criado **PAUSADO** no mesmo adset

### Duplicar em lote (CSV)
1. Selecione o anúncio template (mesma seleção)
2. Prepare um CSV com as colunas: `name`, `feed_url`, `reels_url`
3. Faça upload do CSV e clique "Duplicar em Lote"
4. Cada linha do CSV gera um novo anúncio

### Template CSV
```csv
name,feed_url,reels_url
Criativo V1,https://storage.com/video1_feed.mp4,https://storage.com/video1_reels.mp4
Criativo V2,https://storage.com/video2_feed.mp4,https://storage.com/video2_reels.mp4
Criativo V3,https://storage.com/video3_feed.mp4,
```

> **Dica:** Se não tiver vídeo de Reels, deixe a coluna vazia. O sistema usará o vídeo de Feed para todos os placements.

---

## Permissões necessárias na API

| Permissão | Para que serve |
|-----------|---------------|
| `ads_management` | Criar e modificar anúncios |
| `ads_read` | Listar campanhas, adsets, anúncios |
| `business_management` | Acessar conta de anúncios via Business Manager |

---

## Solução de problemas

| Erro | Solução |
|------|---------|
| "Token expirado" | Gere um novo token (Passo 3) |
| "Permissão negada" | Verifique as permissões do token (Passo 3) |
| "Account ID inválido" | Confirme o formato `act_XXXXXXXXX` (Passo 4) |
| "Vídeo não processado" | O vídeo pode estar corrompido ou em formato não suportado. Use MP4 H.264 |
