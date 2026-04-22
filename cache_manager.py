"""
Cache persistente para dados da Meta API.
Usa SQLite para armazenar respostas da API e evitar chamadas desnecessárias.
Inclui scheduler para atualizar automaticamente às 2h da manhã.
"""

import os
import json
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone

DB_PATH = os.path.join(os.path.dirname(__file__), "cache.db")

# Fuso BR pra timestamps de log. Servidor em producao roda em UTC mas
# o usuario ve em Sao Paulo. Armazenamos ja convertido pra evitar offset
# na UI (simples e consistente em toda a app).
try:
    from zoneinfo import ZoneInfo
    _BR_TZ = ZoneInfo("America/Sao_Paulo")
except Exception:
    _BR_TZ = timezone(timedelta(hours=-3))  # fallback sem tzdata


def _now_br_iso():
    """ISO timestamp no fuso BR — usado em logs pra bater com horario local."""
    return datetime.now(_BR_TZ).replace(tzinfo=None).isoformat(timespec="seconds")


def _get_db():
    """Retorna conexão SQLite (cria tabelas se não existem)."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS api_cache (
            cache_key TEXT PRIMARY KEY,
            data TEXT NOT NULL,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scheduler_lock (
            name TEXT PRIMARY KEY,
            pid INTEGER,
            acquired_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS api_usage_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            camp_type TEXT,
            meta_calls INTEGER DEFAULT 0,
            cache_hit INTEGER DEFAULT 0,
            duration_ms INTEGER,
            user TEXT,
            worst_buc_pct INTEGER
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_api_usage_ts ON api_usage_log(ts)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_api_usage_endpoint ON api_usage_log(endpoint)")
    conn.commit()
    return conn


def log_api_usage(endpoint, camp_type=None, meta_calls=0, cache_hit=False, duration_ms=None, user=None, worst_buc_pct=None):
    """Registra uma chamada ao dashboard pra diagnostico. Timestamp em fuso BR."""
    try:
        conn = _get_db()
        conn.execute(
            "INSERT INTO api_usage_log (ts, endpoint, camp_type, meta_calls, cache_hit, duration_ms, user, worst_buc_pct) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (_now_br_iso(), endpoint, camp_type, int(meta_calls), 1 if cache_hit else 0, duration_ms, user, worst_buc_pct)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[USAGE LOG] Erro: {e}")


def get_api_calls_for_user(user_email, days=7, limit=500):
    """Retorna chamadas de API de um usuario especifico nos ultimos N dias.
    Ordenado do mais recente pro mais antigo. Exclui calls automaticas."""
    try:
        cutoff = (datetime.now(_BR_TZ).replace(tzinfo=None) - timedelta(days=days)).isoformat()
        conn = _get_db()
        rows = conn.execute("""
            SELECT ts, endpoint, camp_type, meta_calls, cache_hit, duration_ms, worst_buc_pct
            FROM api_usage_log
            WHERE user = ? AND ts >= ?
            ORDER BY ts DESC
            LIMIT ?
        """, (user_email, cutoff, limit)).fetchall()
        conn.close()
        return [dict(zip(["ts", "endpoint", "camp_type", "meta_calls", "cache_hit", "duration_ms", "worst_buc_pct"], r)) for r in rows]
    except Exception as e:
        print(f"[USAGE LOG] Erro get_api_calls_for_user: {e}")
        return []


def clear_old_usage_logs(days=7):
    """Apaga logs de uso com mais de N dias (comparacao em fuso BR)."""
    try:
        cutoff = (datetime.now(_BR_TZ).replace(tzinfo=None) - timedelta(days=days)).isoformat()
        conn = _get_db()
        result = conn.execute("DELETE FROM api_usage_log WHERE ts < ?", (cutoff,))
        rows_deleted = result.rowcount
        conn.commit()
        conn.close()
        if rows_deleted > 0:
            print(f"[USAGE LOG] Limpei {rows_deleted} registros > {days}d")
        return rows_deleted
    except Exception as e:
        print(f"[USAGE LOG] Erro no cleanup: {e}")
        return 0


def get_usage_stats(days=7, source="all", user_filter="", from_ts=""):
    """Retorna estatisticas agregadas dos ultimos N dias.

    Args:
        days: janela em dias (1-30)
        source: 'all' | 'user' (apenas humanos) | 'auto' (apenas scheduler)
        user_filter: substring pra filtrar por email do usuario
        from_ts: ISO timestamp de inicio (opcional, sobrescreve days).
                 Usado pra debug com janela curta (ex: ver se o scheduler
                 rodou entre 02:00-06:00 BRT hoje).
    """
    try:
        if from_ts:
            cutoff = from_ts
        else:
            cutoff = (datetime.now(_BR_TZ).replace(tzinfo=None) - timedelta(days=days)).isoformat()
        # Monta clausulas WHERE extras baseado nos filtros
        extra_where = []
        extra_params = []
        if source == "user":
            extra_where.append("(user IS NULL OR user NOT LIKE 'auto:%')")
        elif source == "auto":
            extra_where.append("user LIKE 'auto:%'")
        if user_filter:
            extra_where.append("user LIKE ?")
            extra_params.append("%" + user_filter + "%")
        where_extra = (" AND " + " AND ".join(extra_where)) if extra_where else ""

        conn = _get_db()
        # Top endpoints por chamadas Meta
        by_endpoint = conn.execute(f"""
            SELECT endpoint,
                   COUNT(*) as hits,
                   SUM(meta_calls) as total_calls,
                   SUM(cache_hit) as cache_hits,
                   AVG(duration_ms) as avg_ms,
                   MAX(duration_ms) as max_ms
            FROM api_usage_log
            WHERE ts >= ?{where_extra}
            GROUP BY endpoint
            ORDER BY total_calls DESC
            LIMIT 30
        """, (cutoff, *extra_params)).fetchall()

        # Requests mais pesados
        heaviest = conn.execute(f"""
            SELECT ts, endpoint, camp_type, meta_calls, duration_ms, user, worst_buc_pct
            FROM api_usage_log
            WHERE ts >= ? AND cache_hit = 0{where_extra}
            ORDER BY meta_calls DESC, duration_ms DESC
            LIMIT 30
        """, (cutoff, *extra_params)).fetchall()

        # Top usuarios (humanos) por chamadas
        by_user = conn.execute(f"""
            SELECT COALESCE(user, '(anonimo)') as user,
                   COUNT(*) as hits,
                   SUM(meta_calls) as total_calls,
                   SUM(cache_hit) as cache_hits
            FROM api_usage_log
            WHERE ts >= ?{where_extra}
            GROUP BY user
            ORDER BY total_calls DESC
            LIMIT 20
        """, (cutoff, *extra_params)).fetchall()

        # Totais
        totals = conn.execute(f"""
            SELECT COUNT(*) as total_requests,
                   SUM(meta_calls) as total_meta_calls,
                   SUM(cache_hit) as total_cache_hits
            FROM api_usage_log
            WHERE ts >= ?{where_extra}
        """, (cutoff, *extra_params)).fetchone()

        # Atividade recente (timeline crono descendente) — usado pra verificar
        # se o scheduler rodou em horarios especificos (ex: 02:00 BRT).
        # Limit alto (2000) pra cobrir janelas com muito trafico. Se o usuario
        # usar from_ts com janela curta (ex: 01:00-06:00), pega tudo.
        recent = conn.execute(f"""
            SELECT ts, endpoint, camp_type, meta_calls, duration_ms, cache_hit, user, worst_buc_pct
            FROM api_usage_log
            WHERE ts >= ?{where_extra}
            ORDER BY ts DESC
            LIMIT 2000
        """, (cutoff, *extra_params)).fetchall()

        conn.close()
        return {
            "by_endpoint": [dict(zip(["endpoint", "hits", "total_calls", "cache_hits", "avg_ms", "max_ms"], r)) for r in by_endpoint],
            "heaviest": [dict(zip(["ts", "endpoint", "camp_type", "meta_calls", "duration_ms", "user", "worst_buc_pct"], r)) for r in heaviest],
            "by_user": [dict(zip(["user", "hits", "total_calls", "cache_hits"], r)) for r in by_user],
            "recent": [dict(zip(["ts", "endpoint", "camp_type", "meta_calls", "duration_ms", "cache_hit", "user", "worst_buc_pct"], r)) for r in recent],
            "totals": dict(zip(["total_requests", "total_meta_calls", "total_cache_hits"], totals or (0, 0, 0))),
            "period_days": days,
            "filter_source": source,
            "filter_user": user_filter,
        }
    except Exception as e:
        print(f"[USAGE LOG] Erro stats: {e}")
        return {"error": str(e)}


def try_acquire_scheduler_lock(name, max_age_hours=0.25):
    """Tenta obter lock exclusivo para rodar scheduler em apenas um worker.
    Em gunicorn multi-worker, N processos tentam iniciar o scheduler simultaneamente;
    o lock garante que so um rode de fato. Se o lock anterior e mais velho que
    max_age_hours, assume-se que o worker morreu e toma o lock."""
    import os
    try:
        conn = _get_db()
        row = conn.execute("SELECT pid, acquired_at FROM scheduler_lock WHERE name=?", (name,)).fetchone()
        now = datetime.now()
        if row:
            try:
                prev = datetime.fromisoformat(row[1])
                age_h = (now - prev).total_seconds() / 3600
                if age_h < max_age_hours:
                    conn.close()
                    return False
            except Exception:
                pass
        conn.execute(
            "INSERT OR REPLACE INTO scheduler_lock (name, pid, acquired_at) VALUES (?, ?, ?)",
            (name, os.getpid(), now.isoformat())
        )
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"[LOCK] Erro: {e}")
        return False


def refresh_scheduler_lock(name):
    """Heartbeat do lock — chamado periodicamente pelo worker que detem o lock
    pra manter o timestamp atualizado e evitar que outro worker tome."""
    import os
    try:
        conn = _get_db()
        conn.execute(
            "INSERT OR REPLACE INTO scheduler_lock (name, pid, acquired_at) VALUES (?, ?, ?)",
            (name, os.getpid(), datetime.now().isoformat())
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[LOCK] Erro refresh: {e}")


def get_cached(cache_key):
    """Retorna dados do cache se existir e não estiver expirado. None se não."""
    try:
        conn = _get_db()
        row = conn.execute(
            "SELECT data, expires_at FROM api_cache WHERE cache_key = ?",
            (cache_key,)
        ).fetchone()
        conn.close()
        if not row:
            return None
        data_str, expires_at = row
        if datetime.now().isoformat() > expires_at:
            return None
        return json.loads(data_str)
    except Exception as e:
        print(f"[CACHE] Erro ao ler: {e}")
        return None


def set_cached(cache_key, data, ttl_hours=20):
    """Salva dados no cache com TTL em horas."""
    try:
        conn = _get_db()
        now = datetime.now()
        expires = now + timedelta(hours=ttl_hours)
        conn.execute(
            "INSERT OR REPLACE INTO api_cache (cache_key, data, created_at, expires_at) VALUES (?, ?, ?, ?)",
            (cache_key, json.dumps(data), now.isoformat(), expires.isoformat())
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[CACHE] Erro ao salvar: {e}")


def should_refresh(cache_key, min_ttl_ratio=0.4):
    """Decide se vale a pena refrescar uma entrada de cache.

    Retorna True se:
    - Entrada nao existe (nunca foi populada)
    - Ja expirou
    - Tem menos que min_ttl_ratio do TTL original restante

    Exemplo: TTL de 1h, min_ttl_ratio=0.4 -> refresh quando sobra <24min.
    Evita que o refresh loop gaste API com entradas que ainda estao frescas."""
    try:
        conn = _get_db()
        row = conn.execute(
            "SELECT created_at, expires_at FROM api_cache WHERE cache_key = ?",
            (cache_key,)
        ).fetchone()
        conn.close()
        if not row:
            return True
        created_at = datetime.fromisoformat(row[0])
        expires_at = datetime.fromisoformat(row[1])
        now = datetime.now()
        if now >= expires_at:
            return True
        original_ttl = (expires_at - created_at).total_seconds()
        remaining = (expires_at - now).total_seconds()
        return remaining < original_ttl * min_ttl_ratio
    except Exception:
        return True


def clear_cache():
    """Limpa todo o cache."""
    try:
        conn = _get_db()
        conn.execute("DELETE FROM api_cache")
        conn.commit()
        conn.close()
        print("[CACHE] Cache limpo")
    except Exception as e:
        print(f"[CACHE] Erro ao limpar: {e}")


def clear_expired():
    """Remove entradas expiradas."""
    try:
        conn = _get_db()
        conn.execute("DELETE FROM api_cache WHERE expires_at < ?", (datetime.now().isoformat(),))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[CACHE] Erro ao limpar expirados: {e}")


def cache_stats():
    """Retorna estatísticas do cache."""
    try:
        conn = _get_db()
        total = conn.execute("SELECT COUNT(*) FROM api_cache").fetchone()[0]
        valid = conn.execute(
            "SELECT COUNT(*) FROM api_cache WHERE expires_at > ?",
            (datetime.now().isoformat(),)
        ).fetchone()[0]
        conn.close()
        return {"total_entries": total, "valid_entries": valid, "expired": total - valid}
    except Exception:
        return {"total_entries": 0, "valid_entries": 0, "expired": 0}


# ── Scheduler: atualiza dados às 2h da manhã ──

_scheduler_running = False


def _run_daily_update(app_context_func):
    """Thread que roda às 2h (horario de Sao Paulo) todo dia para atualizar o cache.

    Importante: o agendamento e feito em BR timezone porque o servidor
    em producao roda em UTC — se usassemos datetime.now() sem fuso, o
    "2am" virava 23h BRT do dia anterior, e as caches eram populadas com
    dt_to=ontem-1 (desalinhado do que o usuario pede de manha seguinte)."""
    global _scheduler_running
    _scheduler_running = True
    print("[SCHEDULER] Iniciado — atualizacao diaria as 2:00 (America/Sao_Paulo)")

    while _scheduler_running:
        now_br = datetime.now(_BR_TZ)
        # Proxima execucao as 2:00 BRT
        target = now_br.replace(hour=2, minute=0, second=0, microsecond=0)
        if now_br >= target:
            target += timedelta(days=1)

        wait_seconds = (target - now_br).total_seconds()
        print(f"[SCHEDULER] Proxima atualizacao em {wait_seconds/3600:.1f}h ({target.strftime('%Y-%m-%d %H:%M %Z')})")

        # Dormir em intervalos curtos para poder parar
        for _ in range(int(wait_seconds)):
            if not _scheduler_running:
                return
            time.sleep(1)

        # Hora de atualizar
        print(f"[SCHEDULER] Iniciando atualização automática — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        try:
            # Limpar cache expirado
            clear_expired()
            # Chamar a função de atualização passada pelo app
            if app_context_func:
                app_context_func()
            print(f"[SCHEDULER] Atualização concluída — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            print(f"[SCHEDULER] Erro na atualização: {e}")


def start_scheduler(app_context_func=None):
    """Inicia o scheduler em background."""
    global _scheduler_running
    if _scheduler_running:
        return
    t = threading.Thread(target=_run_daily_update, args=(app_context_func,), daemon=True)
    t.start()


def stop_scheduler():
    """Para o scheduler."""
    global _scheduler_running
    _scheduler_running = False
