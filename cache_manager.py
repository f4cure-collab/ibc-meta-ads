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
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), "cache.db")


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
    """Registra uma chamada ao dashboard pra diagnostico."""
    try:
        conn = _get_db()
        conn.execute(
            "INSERT INTO api_usage_log (ts, endpoint, camp_type, meta_calls, cache_hit, duration_ms, user, worst_buc_pct) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (datetime.now().isoformat(), endpoint, camp_type, int(meta_calls), 1 if cache_hit else 0, duration_ms, user, worst_buc_pct)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[USAGE LOG] Erro: {e}")


def clear_old_usage_logs(days=7):
    """Apaga logs de uso com mais de N dias."""
    try:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
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


def get_usage_stats(days=7):
    """Retorna estatisticas agregadas dos ultimos N dias."""
    try:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        conn = _get_db()
        # Top endpoints por chamadas Meta
        by_endpoint = conn.execute("""
            SELECT endpoint,
                   COUNT(*) as hits,
                   SUM(meta_calls) as total_calls,
                   SUM(cache_hit) as cache_hits,
                   AVG(duration_ms) as avg_ms,
                   MAX(duration_ms) as max_ms
            FROM api_usage_log
            WHERE ts >= ?
            GROUP BY endpoint
            ORDER BY total_calls DESC
            LIMIT 30
        """, (cutoff,)).fetchall()

        # Requests mais pesados
        heaviest = conn.execute("""
            SELECT ts, endpoint, camp_type, meta_calls, duration_ms, user, worst_buc_pct
            FROM api_usage_log
            WHERE ts >= ? AND cache_hit = 0
            ORDER BY meta_calls DESC, duration_ms DESC
            LIMIT 20
        """, (cutoff,)).fetchall()

        # Uso por hora do dia (ultimas 24h)
        cutoff_24h = (datetime.now() - timedelta(hours=24)).isoformat()
        hourly = conn.execute("""
            SELECT substr(ts, 1, 13) as hour,
                   COUNT(*) as requests,
                   SUM(meta_calls) as calls
            FROM api_usage_log
            WHERE ts >= ?
            GROUP BY hour
            ORDER BY hour
        """, (cutoff_24h,)).fetchall()

        # Totais
        totals = conn.execute("""
            SELECT COUNT(*) as total_requests,
                   SUM(meta_calls) as total_meta_calls,
                   SUM(cache_hit) as total_cache_hits
            FROM api_usage_log
            WHERE ts >= ?
        """, (cutoff,)).fetchone()

        conn.close()
        return {
            "by_endpoint": [dict(zip([d[0] for d in [("endpoint",), ("hits",), ("total_calls",), ("cache_hits",), ("avg_ms",), ("max_ms",)]], r)) for r in by_endpoint],
            "heaviest": [dict(zip([d[0] for d in [("ts",), ("endpoint",), ("camp_type",), ("meta_calls",), ("duration_ms",), ("user",), ("worst_buc_pct",)]], r)) for r in heaviest],
            "hourly": [dict(zip(["hour", "requests", "calls"], r)) for r in hourly],
            "totals": dict(zip(["total_requests", "total_meta_calls", "total_cache_hits"], totals or (0, 0, 0))),
            "period_days": days,
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
    """Thread que roda às 2h todo dia para atualizar o cache."""
    global _scheduler_running
    _scheduler_running = True
    print("[SCHEDULER] Iniciado — atualização diária às 2:00")

    while _scheduler_running:
        now = datetime.now()
        # Próxima execução às 2:00
        target = now.replace(hour=2, minute=0, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)

        wait_seconds = (target - now).total_seconds()
        print(f"[SCHEDULER] Próxima atualização em {wait_seconds/3600:.1f}h ({target.strftime('%Y-%m-%d %H:%M')})")

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
