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
    conn.commit()
    return conn


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
