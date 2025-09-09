import psycopg2
from datetime import datetime, timezone


def analyze_pg_settings(conn, ram_gb=None):
    recommendations = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT name, setting, unit, context
            FROM pg_settings
            WHERE name IN (
                'work_mem',
                'shared_buffers',
                'effective_cache_size',
                'maintenance_work_mem',
                'autovacuum',
                'autovacuum_vacuum_scale_factor',
                'autovacuum_naptime',
                'max_parallel_workers_per_gather'
            )
        """)
        params = {name: (setting, unit, context) for name, setting, unit, context in cur.fetchall()}

    # shared buffers
    if "shared_buffers" in params:
        setting, unit, _ = params["shared_buffers"]
        sb = int(setting)
        sb_bytes = sb * 8 * 1024 if unit == "8kB" else sb
        if ram_gb:
            ram_bytes = ram_gb * (1024**3)
            ratio = sb_bytes / ram_bytes
            if ratio < 0.25:
                recommendations.append({
                    "type": "parameter",
                    "priority": "medium",
                    "message": f"shared_buffers = {round(ratio*100,1)}% RAM — рекомендуется увеличить до 25–40% RAM"
                })

    # work_mem
    if "work_mem" in params:
        setting, unit, _ = params["work_mem"]
        wm_kb = int(setting)
        if wm_kb < 4096:  # <4MB
            recommendations.append({
                "type": "parameter",
                "priority": "medium",
                "message": f"work_mem = {wm_kb/1024:.1f}MB — увеличить (8–64MB) для ускорения сортировок/джоинов"
            })

    # maintenance work mem
    if "maintenance_work_mem" in params:
        setting, unit, _ = params["maintenance_work_mem"]
        mw_kb = int(setting)
        if mw_kb < 65536:  # <64MB
            recommendations.append({
                "type": "parameter",
                "priority": "low",
                "message": f"maintenance_work_mem = {mw_kb/1024:.1f}MB — увеличить (128–512MB) для VACUUM/CREATE INDEX"
            })

    # autovacuum
    if "autovacuum" in params:
        setting, _, _ = params["autovacuum"]
        if setting.lower() in ("off", "false", "0"):
            recommendations.append({
                "type": "parameter",
                "priority": "high",
                "message": "autovacuum выключен — включите, иначе таблицы будут разрастаться"
            })

    return recommendations


def analyze_autovacuum(conn, dead_tup_threshold=0.2, analyze_threshold=0.1, max_autovacuum_age_hours=24):
    results = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT relid, schemaname, relname,
                   n_live_tup, n_dead_tup, n_mod_since_analyze,
                   last_autovacuum, last_analyze
            FROM pg_stat_all_tables
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        """)
        tables = cur.fetchall()

        for relid, schema, table, live, dead, mod_since_analyze, last_auto, last_analyze in tables:
            total = (live or 0) + (dead or 0)
            dead_pct = (dead / total) if total > 0 else 0
            analyze_pct = (mod_since_analyze / total) if total > 0 else 0

            recommendations = []
            now = datetime.now(timezone.utc)

            if dead_pct > dead_tup_threshold:
                recommendations.append(
                    f"⚠️ {round(dead_pct*100, 1)}% dead tuples — запустите VACUUM или уменьшите autovacuum_vacuum_scale_factor"
                )
            if last_auto and (now - last_auto).total_seconds() > max_autovacuum_age_hours * 3600:
                recommendations.append(
                    f"⚠️ Автовакуум не запускался {round((now - last_auto).total_seconds()/3600, 1)} ч — проверьте настройки autovacuum_naptime"
                )
            if analyze_pct > analyze_threshold:
                recommendations.append(
                    f"⚠️ С момента последнего ANALYZE изменено {round(analyze_pct*100, 1)}% строк — выполните ANALYZE"
                )

            # ✅ всегда возвращаем recommendations, даже если пусто
            results.append({
                "schema": schema,
                "table": table,
                "dead_pct": round(dead_pct * 100, 1),
                "analyze_pct": round(analyze_pct * 100, 1),
                "last_autovacuum": last_auto,
                "last_analyze": last_analyze,
                "recommendations": recommendations
            })

    return results


def analyze_table_structure(conn, partition_threshold_mb=10_000):
    """Советы по секционированию и дефрагментации"""
    recommendations = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT relname,
                   pg_total_relation_size(relid) AS total_size
            FROM pg_catalog.pg_statio_user_tables;
        """)
        for table, total_size in cur.fetchall():
            size_mb = total_size / (1024 * 1024)
            if size_mb > partition_threshold_mb:
                recommendations.append({
                    "type": "table",
                    "table": table,
                    "priority": "medium",
                    "message": f"Таблица {table} > {partition_threshold_mb}MB — рассмотрите секционирование по дате или ключу"
                })
    return recommendations

def analyze_table_bloat(conn, bloat_threshold=0.2):
    """
    Анализ фрагментации таблиц (bloat). Возвращает список словарей
    с информацией и рекомендациями по VACUUM FULL / CLUSTER.
    """
    results = []
    with conn.cursor() as cur:
        # Можно использовать pgstattuple, но она не всегда включена
        # Для простоты — оценка через pg_stat_all_tables
        cur.execute("""
            SELECT
                schemaname,
                relname,
                n_live_tup,
                n_dead_tup,
                pg_total_relation_size(relid) AS total_bytes
            FROM pg_stat_all_tables
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        """)
        tables = cur.fetchall()

        for schema, table, live, dead, total_bytes in tables:
            total_rows = (live or 0) + (dead or 0)
            dead_pct = (dead / total_rows) if total_rows > 0 else 0

            recommendations = []
            if dead_pct > bloat_threshold:
                recommendations.append(
                    f"⚠️ Bloat ≈ {round(dead_pct * 100, 1)}% — рассмотрите VACUUM FULL или CLUSTER"
                )

            results.append({
                "schema": schema,
                "table": table,
                "bloat_pct": round(dead_pct * 100, 1),
                "size_mb": round(total_bytes / (1024 * 1024), 2),
                "recommendations": recommendations
            })

    return results

def analyze_index_usage(conn, unused_threshold=0.05):
    """
    Анализ использования индексов.
    Возвращает список словарей с ключами schema, table, index, recommendations.
    """
    results = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                schemaname,
                relname AS table_name,
                indexrelname AS index_name,
                idx_scan,
                idx_tup_read,
                idx_tup_fetch,
                pg_relation_size(indexrelid) AS index_size
            FROM pg_stat_all_indexes
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        """)
        indexes = cur.fetchall()

        for schema, table, index, idx_scan, tup_read, tup_fetch, size in indexes:
            recommendations = []

            if idx_scan == 0:
                recommendations.append("⚠️ Индекс не используется — рассмотрите удаление для экономии места")
            elif idx_scan < unused_threshold * tup_read:
                recommendations.append("⚠️ Индекс редко используется — проверьте, нужен ли он")

            results.append({
                "schema": schema,
                "table": table,
                "index": index,
                "size_mb": round(size / (1024 * 1024), 2),
                "idx_scan": idx_scan,
                "idx_tup_read": tup_read,
                "idx_tup_fetch": tup_fetch,
                "recommendations": recommendations
            })

    return results


def health_check(conn, ram_gb=None, verbose=True):
    results = []

    # 1. Настройки PostgreSQL
    pg_settings = analyze_pg_settings(conn, ram_gb=ram_gb)
    if verbose:
        print("\n⚙️ Настройки PostgreSQL:")
        for r in pg_settings:
            print(f"  • {r['recommendation']}")
    results.extend(pg_settings)

    # 2. Автовакуум
    autovac = analyze_autovacuum(conn)
    if verbose:
        print("\n🧹 Автовакуум и ANALYZE:")
        if not autovac:
            print("   Проблем не найдено")
        for r in autovac:
            print(f"  • {r['schema']}.{r['table']}:")
            for rec in r["recommendations"]:
                print(f"    - {rec}")
    results.extend(autovac)

    # 3. Структура таблиц
    tbl_struct = analyze_table_structure(conn)
    if verbose:
        print("\n📐 Структура таблиц:")
        if not tbl_struct:
            print("   Явных проблем не найдено")
        for r in tbl_struct:
            print(f"  • {r['schema']}.{r['table']}:")
            for rec in r["recommendations"]:
                print(f"    - {rec}")
    results.extend(tbl_struct)

    # 4. Фрагментация таблиц
    tbl_bloat = analyze_table_bloat(conn)
    if verbose:
        print("\n📊 Фрагментация таблиц (bloat):")
        if not tbl_bloat:
            print("  ✅ Значительного bloating не обнаружено")
        for r in tbl_bloat:
            print(f"  • {r['schema']}.{r['table']}: bloat ≈ {r['bloat_pct']}%")
            for rec in r["recommendations"]:
                print(f"    - {rec}")
    results.extend(tbl_bloat)

    # 5. Использование индексов
    idx_usage = analyze_index_usage(conn)
    if verbose:
        print("\n🔎 Использование индексов:")
        if not idx_usage:
            print("   Неиспользуемых индексов не найдено")
        for r in idx_usage:
            print(f"  • {r['schema']}.{r['table']}.{r['index']}:")
            for rec in r["recommendations"]:
                print(f"    - {rec}")
    results.extend(idx_usage)

    return results



if __name__ == "__main__":
    conn = psycopg2.connect(
        host="localhost", dbname="dvdrental", user="postgres", password=""
    )
    try:
        recs = health_check(conn, ram_gb=16)
        for r in recs:
            prefix = f"[{r.get('priority','info').upper()}]"
            print(f"{prefix} {r['message']}")
    finally:
        conn.close()
