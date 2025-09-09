import psycopg2
import json




def analyze_query(conn, query: str):
    with conn.cursor() as cur:
        cur.execute(f"EXPLAIN (FORMAT JSON) {query}")
        plan = cur.fetchone()[0][0]["Plan"]
    return extract_metrics(plan)

#по плану строит json с характеристиками(cost,width,memory... ) для каждого узла

def extract_metrics(plan, seq_page_cost=1.0, cpu_tuple_cost=0.01):
    node_type = plan.get("Node Type")
    cost = plan.get("Total Cost", 0)
    rows = plan.get("Plan Rows", 0)
    width = plan.get("Plan Width", 0)
    startup_cost = plan.get("Startup Cost", 0)

    cpu_cost = rows * cpu_tuple_cost
    io_cost = max(cost - cpu_cost, 0)
    memory_bytes = rows * width
    runtime_ms = cost  # очень грубая оценка

    recommendations = []

    # Узел-специфичные советы
    if node_type.endswith("Seq Scan") and rows > 100_000:
        recommendations.append(f"📊 Полный скан {rows} строк — добавьте индекс или WHERE")
    if "Parallel" in node_type:
        recommendations.append("⚡ Используется параллельный план — можно увеличить max_parallel_workers_per_gather для ускорения")
    if node_type in ("Hash", "Sort") and memory_bytes > 10 * 1024 * 1024:
        mb = memory_bytes / (1024 * 1024)
        recommendations.append(f"💾 Узел {node_type} обрабатывает ~{mb:.1f}MB — увеличьте work_mem примерно до {int(mb*2)}MB")
    if node_type == "Nested Loop" and rows > 50_000:
        recommendations.append("🔄 Большой Nested Loop — используйте Hash Join или добавьте индекс")
    if cost > 100_000 and node_type in ("Seq Scan", "Bitmap Heap Scan"):
        recommendations.append(f"🧩 Высокая стоимость ({cost:.0f}) — рассмотрите секционирование таблицы")

    children = [extract_metrics(p) for p in plan.get("Plans", [])]

    return {
        "node_type": node_type,
        "cost": cost,
        "cpu_ms": round(cpu_cost, 2),
        "io_ms": round(io_cost, 2),
        "total_ms": round(runtime_ms, 2),
        "rows": rows,
        "width": width,
        "startup_cost": startup_cost,
        "memory_estimate_mb": round(memory_bytes / (1024 * 1024), 2),
        "recommendations": recommendations,
        "children": children
    }


def pretty_plan(node, level=0):
    indent = "  " * level
    node_type = node.get("node_type","")
    rows = node.get("rows", 0)
    width = node.get("width", 0)
    cost = node.get("cost", 0)
    startup_cost = node.get("startup_cost", 0)
    runtime_ms = node.get("total_ms",0)
    io_ms = node.get("io_ms",0)
    cpu_ms = node.get("cpu_ms",0)
    memory_estimate_mb = node.get("memory_estimate_mb")


    print(f"{indent}▶ {node_type}")
    if node.get("recommendations"):
        for rec in node["recommendations"]:
            print(f"{indent}   ⚠️ {rec}")
    print(f"{indent}   rows={rows}, width={width}, cost={startup_cost:.2f}..{cost:.2f}")
    print(f"{indent}  ~{memory_estimate_mb:.2f} MB данных, I/O ≈ {io_ms} ms, CPU ≈ {cpu_ms} ms, всего  ≈ {runtime_ms:.1f} ms,")


    # обходим вложенные планы
    for subplan in node.get("children", []):
        pretty_plan(subplan, level + 1)
        



LOCK_LEVELS = {
    "ACCESS SHARE": {
        "risk": "🟢",
        "description": "Только чтение. Не мешает другим SELECT/INSERT/UPDATE/DELETE. Наименее опасная блокировка."
    },
    "ROW SHARE": {
        "risk": "🟢",
        "description": "Блокирует только операции изменения структуры (DDL), но не мешает другим SELECT/UPDATE."
    },
    "ROW EXCLUSIVE": {
        "risk": "🟡",
        "description": "Ставится при INSERT/UPDATE/DELETE. Конкурирует с другими изменяющими запросами."
    },
    "SHARE UPDATE EXCLUSIVE": {
        "risk": "🟡",
        "description": "Используется при CREATE INDEX CONCURRENTLY, ANALYZE, VACUUM. Не мешает SELECT, но блокирует тяжелые DDL."
    },
    "SHARE": {
        "risk": "🟡",
        "description": "Блокирует изменение структуры таблицы, может конкурировать с другими SHARE/EXCLUSIVE блокировками."
    },
    "SHARE ROW EXCLUSIVE": {
        "risk": "🟠",
        "description": "Более сильная блокировка, чем ROW EXCLUSIVE. Мешает VACUUM FULL и другим DDL."
    },
    "EXCLUSIVE": {
        "risk": "🔴",
        "description": "Блокирует почти все операции, кроме SELECT. Может сильно замедлить работу при долгих запросах."
    },
    "ACCESS EXCLUSIVE": {
        "risk": "🔴",
        "description": "Самая сильная блокировка: блокирует любые операции с таблицей, включая SELECT. Используется при TRUNCATE, ALTER TABLE, DROP."
    },
    "UNKNOWN": {
        "risk": "❓",
        "description": "Не удалось определить уровень блокировки. Проверьте запрос вручную."
    }
}

def detect_lock_level(query: str) -> dict:
    q = " ".join(query.strip().upper().split())
    if q.startswith("SELECT") and "FOR UPDATE" in q:
        return {"level": "ROW EXCLUSIVE", **LOCK_LEVELS["ROW EXCLUSIVE"]}
    if q.startswith("SELECT"):
        return {"level": "ACCESS SHARE", **LOCK_LEVELS["ACCESS SHARE"]}
    if q.startswith("INSERT"):
        return {"level": "ROW EXCLUSIVE", **LOCK_LEVELS["ROW EXCLUSIVE"]}
    if q.startswith("UPDATE"):
        return {"level": "ROW EXCLUSIVE", **LOCK_LEVELS["ROW EXCLUSIVE"]}
    if q.startswith("DELETE"):
        return {"level": "ROW EXCLUSIVE", **LOCK_LEVELS["ROW EXCLUSIVE"]}
    if q.startswith("CREATE INDEX CONCURRENTLY"):
        return {"level": "SHARE UPDATE EXCLUSIVE", **LOCK_LEVELS["SHARE UPDATE EXCLUSIVE"]}
    if q.startswith("CREATE INDEX"):
        return {"level": "ACCESS EXCLUSIVE", **LOCK_LEVELS["ACCESS EXCLUSIVE"]}
    if q.startswith("ALTER"):
        return {"level": "ACCESS EXCLUSIVE", **LOCK_LEVELS["ACCESS EXCLUSIVE"]}
    if q.startswith("DROP"):
        return {"level": "ACCESS EXCLUSIVE", **LOCK_LEVELS["ACCESS EXCLUSIVE"]}
    if q.startswith("TRUNCATE"):
        return {"level": "ACCESS EXCLUSIVE", **LOCK_LEVELS["ACCESS EXCLUSIVE"]}
    return {"level": "UNKNOWN", **LOCK_LEVELS["UNKNOWN"]}

#анализ запросов из pg_stat_statements
def full_pg_analysis(conn, top_n=5,
                     calls_threshold=1000,
                     max_mean_time_ms=10,
                     max_rows=10):
    report = {"detailed_queries": [], "n_plus_one_suspects": []}

    # 1. Проверяем какие колонки есть в pg_stat_statements
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'pg_stat_statements'
        """)
        columns = {row[0] for row in cur.fetchall()}

    total_col = "total_exec_time" if "total_exec_time" in columns else "total_time"
    mean_col = "mean_exec_time" if "mean_exec_time" in columns else "mean_time"

    # --- 1. Топ дорогих запросов
    with conn.cursor() as cur:
        sql = f"""
            SELECT query, calls, {total_col}, {mean_col}, rows,
                   shared_blks_read, shared_blks_hit
            FROM pg_stat_statements
            WHERE query NOT ILIKE '%%pg_stat_statements%%'
            ORDER BY {total_col} DESC
            LIMIT %s
        """
        cur.execute(sql, (top_n,))
        rows = cur.fetchall()

    for query, calls, total_time, mean_time, rows_count, blks_read, blks_hit in rows:
        try:
            plan = analyze_query(conn, query)
        except Exception as e:
            plan = {"error": str(e)}

        recommendations = []
        if calls > calls_threshold and mean_time > 50:
            recommendations.append(f"🔥 Частый и тяжелый запрос: {calls} вызовов, среднее время {mean_time:.1f} мс")
        if blks_read > blks_hit:
            recommendations.append(f"📉 Низкий hit-rate ({blks_hit}/{blks_read}): запрос часто ходит на диск")

        report["detailed_queries"].append({
            "query": query.strip(),
            "calls": calls,
            "total_exec_time_ms": round(total_time, 2),
            "mean_exec_time_ms": round(mean_time, 2),
            "rows": rows_count,
            "blks_read": blks_read,
            "blks_hit": blks_hit,
            "recommendations": recommendations,
            "plan": plan
        })

    # --- 2. Поиск N+1
    with conn.cursor() as cur:
        sql = f"""
            SELECT query, calls, {mean_col}, rows
            FROM pg_stat_statements
            WHERE query NOT ILIKE '%%pg_stat_statements%%'
            ORDER BY calls DESC
            LIMIT %s
        """
        cur.execute(sql, (top_n * 2,))
        rows = cur.fetchall()

    for query, calls, mean_time, rows_count in rows:
        if calls > calls_threshold and mean_time < max_mean_time_ms and rows_count <= max_rows:
            report["n_plus_one_suspects"].append({
                "query": query.strip(),
                "calls": calls,
                "mean_exec_time_ms": round(mean_time, 2),
                "rows_per_call": rows_count,
                "recommendation": (
                    f"🐢 Подозрение на N+1: {calls} вызовов по {mean_time:.1f} мс, "
                    f"{rows_count} строк на вызов. Объедините запросы (JOIN, IN, батч-загрузка)."
                )
            })

    return report


def print_full_pg_report(report):
    print("\n=== 🔍 ДЕТАЛЬНЫЙ АНАЛИЗ ТОП-ЗАПРОСОВ ===")
    for q in report["detailed_queries"]:
        print(f"\n▶ {q['query'][:120]}...")
        print(f"   calls={q['calls']}, total_time={q['total_exec_time_ms']} ms, mean={q['mean_exec_time_ms']} ms")
        print(f"   rows={q['rows']}, blks_read={q['blks_read']}, blks_hit={q['blks_hit']}")
        for r in q["recommendations"]:
            print(f"   ⚠️ {r}")
        if "error" in q["plan"]:
            print(f"   ❌ Не удалось построить план: {q['plan']['error']}")
        else:
            pretty_plan(q["plan"], level=1)  # 

    print("\n=== 🐢 ПОДОЗРЕНИЕ НА N+1 ===")
    if not report["n_plus_one_suspects"]:
        print("✅ N+1 запросов не найдено")
    for s in report["n_plus_one_suspects"]:
        print(f"\n▶ {s['query'][:100]}...")
        print(f"   calls={s['calls']}, mean_time={s['mean_exec_time_ms']} ms, rows/call={s['rows_per_call']}")
        print(f"   ⚠️ {s['recommendation']}")







