import re
import psycopg2


def get_existing_indexes(conn, table):
    """Возвращает список существующих индексов для таблицы"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE tablename = %s
        """, (table,))
        return cur.fetchall()

def get_unused_indexes(conn, min_size_mb=1):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                s.schemaname,
                s.relname AS table_name,
                s.indexrelname AS index_name,
                pg_size_pretty(pg_relation_size(s.indexrelid)) AS index_size,
                s.idx_scan
            FROM pg_stat_user_indexes s
            JOIN pg_index i ON s.indexrelid = i.indexrelid
            WHERE s.idx_scan = 0
              AND i.indisunique IS FALSE
              AND pg_relation_size(s.indexrelid) > %s * 1024 * 1024
        """, (min_size_mb,))
        return cur.fetchall()


def extract_columns_from_filter(filter_cond: str):
    """Грубо парсим фильтр и достаем имена колонок для индекса"""
    if not filter_cond:
        return []
    # ищем все вхождения "col = value", "col > value", "col < value"
    cols = re.findall(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s*(=|>|<|>=|<=|LIKE|ILIKE)", filter_cond)
    return list({c[0] for c in cols})  # уникальные колонки

def recommend_indexes(conn, query):
    with conn.cursor() as cur:
        cur.execute(f"EXPLAIN (FORMAT JSON) {query}")
        plan = cur.fetchone()[0][0]["Plan"]

    recommendations = []
    seen_tables = {}

    def walk(node):
        node_type = node.get("Node Type", "")
        relation = node.get("Relation Name")
        filter_cond = node.get("Filter")
        join_cond = node.get("Join Filter")
        sort_key = node.get("Sort Key")

        if relation and relation not in seen_tables:
            seen_tables[relation] = get_existing_indexes(conn, relation)

        # Рекомендации по Seq Scan
        if node_type == "Seq Scan" and relation:
            if filter_cond:
                cols = extract_columns_from_filter(filter_cond)
                if cols:
                    existing = seen_tables[relation]
                    if not any(all(col in idxdef for col in cols) for _, idxdef in existing):
                        idx_name = f"idx_{relation}_{'_'.join(cols)}"
                        recommendations.append({
                            "type": "add_index",
                            "priority": "high",
                            "message": f"Полный Seq Scan таблицы {relation} с фильтром {filter_cond} — "
                                       f"создайте индекс:\nCREATE INDEX {idx_name} ON {relation}({', '.join(cols)});"
                        })
            else:
                recommendations.append({
                    "type": "add_index",
                    "priority": "medium",
                    "message": f"Полный Seq Scan таблицы {relation} без фильтра — возможно, стоит подумать о партиционировании"
                })

        # Рекомендации по JOIN
        if node_type == "Nested Loop" and join_cond:
            cols = extract_columns_from_filter(join_cond)
            if cols:
                recommendations.append({
                    "type": "add_index",
                    "priority": "medium",
                    "message": f"Nested Loop с Join Filter {join_cond} — рассмотрите индекс на {', '.join(cols)}"
                })

        # Рекомендации по ORDER BY
        if sort_key and relation:
            key = ", ".join(sort_key)
            existing = seen_tables[relation]
            if not any(key in idxdef for _, idxdef in existing):
                idx_name = f"idx_{relation}_{'_'.join(sort_key)}"
                recommendations.append({
                    "type": "add_index",
                    "priority": "low",
                    "message": f"Сортировка по {key} — создайте индекс с сортировкой:\n"
                               f"CREATE INDEX {idx_name} ON {relation}({key});"
                })

        for child in node.get("Plans", []):
            walk(child)

    walk(plan)

    # Рекомендации по удалению индексов
    unused = get_unused_indexes(conn)
    for schema, table, idx, scans, size in unused:
        recommendations.append({
            "type": "drop_index",
            "priority": "low",
            "message": f"Индекс {idx} в {schema}.{table} не использовался (idx_scan=0, размер={size}) — "
                       "рассмотрите удаление для экономии места."
        })

    return recommendations


def detect_n_plus_one(conn, calls_threshold=1000, max_mean_time_ms=10, max_rows=10, top_n=20):
    """
    Эвристический поиск N+1 запросов на основе pg_stat_statements.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT query, calls, mean_exec_time, rows
            FROM pg_stat_statements
            WHERE query NOT ILIKE '%pg_stat_statements%'
            ORDER BY calls DESC
            LIMIT %s
        """, (top_n,))
        rows = cur.fetchall()

    suspects = []
    for query, calls, mean_time, rows_count in rows:
        if calls > calls_threshold and mean_time < max_mean_time_ms and rows_count <= max_rows:
            suspects.append({
                "query": query.strip(),
                "calls": calls,
                "mean_exec_time_ms": round(mean_time, 2),
                "rows_per_call": rows_count,
                "recommendation": (
                    "🐢 Похоже на N+1 проблему: запрос вызывается очень часто "
                    f"({calls} раз) и быстрый ({mean_time:.1f} мс), но возвращает мало строк. "
                    "Объедините запросы (JOIN, IN, батч-загрузка)."
                )
            })
    return suspects
