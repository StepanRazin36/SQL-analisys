#!/usr/bin/env python3
import argparse
import psycopg2
import json
import sys
from info_about_query import analyze_query, pretty_plan, full_pg_analysis
from rec_db import health_check
from rec_query import recommend_indexes


def connect_db(dsn):
    try:
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f"❌ Ошибка подключения к БД: {e}")
        sys.exit(1)

def check_pg_stat_statements_enabled(conn):
    """Проверяет, доступно ли pg_stat_statements в текущей БД"""
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT extname FROM pg_extension WHERE extname='pg_stat_statements'")
            if cur.fetchone():
                return True
            return False
        except Exception:
            return False


def main():
    parser = argparse.ArgumentParser(description="PostgreSQL Query & DB Analyzer")
    parser.add_argument("--dsn", type=str, required=True,
                        help="DSN для подключения к БД, пример: 'host=localhost dbname=test user=postgres password=123'")
    parser.add_argument("--query", type=str, help="SQL-запрос для анализа")
    parser.add_argument("--file", type=str, help="SQL-файл для анализа")
    parser.add_argument("--analyze-db", action="store_true", help="Анализ параметров БД и bloating")
    parser.add_argument("--pgstat", action="store_true", help="Анализ pg_stat_statements (топ-10 медленных запросов)")
    args = parser.parse_args()

    conn = connect_db(args.dsn)

    if args.analyze_db:
        print("\n Анализ параметров БД:")
        health_check(conn)

    if args.query or args.file:
        query = args.query
        if args.file:
            with open(args.file, "r") as f:
                query = f.read()

        print("\n План выполнения запроса (EXPLAIN):")
        plan = analyze_query(conn, query)
        pretty_plan(plan)

        print("\n Рекомендации по индексам:")
        recs = recommend_indexes(conn, query)
        if recs:
            for r in recs:
                print(f"   ⚠️ {r}")
        else:
            print("   Проблем не найдено.")

    if args.pgstat:
        stats = full_pg_analysis(conn)

        print("\n📈 Анализ pg_stat_statements:")

        print("\n🔥 Топ медленных запросов:")
        for q in stats["detailed_queries"]:
            print(f"  • {q['query'][:80]}...")
            print(f"    ⏱ вызовы: {q['calls']}, среднее время: {q['mean_exec_time_ms']} мс, "
                f"строк: {q['rows']}, hit-rate: {q['blks_hit']}/{q['blks_read']}")
            if q["recommendations"]:
                for rec in q["recommendations"]:
                    print(f"    ⚠️ {rec}")

        print("\n🐢 Подозрения на N+1:")
        for q in stats["n_plus_one_suspects"]:
            print(f"  • {q['query'][:80]}...")
            print(f"    {q['recommendation']}")




if __name__ == "__main__":
    main()
