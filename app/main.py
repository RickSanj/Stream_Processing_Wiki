from fastapi import FastAPI
from cassandra.cluster import Cluster
from datetime import datetime, timedelta
from collections import defaultdict
import time
from fastapi import Query


app = FastAPI()

CASSANDRA_NODES = ['cassandra']
CASSANDRA_KEYSPACE = 'wiki_analytics'

cluster = Cluster(CASSANDRA_NODES)
session = cluster.connect()
session.set_keyspace(CASSANDRA_KEYSPACE)

def round_to_hour(dt):
    return dt.replace(minute=0, second=0, microsecond=0)

@app.get("/domain_statistics/")
def get_domain_statistics():
    now = round_to_hour(datetime.utcnow())
    stats = []

    for i in range(6, 0, -1):
        start_time = now - timedelta(hours=i)
        end_time = start_time + timedelta(hours=1)

        print(f"🔍 Querying stats from {start_time} to {end_time}")

        query = """
            SELECT statistics FROM hourly_domain_stats
            WHERE time_start = %s AND time_end = %s AND bots_only = false
        """
        rows = session.execute(query, (start_time, end_time))
        domain_counts = []
        for row in rows:
            stats_dict = row.statistics
            for domain, count in stats_dict.items():
                domain_counts.append({domain: count})

        stats.append({
            "time_start": start_time.strftime("%H:%M"),
            "time_end": end_time.strftime("%H:%M"),
            "statistics": domain_counts
        })

    return stats

@app.get("/pages_statistics/")
def get_pages_statistics():
    now = round_to_hour(datetime.utcnow())
    start_time = now - timedelta(hours=6)
    end_time = now - timedelta(hours=1)

    combined_stats = defaultdict(int)

    for i in range(6, 1, -1):
        hour_start = now - timedelta(hours=i)
        hour_end = hour_start + timedelta(hours=1)

        print(f"🔍 Querying bot stats from {hour_start} to {hour_end}")

        query = """
            SELECT statistics FROM hourly_domain_stats
            WHERE time_start = %s AND time_end = %s AND bots_only = true
        """
        rows = session.execute(query, (hour_start, hour_end))
        for row in rows:
            for domain, count in row.statistics.items():
                combined_stats[domain] += count

    return {
        "time_start": start_time.strftime("%H:%M"),
        "time_end": end_time.strftime("%H:%M"),
        "statistics": [
            {"domain": domain, "created_by_bots": count}
            for domain, count in combined_stats.items()
        ]
    }

@app.get("/top_users/")
def get_top_users():
    now = datetime.utcnow()
    six_hours_ago = now - timedelta(hours=6)
    one_hour_ago = now - timedelta(hours=1)

    print(f"🔍 Querying users from {six_hours_ago} to {one_hour_ago}")

    query = """
        SELECT user_id, user_name, time, page_id, page_title FROM users
    """
    rows = session.execute(query)

    user_data = defaultdict(lambda: {"user_name": "", "pages": []})

    for row in rows:
        if six_hours_ago <= row.time < one_hour_ago:
            user_data[row.user_id]["user_name"] = row.user_name
            user_data[row.user_id]["pages"].append(row.page_title)

    top_users = sorted(
        user_data.items(),
        key=lambda item: len(item[1]["pages"]),
        reverse=True
    )[:20]

    return [
        {
            "user_id": user_id,
            "user_name": data["user_name"],
            "time_start": six_hours_ago.strftime("%H:%M"),
            "time_end": one_hour_ago.strftime("%H:%M"),
            "pages_created": data["pages"],
            "count": len(data["pages"])
        }
        for user_id, data in top_users
    ]


@app.get("/domains/")
def get_existing_domains():
    query = "SELECT DISTINCT domain FROM page_by_id ALLOW FILTERING"
    rows = session.execute(query)
    domains = [row.domain for row in rows]
    return {"domains": domains}


# 2) Return all the pages which were created by the user with a specified user_id.
@app.get("/pages/by_user/{user_id}")
def get_pages_by_user(user_id: int):
    query = "SELECT page_id, page_title, time FROM users WHERE user_id = %s ALLOW FILTERING"
    rows = session.execute(query, (user_id,))
    pages = [{"page_id": row.page_id, "page_title": row.page_title, "time": row.time} for row in rows]
    return {"user_id": user_id, "pages": pages}


# 3) Return the number of articles created for a specified domain.
@app.get("/articles/count/")
def get_article_count(domain: str = Query(...)):
    query = "SELECT COUNT(*) FROM page_by_id WHERE domain = %s ALLOW FILTERING"
    rows = session.execute(query, (domain,))
    count = list(rows)[0].count if rows else 0
    return {"domain": domain, "article_count": count}


# 4) Return the page with the specified page_id.
@app.get("/pages/{page_id}")
def get_page_by_id(page_id: int):
    query = "SELECT page_id, page_title, domain FROM page_by_id WHERE page_id = %s"
    rows = session.execute(query, (page_id,))
    page = list(rows)
    if page:
        return dict(page[0]._asdict())
    else:
        return {"error": "Page not found"}


# 5) Return the id, name, and number of created pages of all the users who created at least one page in a specified time range.
@app.get("/users/created_pages/")
def get_users_by_time_range(start: str, end: str):
    start_time = datetime.fromisoformat(start)
    end_time = datetime.fromisoformat(end)

    query = "SELECT user_id, user_name, time, page_id FROM users"
    rows = session.execute(query)

    result = defaultdict(lambda: {"user_name": "", "count": 0})

    for row in rows:
        if start_time <= row.time <= end_time:
            result[row.user_id]["user_name"] = row.user_name
            result[row.user_id]["count"] += 1

    return [
        {"user_id": user_id, "user_name": data["user_name"], "page_count": data["count"]}
        for user_id, data in result.items() if data["count"] > 0
    ]
