from fastapi import FastAPI
from cassandra.cluster import Cluster
from datetime import datetime, timedelta
from collections import defaultdict
import time
from fastapi import Query
from fastapi.responses import JSONResponse


app = FastAPI()

CASSANDRA_NODES = ['cassandra']
CASSANDRA_KEYSPACE = 'wiki_analytics'

cluster = Cluster(CASSANDRA_NODES)
session = cluster.connect()
session.set_keyspace(CASSANDRA_KEYSPACE)



@app.get("/domain_statistics/")
def domain_statistics():
    now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)

    stats = []

    for i in range(6, 0, -1):
        start = now - timedelta(hours=i)
        end = start + timedelta(hours=1)

        rows = session.execute("""
            SELECT statistics FROM hourly_domain_stats
            WHERE time_start=%s AND time_end=%s AND bots_only=False
        """, (start, end))

        row = next(iter(rows), None)

        if row and row.statistics:
            formatted_stats = [{domain: count} for domain, count in row.statistics.items()]
            stats.append({
                "time_start": start.strftime("%H:%M"),
                "time_end": end.strftime("%H:%M"),
                "statistics": formatted_stats
            })

    return stats

@app.get("/pages_statistics/")
def pages_created_by_bots():
    now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    total_statistics = defaultdict(int)

    for i in range(6, 0, -1):
        start = now - timedelta(hours=i)
        end = start + timedelta(hours=1)

        rows = session.execute("""
            SELECT statistics FROM hourly_domain_stats
            WHERE time_start=%s AND time_end=%s AND bots_only=True
        """, (start, end))

        row = next(iter(rows), None)

        if row and row.statistics:
            for domain, count in row.statistics.items():
                total_statistics[domain] += count

    return {
        "time_start": (now - timedelta(hours=7)).replace(minute=0, second=0, microsecond=0).strftime("%H:%M"),
        "time_end": (now - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0).strftime("%H:%M"),
        "statistics": [
            {"domain": domain, "created_by_bots": count}
            for domain, count in total_statistics.items()
        ]
    }


   

@app.get("/top_users/")
def top_users():
    now = datetime.utcnow()
    start_time = (now - timedelta(hours=7)).replace(minute=0, second=0, microsecond=0)
    end_time = (now - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)

    rows = session.execute("SELECT user_id, user_name, time, page_title FROM users")

    users = defaultdict(lambda: {
        "user_name": "",
        "page_titles": [],
        "page_count": 0
    })

    for row in rows:
        if start_time <= row.time <= end_time:
            users[row.user_id]["user_name"] = row.user_name
            users[row.user_id]["page_titles"].append(row.page_title)
            users[row.user_id]["page_count"] += 1

    top_users_list = sorted(users.items(), key=lambda item: item[1]["page_count"], reverse=True)[:20]

    return [{
        "user_id": user_id,
        "user_name": data["user_name"],
        "time_start": start_time.strftime("%H:%M"),
        "time_end": end_time.strftime("%H:%M"),
        "page_titles": data["page_titles"],
        "page_count": data["page_count"]
    } for user_id, data in top_users_list]



@app.get("/domains/")
def get_existing_domains():
    rows = session.execute("SELECT domain FROM domains")
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
