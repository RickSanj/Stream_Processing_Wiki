CREATE KEYSPACE IF NOT EXISTS wiki_analytics
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE wiki_analytics;

CREATE TABLE IF NOT EXISTS domain (
    time timestamp,        
    domain text,                 
    page_id int,           
    user_is_bot boolean,         
    PRIMARY KEY ((domain), time)
);

DROP TABLE IF EXISTS hour;

CREATE TABLE hour (
    time timestamp,        
    domain text,                 
    page_id int,           
    user_is_bot boolean,               
    PRIMARY KEY ((time), domain)
);

CREATE TABLE IF NOT EXISTS users (
    user_id int,
    user_name text,
    time timestamp,                 
    page_id int,                   
    page_title text,             
    PRIMARY KEY (user_id)
);

CREATE TABLE IF NOT EXISTS page_by_id (
    domain text,
    page_id int,
    page_title text,
    PRIMARY KEY (domain, page_id)
);

CREATE TABLE IF NOT EXISTS domains (
    domain text PRIMARY KEY
);

DROP TABLE IF EXISTS hourly_domain_stats;

CREATE TABLE hourly_domain_stats (
    time_start timestamp,
    time_end timestamp,
    bots_only boolean,
    statistics map<text, int>,
    PRIMARY KEY ((time_start, time_end), bots_only)
);
