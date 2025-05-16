CREATE KEYSPACE IF NOT EXISTS wiki_analytics
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE wiki_analytics;

CREATE TABLE IF NOT EXISTS domain (
    time timestamp,        
    domain text,                 
    created_pages int,           
    created_by_bots int,         
    PRIMARY KEY ((domain), time)
) WITH CLUSTERING ORDER BY (domain ASC);

CREATE TABLE IF NOT EXISTS hour (
    time timestamp,        
    domain text,                 
    created_pages int,           
    created_by_bots int,            
    PRIMARY KEY ((time), domain)
) WITH CLUSTERING ORDER BY (domain ASC);

CREATE TABLE IF NOT EXISTS users (
    user_id int,
    user_name text,
    time timestamp,                 
    created_pages int,                    
    page_titles text,             
    PRIMARY KEY (user_id)
) WITH CLUSTERING ORDER BY (user_id DESC);

CREATE TABLE IF NOT EXISTS page_by_id (
    page_id int,                 
    page_title text,                            
    domain text,                                
    PRIMARY KEY (page_id)                
);

CREATE TABLE hourly_domain_stats (
    time_start timestamp,
    time_end timestamp,
    statistics map<text, int>,
    PRIMARY KEY (time_start, time_end)
);