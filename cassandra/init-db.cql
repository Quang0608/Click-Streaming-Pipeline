-- Use your keyspace
CREATE KEYSPACE IF NOT EXISTS my_keyspace
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE my_keyspace;

-- Create tracking table
CREATE TABLE tracking_new (
    event_date date,
    ts timestamp,
    create_time text,
    bid text,
    bn text,
    campaign_id text,
    cd text,
    custom_track text,
    de text,
    dl text,
    dt text,
    ev text,
    group_id text,
    id text,
    job_id text,
    md text,
    publisher_id text,
    rl text,
    sr text,
    tz text,
    ua text,
    uid text,
    utm_campaign text,
    utm_content text,
    utm_medium text,
    utm_source text,
    utm_term text,
    v text,
    vp text,
    PRIMARY KEY (event_date, ts, job_id)
)