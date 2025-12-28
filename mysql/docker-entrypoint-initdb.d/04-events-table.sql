CREATE DATABASE IF NOT EXISTS my_sql;
USE my_sql;

CREATE TABLE IF NOT EXISTS events (
    id INT AUTO_INCREMENT PRIMARY KEY,
    job_id VARCHAR(50),
    dates DATE,
    hours INT,
    publisher_id VARCHAR(50),
    campaign_id VARCHAR(50),
    group_id VARCHAR(50),
    bid_set DOUBLE,
    spend_hour DOUBLE,
    clicks BIGINT,
    conversion BIGINT,
    qualified_application BIGINT,
    disqualified_application BIGINT,
    company_id INT,
    updated_at DATETIME,
    sources VARCHAR(255),
    UNIQUE KEY unique_event (job_id, dates, hours, publisher_id, campaign_id, group_id, company_id,sources)
);