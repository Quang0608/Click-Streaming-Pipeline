CREATE DATABASE IF NOT EXISTS jobsdb;
USE jobsdb;

CREATE TABLE IF NOT EXISTS job_table (
  job_id INT,
  group_id INT,
  campaign_id INT,
  company_id INT
);
