#!/bin/bash
echo "Importing job_table CSV..."

mysql --local-infile=1 -u root -proot jobsdb <<EOF
LOAD DATA LOCAL INFILE '/docker-entrypoint-initdb.d/02-job-table.csv'
INTO TABLE job_table
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(job_id, group_id, campaign_id, company_id);
EOF

echo "CSV Import completed!"
