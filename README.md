# job-streaming-pipeline

Full Kafka -> Cassandra -> Spark -> Airflow -> MySQL demo repo.

## Quick start (development)
1. Build & start services:
   ```bash
   docker compose up --build

# Tracking ETL Project

## Overview
This project ingests Kafka tracking events, writes them to Cassandra, and aggregates them into MySQL.

## Requirements
- Python 3.10+
- Apache Spark 3.5+
- Cassandra 4.x
- Kafka 3.x
- MySQL 8.x
- Airflow (for DAG)

## Setup
1. Copy `.env.example` to `.env` and update credentials.
2. Install Python dependencies:
   ```bash
   pip install pyspark kafka-python mysql-connector-python python-dotenv
