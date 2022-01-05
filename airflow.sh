#!/bin/bash

sudo mkdir -p ./dags ./logs ./plugins
sudo chmod -R 777 logs/ plugins/ dags/ airflow.cfg
echo -e "AIRFLOW_UID=$(id -u) \nAIRFLOW_GID=0" > .env
docker-compose up airflow-init
docker-compose up -d