#!/bin/bash

docker-compose down
sudo mkdir -p ./dags ./logs ./plugins
sudo chmod -R 777 logs/ plugins/ dags/ airflow.cfg
#echo -e "AIRFLOW_UID=50000\nAIRFLOW_GID=0" > .env
 echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
docker-compose up airflow-init
docker-compose up -d