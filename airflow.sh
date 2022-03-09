#!/bin/bash

docker-compose down
sudo mkdir -p ./dags ./logs ./plugins ./project ./tmp
sudo chmod -R 777 ./dags ./logs ./plugins ./project ./tmp airflow.cfg
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
docker-compose up airflow-init
docker-compose up -d
cd DI
docker build -t airflow_task_python_3.8 .