#!/bin/bash

docker-compose down
sudo mkdir -p ./dags ./logs ./plugins ./project ./tmp ./postgres-db-volume
sudo chmod -R 777 ./dags ./logs ./plugins ./project ./tmp ./postgres-db-volume airflow.cfg
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
docker-compose up airflow-init
docker-compose up -d
chmod -R 667 /var/run/docker.sock
cd DI
docker build -t airflow_task_python_3.8 .
