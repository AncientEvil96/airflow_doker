#!/bin/bash

#docker-compose down
#sudo mkdir -p ./dags ./logs ./plugins ./project ./postgres-db-volume
#sudo chmod -R 777 ./dags ./logs ./plugins ./project ./postgres-db-volume airflow.cfg ./tmp ./.ssh
#echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
#docker-compose up airflow-init
#docker-compose up -d
sudo chmod -R 777 /var/run/docker.sock
#docker build -t airflow_task_python_3.8 DI/airflow_task_python_3.8/.