FROM ubuntu

ENV TZ=Europe/Moscow

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY requirements.txt /tmp/tmp/requirements.txt

RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get install -y python3.9 && \
    apt-get install -y python3-pip

RUN cd /usr/bin && \
    ln -s python3.9 python && \
    python -m pip install --upgrade pip

WORKDIR /tmp/tmp

RUN apt-get install -y build-essential libssl-dev libffi-dev python3.9-dev && \
    apt-get install -y unixodbc-dev

RUN pip install -r requirements.txt

