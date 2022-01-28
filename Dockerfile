FROM ubuntu

ENV TZ=Europe/Moscow

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y \
    sudo \
    apt install software-properties-common \
    add-apt-repository ppa:deadsnakes/ppa \
    apt install python3.9 \
    

RUN cd /usr/bin && \
    sudo ln -s python3.9 python && \
    python -m pip install --upgrade pip
    
RUN apt-get install build-essential libssl-dev libffi-dev python3.9-dev \
    apt-get install unixodbc-dev

RUN pip install -r requirements.txt

WORKDIR /tmp/tmp