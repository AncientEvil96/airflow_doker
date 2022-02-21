FROM ubuntu

ENV TZ=Europe/Moscow

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY requirements.txt /tmp/tmp/requirements.txt

RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get install -y python3.8 && \
    apt-get install -y python3-pip &&\
    apt-get install -y curl

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17

RUN cd /usr/bin && \
    ln -s python3.8 python && \
    python -m pip install --upgrade pip

WORKDIR /tmp/tmp

RUN python -m pip install --upgrade pip
RUN apt-get install -y build-essential libssl-dev libffi-dev python3.9-dev unixodbc-dev

RUN pip install -r requirements.txt

