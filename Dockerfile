FROM ubuntu:18.04 AS BUILD

RUN apt-get update && apt-get -y install make python3.7 python3-pip git
RUN python3.7 -m pip install --upgrade pip

COPY . /app
WORKDIR /app

RUN pip install -r requirements
CMD python3.7 main.py
