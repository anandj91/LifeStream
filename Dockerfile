FROM ubuntu:20.04

ADD ./src /root
WORKDIR /root

RUN apt-get update
RUN apt-get install -y wget

RUN ./setup.sh
