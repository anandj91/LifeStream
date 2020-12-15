FROM ubuntu:20.04

ADD ./LifeStream /root
WORKDIR /root

RUN apt-get update
RUN apt-get install -y wget

RUN cd LifeStream
RUN ./setup.sh
