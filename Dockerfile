FROM ubuntu:16.04
RUN apt-get -y update \
  && apt-get -y upgrade \
  && apt-get -y install \
    git \
    maven \
    openjdk-8-jdk \
  && apt-get clean
