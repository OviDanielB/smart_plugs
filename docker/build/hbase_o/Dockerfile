#
#  Author: Hari Sekhon
#  Date: 2016-04-24 21:18:57 +0100 (Sun, 24 Apr 2016)
#
#  vim:ts=4:sts=4:sw=4:et
#
#  https://github.com/harisekhon/Dockerfiles
#
#  If you're using my code you're welcome to connect with me on LinkedIn and optionally send me feedback
#
#  https://www.linkedin.com/in/harisekhon
#

FROM openjdk:latest
MAINTAINER Hari Sekhon (https://www.linkedin.com/in/harisekhon)

ENV PATH $PATH:/hbase/bin
ENV HBASE_VERSION=1.4.4
ENV JAVA_HOME=/usr

WORKDIR /

ADD hbase-1.4.4-bin.tar.gz .
ADD alluxio-1.7.1-client.jar /opt/alluxio/client/alluxio-1.7.1-client.jar

RUN mv hbase-1.4.4 hbase
#VOLUME hdfs://master:54310/hbase-data

COPY entrypoint.sh /
COPY conf/hbase-site.xml /hbase/conf/

#RUN ln -sv "usr/local/hbase-$HBASE_VERSION-bin" hbase

# Stargate  8080  / 8085
# Thrift    9090  / 9095
# HMaster   16000 / 16010
# RS        16201 / 16301
EXPOSE 2181 8080 8085 9090 9095 16000 16010 16201 16301

CMD "/entrypoint.sh"
