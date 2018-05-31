FROM apache/nifi
USER root

RUN apt-get update \
    && apt-get install -y curl tar sudo rsync default-jre vim net-tools \
    && apt-get install -y jq xmlstarlet

ADD config/core-site.xml /core-site.xml
ADD config/hdfs-site.xml /hdfs-site.xml
ADD config/hbase-site.xml /hbase-site.xml

RUN mkdir /data
WORKDIR /data
RUN curl -O https://storage.googleapis.com/debs14_dataset/d14_filtered.csv

WORKDIR /
RUN wget http://central.maven.org/maven2/com/github/hermannpencole/nifi-deploy-config/1.1.32/nifi-deploy-config-1.1.32.jar

# Apply configuration and start NiFi
CMD ${NIFI_BASE_DIR}/scripts/start.sh