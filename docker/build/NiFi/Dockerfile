FROM apache/nifi
USER root

RUN apt-get update \
    && apt-get install -y curl tar sudo rsync default-jre vim net-tools \
    && apt-get install -y jq xmlstarlet

ADD config/core-site.xml /core-site.xml
ADD config/hdfs-site.xml /hdfs-site.xml
ADD config/hbase-site.xml /hbase-site.xml

# Apply configuration and start NiFi
CMD ${NIFI_BASE_DIR}/scripts/start.sh