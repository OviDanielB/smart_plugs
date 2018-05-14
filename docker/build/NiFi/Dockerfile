FROM ubuntu:16.04
USER root

ARG NIFI_VERSION=1.6.0
ARG MIRROR=https://archive.apache.org/dist

ENV NIFI_BASE_DIR /opt/nifi
ENV NIFI_HOME=${NIFI_BASE_DIR}/nifi-${NIFI_VERSION} \
    NIFI_BINARY_URL=/nifi/${NIFI_VERSION}/nifi-${NIFI_VERSION}-bin.tar.gz

ADD scripts/ /opt/nifi/scripts/

RUN apt-get update \
    && apt-get install -y curl tar sudo rsync default-jre vim net-tools \
    && apt-get install -y jq xmlstarlet

RUN mkdir -p ${NIFI_HOME}/conf/templates

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/jre
ENV PATH $PATH:$JAVA_HOME/bin

# Download, validate, and expand Apache NiFi binary.
RUN curl -fSL ${MIRROR}/${NIFI_BINARY_URL} -o ${NIFI_BASE_DIR}/nifi-${NIFI_VERSION}-bin.tar.gz \
    && echo "$(curl https://archive.apache.org/dist/${NIFI_BINARY_URL}.sha256) *${NIFI_BASE_DIR}/nifi-${NIFI_VERSION}-bin.tar.gz" | sha256sum -c - \
    && tar -xvzf ${NIFI_BASE_DIR}/nifi-${NIFI_VERSION}-bin.tar.gz -C ${NIFI_BASE_DIR} \
    && rm ${NIFI_BASE_DIR}/nifi-${NIFI_VERSION}-bin.tar.gz

ADD config/core-site.xml /core-site.xml
ADD config/hdfs-site.xml /hdfs-site.xml

# Web HTTP(s) & Socket Site-to-Site Ports
EXPOSE 8080 8443 10000

WORKDIR ${NIFI_HOME}

# Apply configuration and start NiFi
CMD ${NIFI_BASE_DIR}/scripts/start.sh