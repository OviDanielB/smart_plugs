
FROM openjdk:latest
USER root

# install dev tools
RUN apt-get update
RUN apt-get install -y curl sudo openssh-server rsync net-tools


# passwordless ssh
RUN ssh-keygen -q -N "" -t rsa -f /root/.ssh/id_rsa
RUN cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys

ENV USR_LOCAL /usr/local
ENV SPARK_HOME $USR_LOCAL/spark

# copy spark from remote
#ADD http://it.apache.contactlab.it/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz $USR_LOCAL
# comment only one of them!!
# copy spark from local
ADD spark-2.3.0-bin-hadoop2.7.tgz $USR_LOCAL

RUN ln -s ${USR_LOCAL}/spark-2.3.0-bin-hadoop2.7 $SPARK_HOME

RUN chmod 700 ${SPARK_HOME}/sbin/start-master.sh

ADD config/init.sh /init.sh
RUN chmod 700 /init.sh

CMD /init.sh

# spark master
EXPOSE 7077

# spark web UI
EXPOSE 8080
