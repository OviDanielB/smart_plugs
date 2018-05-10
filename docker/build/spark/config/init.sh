#!/usr/bin/env bash

service ssh start

if [ ${SLAVES} != "" ]; then

    touch slaves
    IFS=";"
    arrSLAVES=(${SLAVES})
    unset IFS
    for j in ${arrSLAVES[@]}
    do
        echo ${j} >> slaves
    done
    mv slaves ${SPARK_HOME}/conf/

    cat ${SPARK_HOME}/conf/slaves

    chmod 700 ${SPARK_HOME}/sbin/start-all.sh
    ${SPARK_HOME}/sbin/start-all.sh
fi


# ${SPARK_HOME}/sbin/start-master.sh

/bin/bash