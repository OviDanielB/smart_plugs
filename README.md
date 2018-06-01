Table of Contents
=================

* [Application usage](#application-usage)
* [Local Deployment](#local-deployment)
   * [Data Ingestion: Apache NiFi](#data-ingestion-apache-nifi)
   * [Alluxio](#alluxio)
* [Google Cloud Deployment](#google-cloud-deployment)
   * [Software Needed](#software-needed)
   * [Region, Zone and VPC](#region-zone-and-vpc)
   * [DataProc Cluster](#dataproc-cluster)
   * [Kubernetes Cluster](#kubernetes-cluster)
   * [Alluxio Deployment](#alluxio-deployment)
   * [MongoDB Deployment](#mongodb-deployment)

#### Application usage
The jar has two main entrypoint the AppMain and the BenchmarkMain.
You can run the application on a Spark cluster exploiting the spark-submit.sh script in the master node.
For more info refer to <https://spark.apache.org/docs/latest/submitting-applications.html> .
The AppMain executes the queries and store the results in a file. Use it as follows:

```
./bin/spark-submit --class AppMain --master <master-url> \
                   --deploy-mode <deploy-mode> hdfs://master:54310/app.jar \
                    <csv file> <parquet file> <avro file> <deploymode> <cacheOrNot>
```

The argument <deploymode> can be "local" or "cluster". <cacheOrNot> can be "cache" or "no_cache"
The BenchmarkMain runs the queries and compute the execution time saving it in a file.

```
./bin/spark-submit --class BenchmarkMain --master <master-url> \
                   --deploy-mode <deploy-mode> hdfs://master:54310/app.jar \
                    <output> <<csv file> <parquet file> <avro file> <deploymode> <cacheOrNot> <#run>
```

The <output> parameter take the path to the file where write the execution times for the query performed with the different
input file format. <#run> specify the number of runs for each query on which to compute the average of the execution time.


Local Deployment
=================

To deploy locally the layers of the application you can exploit Docker.
Several scripts are available in the directory `/docker/run`

##### Data Ingestion: Apache NiFi
The framework Apache NiFi is used to transform the dataset from csv to Avro and Parquet formats.
The dataset is filtered removing tuple with value field equals to zero and then it is loaded into HDFS.
In order to create the NiFi container using Docker, you can run the `nifi-start.sh` script. 
It defines the environment variable HDFS_DEFAULT_FS to which is assigned the address for HDFS.
The default is hdfs://master:54310 but you can change it before run the script.

The image already contains the template that is instantiated automatically on NiFi.
To start it, you can access the NiFi UI at <http://localhost:9999/nifi/>. After the container startup
the UI can take several minute to be available.
To start the dataFlow you must first enable all the controller services as describet at <https://nifi.apache.org/docs/nifi-docs/html/user-guide.html#Enabling_Disabling_Controller_Services>
For more info please refer to <https://nifi.apache.org/docs/nifi-docs/html/user-guide.html#starting-a-component>

##### Alluxio
To build and run Alluxio docker image please refer to: [Build and run alluxio docker image](https://github.com/trillaura/smart_plugs/blob/master/docker/build/alluxio/README.md)

Google Cloud Deployment
=================

##### Software Needed
* *kubectl* - Kubernetes CLI
* *gcloud*  - Google Cloud CLI


##### Region, Zone and VPC
First of all choose a region (e.g. europe-west-4) and a zone (e.g. europe-west-4a) on GC on which all components will be deployed.
Create a new Virtual Private Cloud (VPC) network or use the default one. If a new network
is created, be sure that the Firewall rules are set accordingly. For development simplicity all
ports can be opened for Ingress traffic. 

##### DataProc Cluster
To run the application, you need a cluster with Spark and Hadoop frameworks. 
To create one, use the **DataProc** service. In the cluster section, choose to 
create a new one. Set the region,zone and VPC network according to *Region, Zone and VPC * section, cluster 
mode to Standard (1 master, N worker) and set the machine type and storage configuration
(the default ones should do fine). Once the cluster deployment has finished, 
find the master's external IP (**smIP**). The cluster contains by default HDFS, Hadoop MapReduce,Spark, Hive and other
Big Data frameworks. Some available default web interfaces:
* http://**smIP**:9870   - HDFS namenode
* http://**smIP**:8088   - YARN resource manager

From the HDFS Web UI, there is the master hostname and ports. By default
it is *cluster-name*-m:8020 .

Perform SSH into master (VM Instances subsection in cluster info). Run the following:
```
hdfs dfs -mkdir /alluxio
```

to create a directory that *Alluxio* will use as its root directory. 

Note: the master IP is ephemeral by default. If needed, a static IP can be assigned. 


##### Kubernetes Cluster
Using the **Kubernetes Engine**, create a new cluster with the same region and network 
configuration as the DataProc one (else the component's won't find themselves).
Connect with a local terminal to the cluster (using the gcloud cli):
```
gcloud container clusters get-credentials <cluster-name> \
      --zone <selected-zone>
```
 
This way the cluster's credentials will be available to *kubectl*.
To verify that everything went well, run 
```
kubectl get nodes
```
where all the cluster's nodes should be displayed.


##### Alluxio Deployment
In the project file *kubernetes/alluxio/conf/alluxio.properties* , set the 
**ALLUXIO_UNDERFS_ADDRESS** to *hdfs://**smIP**:8020/alluxio* so that Alluxio
will use HDFS as its underlying storage. You can use other underlying storage
such as Google Cloud Storage specifying *gs://bucket-name/directory*. 
After all settings are configured, run the script
```
kubernetes/alluxio/load_config.sh
```
to load a ConfigMap in Kubernetes where Alluxio's configuration is stored and 
later retrieved.

After, deploy Alluxio master and worker with
```
kubernetes/alluxio/deploy_alluxio.sh
```

##### MongoDB Deployment
Mongo is used for storing query results. To deploy it, run
```
kubernetes/mongodb/deploy_mongo.sh
```