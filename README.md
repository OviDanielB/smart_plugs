#### Google Cloud Deployment

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