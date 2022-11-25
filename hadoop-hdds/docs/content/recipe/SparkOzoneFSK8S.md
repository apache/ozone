---
title: Spark in Kubernetes with OzoneFS
linktitle: Spark
summary: How to use Apache Spark with Ozone on K8s?
---
<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

This recipe shows how Ozone object store can be used from Spark using:

 - OzoneFS (Hadoop compatible file system)
 - Hadoop 2.7 (included in the Spark distribution)
 - Kubernetes Spark scheduler
 - Local spark client


## Requirements

Download latest Spark and Ozone distribution and extract them. This method is
tested with the `spark-2.4.6-bin-hadoop2.7` distribution.

You also need the following:

 * A container repository to push and pull the spark+ozone images. (In this recipe we will use the dockerhub)
 * A repo/name for the custom containers (in this recipe _myrepo/ozone-spark_)
 * A dedicated namespace in kubernetes (we use _yournamespace_ in this recipe)

## Create the docker image for drivers

### Create the base Spark driver/executor image

First of all create a docker image with the Spark image creator.
Execute the following from the Spark distribution

```bash
./bin/docker-image-tool.sh -r myrepo -t 2.4.6 build
```

_Note_: if you use Minikube add the `-m` flag to use the docker daemon of the Minikube image:

```bash
./bin/docker-image-tool.sh -m -r myrepo -t 2.4.6 build
```

`./bin/docker-image-tool.sh` is an official Spark tool to create container images and this step will create multiple Spark container images with the name _myrepo/spark_. The first container will be used as a base container in the following steps.

### Customize the docker image

Create a new directory for customizing the created docker image.

Copy the `ozone-site.xml` from the cluster:

```bash
kubectl cp om-0:/opt/hadoop/etc/hadoop/ozone-site.xml .
```

And create a custom `core-site.xml`.

```xml
<configuration>
    <property>
        <name>fs.AbstractFileSystem.o3fs.impl</name>
        <value>org.apache.hadoop.fs.ozone.OzFs</value>
     </property>
</configuration>
```

Copy the `ozonefs.jar` file from an ozone distribution (__use the hadoop2 version!__)

```
kubectl cp om-0:/opt/hadoop/share/ozone/lib/ozone-filesystem-hadoop2-VERSION.jar ozone-filesystem-hadoop2.jar
```


Create a new Dockerfile and build the image:
```
FROM myrepo/spark:2.4.6
ADD core-site.xml /opt/hadoop/conf/core-site.xml
ADD ozone-site.xml /opt/hadoop/conf/ozone-site.xml
ENV HADOOP_CONF_DIR=/opt/hadoop/conf
ENV SPARK_EXTRA_CLASSPATH=/opt/hadoop/conf
ADD ozone-filesystem-hadoop2.jar /opt/ozone-filesystem-hadoop2.jar
```

```bash
docker build -t myrepo/spark-ozone
```

For remote Kubernetes cluster you may need to push it:

```bash
docker push myrepo/spark-ozone
```

## Create a bucket

Download any text file and put it to the `/tmp/alice.txt` first.

```bash
kubectl port-forward s3g-0 9878:9878
aws s3api --endpoint http://localhost:9878 create-bucket --bucket=test
aws s3api --endpoint http://localhost:9878 put-object --bucket test --key alice.txt --body /tmp/alice.txt
```

## Create service account to use

```bash
kubectl create serviceaccount spark -n yournamespace
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=yournamespace:spark --namespace=yournamespace
```
## Execute the job

Execute the following spark-submit command, but change at least the following values:

 * the Kubernetes master url (you can check your _~/.kube/config_ to find the actual value)
 * the Kubernetes namespace (_yournamespace_ in this example)
 * serviceAccountName (you can use the _spark_ value if you followed the previous steps)
 * container.image (in this example this is _myrepo/spark-ozone_. This is pushed to the registry in the previous steps)

```bash
bin/spark-submit \
    --master k8s://https://kubernetes:6443 \
    --deploy-mode cluster \
    --name spark-word-count \
    --class org.apache.spark.examples.JavaWordCount \
    --conf spark.executor.instances=1 \
    --conf spark.kubernetes.namespace=yournamespace \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=myrepo/spark-ozone \
    --conf spark.kubernetes.container.image.pullPolicy=Always \
    --jars /opt/ozone-filesystem-hadoop2.jar \
    local:///opt/spark/examples/jars/spark-examples_2.11-2.4.0.jar \
    o3fs://test.s3v.ozone-om-0.ozone-om:9862/alice.txt
```

Check the available `spark-word-count-...` pods with `kubectl get pod`

Check the output of the calculation with \
`kubectl logs spark-word-count-1549973913699-driver`

You should see the output of the wordcount job. For example:

```
...
name: 8
William: 3
this,': 1
SOUP!': 1
`Silence: 1
`Mine: 1
ordered.: 1
considering: 3
muttering: 3
candle: 2
...
```
