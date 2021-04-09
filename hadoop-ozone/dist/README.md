<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Ozone Distribution

This folder contains the project to create the binary Ozone distribution and provide all the helper scripts and Docker files to start Ozone locally or on a remote cluster.

## Testing with local docker based cluster

After a full dist build you can find multiple docker-compose-based cluster definitions in the `target/ozone-*/compose` folder.

Please check the README files there.

Usually, you can start the cluster with:

```
cd compose/ozone
docker-compose up -d
```

More information can be found in the Getting Started Guide:
* [Getting Started: Run Ozone with Docker Compose](https://hadoop.apache.org/ozone/docs/current/start/runningviadocker.html)

## Testing on Kubernetes


### Installation

Please refer to the Getting Started guide for a couple of options for testing Ozone on Kubernetes:
* [Getting Started: Minikube and Ozone](https://hadoop.apache.org/ozone/docs/current/start/minikube.html)
* [Getting Started: Ozone on Kubernetes](https://hadoop.apache.org/ozone/docs/current/start/kubernetes.html)

### Monitoring

Apache Ozone supports Prometheus out of the box. It contains a prometheus-compatible exporter servlet. To start monitoring you need a Prometheus deployment in your Kubernetes cluster:

```
cd src/main/k8s/prometheus
kubectl apply -f .
```

The Prometheus UI can be made accessible via a NodePort service:

```
minikube service prometheus-public
```

### Notes on the Kubernetes setup

Please note that the provided Kubernetes resources are not suitable for production:

1. There is no security setup.
2. The datanode is started as a StatefulSet instead of DaemonSet.  This is to make it possible to scale it up on one-node minikube cluster.
3. All of the UI pages are published with NodePort services.