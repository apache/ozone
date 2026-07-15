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

Ozone on Kubernetes
===

**Note:** The Kubernetes examples and scripts in this project have been tested with Kubernetes 1.34.2 (k3s v1.34.2+k3s1).

## Connecting IDE to Ozone on Kubernetes

### Setup Ozone and Kubernetes

To start Ozone in Kubernetes, you need Kubernetes and kubectl installed. You also need to add the [flekszible](https://github.com/elek/flekszible) binary in your path.

1. Build the project `mvn clean install -DskipShade -DskipTests`
2. `cd <ozone-root>/hadoop-ozone/dist/target/ozone-X.X../kubernetes/examples/ozone`
3. `source ../testlib.sh`
4. `regenerate_resources`

### Configure IntelliJ

Select some preferred ports for attaching the debugger.
Below there is an example configuration for connecting OM to port 5005, SCM to port 6006 and S3G to port 7007.

1. `Run -> Edit Configurations... -> Add New Configuration -> Remote JVM Debug`
2. Name: `om-0`, Debugger mode: `Attach to remote JVM`, Host: `localhost`, Port: `5005`
3. Name: `scm-0`, Debugger mode: `Attach to remote JVM`, Host: `localhost`, Port: `6006`
4. Name: `s3g-0`, Debugger mode: `Attach to remote JVM`, Host: `localhost`, Port: `7007`

Update Ozone startup to use the preferred ports, as follows:

In the file `<ozone-root>/hadoop-ozone/dist/target/ozone-X.X../bin/ozone`

1. Replace `OZONE_OM_OPTS="${RATIS_OPTS} ${OZONE_OM_OPTS}"` with `OZONE_OM_OPTS="${RATIS_OPTS} ${OZONE_OM_OPTS} -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005"`
2. Replace `OZONE_SCM_OPTS="${RATIS_OPTS} ${OZONE_SCM_OPTS}"` with `OZONE_SCM_OPTS="${RATIS_OPTS} ${OZONE_SCM_OPTS}  -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:6006"`
3. In `ozone-s3gateway` add `OZONE_S3G_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:7007"`

### Start Ozone in Kubernetes

1. Start the kubernetes server. For example, if using k3s, `sudo k3s server`
2. In a new terminal `cd <ozone-root>/hadoop-ozone/dist/target/ozone-X.X../kubernetes/examples/ozone`
3. `kubectl apply -f .`
4. Check if the pods are running `kubectl get pods`
5. Start port-forwarding in the background
   ```
      kubectl port-forward om-0 5005:5005 &
      kubectl port-forward scm-0 6006:6006 &
      kubectl port-forward s3g-0 7007:7007 &
   ```

### Connect IntelliJ to Kubernetes cluster

1. Go over the code and set breakpoints for the parts you want to debug
2. Start in Debug mode the Remote JVM Debug configurations that we created earlier in IntelliJ
3. An example use would be to read and write objects in bash and check the output in the Debugger in IntelliJ.  For example, set a breakpoint in the OMVolumeCreateRequest, then create a volume like so to hit it:
```
      kubectl exec -it scm-0 bash
      ozone sh volume create testVolume
```
