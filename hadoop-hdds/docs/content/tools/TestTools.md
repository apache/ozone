---
title: "Testing tools"
summary: Ozone contains multiple test tools for load generation, partitioning test or acceptance tests.
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

Testing is one of the most important part during the development of a distributed system. We have the following type of test.

This page includes our existing test tool which are part of the Ozone source base.

Note: we have more tests (like TCP-DS, TCP-H tests via Spark or Hive) which are not included here because they use external tools only.

## Unit test

As every almost every java project we have the good old unit tests inside each of our projects.

## Integration test (JUnit)

Traditional unit tests are supposed to test only one unit, but we also have higher level unit tests. They use `MiniOzoneCluster` which is a helper method to start real daemons (scm,om,datanodes) during the unit test.

From maven/java point of view they are just simple unit tests (JUnit library is used) but to separate them (and solve some dependency problems) we moved all of these tests to `hadoop-ozone/integration-test`

## Smoketest

We use docker-compose based pseudo-cluster to run different configuration of Ozone. To be sure that the different configuration can be started we implemented _acceptance_ tests with the help of https://robotframework.org/.

The smoketests are available from the distribution (`./smoketest`) but the robot files defines only the tests: usually they start CLI and check the output.

To run the tests in different environment (docker-compose, kubernetes) you need a definition to start the containers and execute the right tests in the right containers.

These definition of the tests are included in the `compose` directory (check `./compose/*/test.sh` or `./compose/test-all.sh`).

For example a simple way to test the distribution package:

```
cd compose/ozone
./test.sh
```

## Blockade

[Blockade](https://github.com/worstcase/blockade) is a tool to test network failures and partitions (it's inspired by the legendary [Jepsen tests](https://jepsen.io/analyses)).

Blockade tests are implemented with the help of tests and can be started from the `./blockade` directory of the distribution.

```
cd blockade
pip install pytest==2.8.7,blockade
python -m pytest -s .
```

See the README in the blockade directory for more details.

## MiniChaosOzoneCluster

This is a way to get [chaos](https://en.wikipedia.org/wiki/Chaos_engineering) in your machine. It can be started from the source code and a MiniOzoneCluster (which starts real daemons) will be started and killed randomly.

## Freon

Freon is a command line application which is included in the Ozone distribution. It's a load generator which is used in our stress tests.

Random keys:

In randomkeys mode, the data written into ozone cluster is randomly generated. Each key will be of size 10 KB.

The number of volumes/buckets/keys can be configured. The replication type and factor (eg. replicate with ratis to 3 nodes) also can be configured.

For more information use:

bin/ozone freon --help

For example:

```
ozone freon randomkeys --num-of-volumes=10 --num-of-buckets 10 --num-of-keys 10  --replication-type=RATIS --factor=THREE
```

```
***************************************************
Status: Success
Git Base Revision: 48aae081e5afacbb3240657556b26c29e61830c3
Number of Volumes created: 10
Number of Buckets created: 100
Number of Keys added: 1000
Ratis replication factor: THREE
Ratis replication type: RATIS
Average Time spent in volume creation: 00:00:00,035
Average Time spent in bucket creation: 00:00:00,319
Average Time spent in key creation: 00:00:03,659
Average Time spent in key write: 00:00:10,894
Total bytes written: 10240000
Total Execution time: 00:00:16,898
***********************
```
