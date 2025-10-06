<div align="center">
  <a href="https://ozone.apache.org">
    <img src="https://www.apache.org/logos/res/ozone/default.png" alt="Apache Ozone Logo" />
  </a>
</div>

[![License](https://img.shields.io/:license-Apache%202-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0.txt)
[![Docker Pulls](https://img.shields.io/docker/pulls/apache/ozone.svg)](https://hub.docker.com/r/apache/ozone)
[![Docker Stars](https://img.shields.io/docker/stars/apache/ozone.svg)](https://hub.docker.com/r/apache/ozone)
[![Contributors](https://img.shields.io/github/contributors/apache/ozone)](https://github.com/apache/ozone/graphs/contributors)
[![Commit Activity](https://img.shields.io/github/commit-activity/m/apache/ozone)](https://github.com/apache/ozone/commits/master)
[![OSSRank](https://shields.io/endpoint?url=https://ossrank.com/shield/3018)](https://ossrank.com/p/3018-apache-ozone)

Apache Ozone
===

Ozone is a scalable, redundant, and distributed object store for Hadoop and Cloud-native environments. Apart from scaling to billions of objects of varying sizes, Ozone can function effectively in containerized environments such as Kubernetes and YARN.


 * MULTI-PROTOCOL SUPPORT: Ozone supports different protocols like S3 and Hadoop File System APIs.
 * SCALABLE: Ozone is designed to scale to tens of billions of files and blocks and, in the future, even more.
 * CONSISTENT: Ozone is a strongly consistent object store. This consistency is achieved by using protocols like RAFT.
 * CLOUD-NATIVE: Ozone is designed to work well in containerized environments like YARN and Kubernetes.
 * SECURE: Ozone integrates with Kerberos infrastructure for authentication, supports native ACLs and integrates with Ranger for access control and supports TDE and on-wire encryption.
 * HIGHLY AVAILABLE: Ozone is a fully replicated system that is designed to survive multiple failures.

## Documentation

The latest documentation is generated together with the releases and hosted on the apache site.

Please check [the documentation page](https://ozone.apache.org/docs/) for more information.

## Contact

Ozone is a top level project under the [Apache Software Foundation](https://apache.org)

 * Ozone [web page](https://ozone.apache.org)
 * Mailing lists
     * For any questions use: [dev@ozone.apache.org](https://lists.apache.org/list.html?dev@ozone.apache.org)
 * Chat: There are a few ways to interact with the community
     * You can find the #ozone channel on the official ASF Slack. Invite link is [here](http://s.apache.org/slack-invite).
     * You can use [GitHub Discussions](https://github.com/apache/ozone/discussions) to post questions or follow community syncs. 
 * There are Open [Weekly calls](https://cwiki.apache.org/confluence/display/OZONE/Ozone+Community+Calls) where you can ask anything about Ozone.
    * Past meeting notes are also available from the wiki.
 * Reporting security issues: Please consult with [SECURITY.md](./SECURITY.md) about reporting security vulnerabilities and issues.

## Download

Latest release artifacts (source release and binary packages) are [available](https://ozone.apache.org/downloads/) from the Ozone web page.

## Quick start

### Run Ozone with Docker Compose

The easiest way to start a cluster with docker is by using Docker Compose:

- Obtain Ozone’s sample Docker Compose configuration:
```bash
curl -O https://raw.githubusercontent.com/apache/ozone-docker/refs/heads/latest/docker-compose.yaml
```

- Start the cluster
```bash
docker compose up -d --scale datanode=3
```

- Note: By default, the cluster will be started with replication factor set to 1. It can be changed by setting the environment variable `OZONE_REPLICATION_FACTOR` to the desired value.

And you can use AWS S3 cli:

- First, let’s configure AWS access key and secret key. Because the cluster is not secured, you can use any arbitrary access key and secret key. For example:
```bash
export AWS_ACCESS_KEY_ID=testuser/scm@EXAMPLE.COM
export AWS_SECRET_ACCESS_KEY=c261b6ecabf7d37d5f9ded654b1c724adac9bd9f13e247a235e567e8296d2999
```

- Then we can create a bucket and upload a file to it:
```
aws s3api --endpoint http://localhost:9878/ create-bucket --bucket=wordcount
# create a temporary file to upload to Ozone via S3 support 
ls -1 > /tmp/testfile
aws s3 --endpoint http://localhost:9878 cp --storage-class REDUCED_REDUNDANCY  /tmp/testfile  s3://wordcount/testfile
```

### Run Ozone from released artifact

If you need a more realistic cluster, you can [download](https://ozone.apache.org/downloads/) the latest (binary) release package, and start a cluster with the help of docker-compose:

After you untar the binary:

```
cd compose/ozone
docker-compose up -d --scale datanode=3
```

The `compose` folder contains different sets of configured clusters (secure, HA, mapreduce example), you can check the various subfolders for more examples.

### Run on Kubernetes

Ozone is a first class citizen of the Cloud-Native environments. The binary package contains multiple sets of K8s resource files to show how it can be deployed.

## Build from source

Ozone can be built with [Apache Maven](https://maven.apache.org):

```
mvn clean install -DskipTests
```

And can be started with the help of Docker:

```
cd hadoop-ozone/dist/target/ozone-*/compose/ozone
docker-compose up -d --scale datanode=3
```
For more information, you can check the [Contribution guideline](./CONTRIBUTING.md)

## Contribute

All contributions are welcome.

 1. Please open a [Jira](https://issues.apache.org/jira/projects/HDDS/issues) issue
 2. And create a pull request

For more information, you can check the [Contribution guideline](./CONTRIBUTING.md)

## License

The Apache Ozone project is licensed under the Apache 2.0 License. See the [LICENSE](./LICENSE.txt) file for details.
