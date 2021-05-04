Apache Ozone Contribution guideline
===

Ozone is an Apache project. The bug tracking system for Ozone is under the [Apache Jira project named HDDS](https://issues.apache.org/jira/projects/HDDS/).

This document summarize the contribution process:

## What can I contribute?

We welcome contributions of:

 * **Code**. File a bug and submit a patch, or pick up any one of the unassigned Jiras.
   * [Newbie Ozone jiras](https://s.apache.org/OzoneNewbieJiras)
   * [All open and unassigned Ozone jiras](https://s.apache.org/OzoneUnassignedJiras)
 * **Documentation Improvements**: You can submit improvements to either:
     * Ozone website. Instructions are here: [Modifying the Ozone Website](https://cwiki.apache.org/confluence/display/OZONE/Modifying+the+Ozone+Website)
     * Developer docs. These are markdown files [checked into the Apache Ozone Source tree](https://github.com/apache/ozone/tree/master/hadoop-hdds/docs/content).
 * The [wiki pages](https://cwiki.apache.org/confluence/display/OZONE/Contributing+to+Ozone): Please contact us at dev@ozone.apache.org and we can provide you write access to the wiki.
 * **Testing**: We always need help to improve our testing
      * Unit Tests (JUnit / Java)
      * Acceptance Tests (docker + robot framework)
      * Blockade tests (python + blockade) 
      * Performance: We have multiple type of load generator / benchmark tools (`ozone freon`, `ozone genesis`), which can be used to test cluster and report problems.
 * **Bug reports** pointing out broken functionality, docs, or suggestions for improvements are always welcome!
 
## Who To Contact

If you have any questions, please don't hesitate to contact

 * **email**: use dev@ozone.apache.org.
 * **chat**: You can find the #ozone channel at the ASF slack. Invite link is [here](http://s.apache.org/slack-invite)
 * **meeting**: [We have weekly meetings](https://cwiki.apache.org/confluence/display/OZONE/Ozone+Community+Calls) which is open to anybody. Feel free to join and ask any questions
    
## Building from the source code

### Requirements

Requirements to compile the code:

* Unix System
* JDK 1.8 or higher
* Maven 3.5 or later
* Internet connection for first build (to fetch all Maven and Ozone dependencies)

Additional requirements to run your first pseudo cluster:

* docker
* docker-compose

Additional requirements to execute different type of tests:

* [Robot framework](https://robotframework.org/) (for executing acceptance tests)
* docker-compose (to start pseudo cluster, also used for blockade and acceptance tests)
* [blockade](https://pypi.org/project/blockade/) To execute network fault-injection testing.
* [jq](https://stedolan.github.io/jq/) (for executing acceptance tests)

Optional dependencies:

* [hugo](https://gohugo.io/) to include the documentation in the web ui.

(Standard development tools such as make, gcc, etc. are required.)

### Build the project

After installing the requirements (especially maven) the build is as simple as:

```
mvn clean install -DskipTests
```

And you can start your first cluster:

```
cd hadoop-ozone/dist/target/ozone-*-SNAPSHOT/compose/ozone
docker-compose up -d --scale datanode=3
```

### Helper scripts

`hadoop-ozone/dev-support/checks` directory contains helper scripts to build and check your code. (Including findbugs and checkstyle). Use them if you don't know the exact maven goals / parameters.

These scripts are executed by the CI servers, so it's always good to run them locally before creating a PR.

### Maven build options:

  * Use `-DskipShade` to exclude ozonefs jar file creation from the release. It's way more faster, but you can't test Hadoop Compatible file system.
  * Use `-DskipRecon` to exclude the Recon build (Web UI and monitoring) from the build. It saves about 2 additional minutes.
  * Use `-Pdist` to build a distribution (Without this profile you won't have the final tar file)
  * Use `-Pdocker-build` to build a docker image which includes Ozone
  * Use `-Ddocker.image=repo/name` to define the name of your docker image
  * USe `-Pdocker-push` to push the created docker image to the docker registry
  
## Contribute your modifications

We use github pull requests instead of uploading patches to JIRA. The main contribution workflow is as follows:

  1. Fork `apache/ozone` github repository (first time)
  2. Create a new Jira in HDDS project (eg. HDDS-1234)
  3. Create a local branch for your contribution (eg. `git checkout -b HDDS-1234`)
  4. Create your commits and push your branches to your personal fork.
  5. Create a pull request on github UI 
      * Please include the Jira link, problem description and testing instruction
  6. Set the Jira to "Patch Available" state
  7. Address any review comments if applicable by pushing new commits to the PR.
  8. When addressing review comments, there is no need to squash your commits. This makes it easy for reviewers to only review the incremental changes. The committer will take care to squash all your commits before merging to master.
    
## Code convention and tests

We follow the code convention of Hadoop project (2 spaces instead of tabs, 80 char line width, ASF licence headers). The code checked with checkstyle, findbugs and various test frameworks.

Please don't post / commit any code with any code violations (all checks are not checking the introduced violations as checks in Hadoop but all the available violations).

### Check your contribution

The easiest way to check your contribution is using the simplified shell scripts under `hadoop-ozone/dev-support/checks`. The problems will be printed out on the standard output.

For example:
```
hadoop-ozone/dev-support/checks/rat.sh
hadoop-ozone/dev-support/checks/checkstyle.sh
hadoop-ozone/dev-support/checks/findbugs.sh
```

Execution of rat and checkstyle are very fast. Findbugs is slightly slower. Executing unit.sh takes about 30 minutes.

The same scripts are executed by the github PR checker.

It's always good practice (and fast) to test with the related docker-compose based pseudo clusters:

```
cd hadoop-ozone/dist/target/ozone-*-SNAPSHOT/compose/ozone
./test.sh
```

(To test S3 use `compose/ozones3`, to test security use `compose/ozonsecure`, etc.

### False positive findbugs violation

If you have __very good__ reasons, you can ignore any Fingbugs warning. Your good reason can be persisted with the `@SuppressFBWarnings` annotation.

```java
@SuppressFBWarnings(value="AT_OPERATION_SEQUENCE_ON_CONCURRENT_ABSTRACTION",
      justification="The method is synchronized and this is the only place "+
          "dnsToUuidMap is modified")
private synchronized void addEntryTodnsToUuidMap(
...
```

## Using IDE

As Ozone uses Apache Maven it can be developed from any IDE. As IntelliJ is a common choice, here are some suggestions to use it for Ozone development:

### Run Ozone from IntelliJ

Ozone components depends on maven classpath. We generate classpath descriptor from the maven pom.xml files to use exactly the same classpath at runtime.

As a result, it's easy to start _all_ the components from IDE as the right classpath (without provided scope) has already been set.

To start Ozone from IntelliJ:

1. Stop your IDE
2. Execute the `./hadoop-ozone/dev-support/intellij/install-runconfigs.sh` helper script.
3. Start the IDE
4. New runner definitions are available from the Run menu.

You can use the installed Run configurations in the following order:

1. StorageContainerManagerInit (to initialize the SCM dir)
2. StorageContainerManger (to start SCM)
3. OzoneManagerInit (to initialize OM, it requires SCM)
4. OzoneManager
5. Recon (required by datanode)
6. Datanode1, Datanode2, Datanode3

### Setting up Checkstyle

Checkstyle plugin may help to detect violations directly from the IDE.

1. Install `Checkstyle+IDEA` plugin from `File` -> `Settings` -> `Plugins`
2. Open `File` -> `Settings` -> `Other settings` -> `Checkstyle` and Add (`+`) a new `Configuration File`
  * Description: `Ozone`
  * Use a local checkstyle `./hadoop-hdds/dev-support/checkstyle/checkstyle.xml`
3. Check the `pom.xml` for the current version of the used checkstyle and use the same version with the plugin (`File` -> `Settings` -> `Other settings` -> `Checkstyle`)
4. Open the _Checkstyle Tool Window_, select the `Ozone` rule and execute the check

### Common problems

IntelliJ may not pick up protoc generated classes as they can be very huge. If the protoc files can't be compiled try the following:

1. Open _Help_ -> _Edit custom properties_ menu.
2. Add `idea.max.intellisense.filesize=5000` entry
3. Restart your IDE


