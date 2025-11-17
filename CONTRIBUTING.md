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
      * Performance: We have multiple type of load generator / benchmark tools (`ozone freon`),
        which can be used to test cluster and report problems.
 * **Bug reports** pointing out broken functionality, docs, or suggestions for improvements are always welcome!
 
## Who To Contact

If you have any questions, please don't hesitate to contact

 * **email**: use dev@ozone.apache.org.
 * **chat**: You can find the #ozone channel at the ASF slack. Invite link is [here](http://s.apache.org/slack-invite)
 * **GitHub Discussions**: You can also interact with the community using [GitHub Discussions](https://github.com/apache/ozone/discussions). 
 * **meeting**: [We have weekly meetings](https://cwiki.apache.org/confluence/display/OZONE/Ozone+Community+Calls) which is open to anybody. Feel free to join and ask any questions
    
## Building from source

### Requirements

Requirements to compile the code:

* Unix System
* JDK 1.8 or higher
* Maven 3.6 or later
* Internet connection for first build (to fetch all Maven and Ozone dependencies)

(Standard development tools such as make, gcc, etc. are required.)

### Build the project

After installing the requirements (especially Maven) build is as simple as:

```
mvn clean verify -DskipTests
```

### Useful Maven build options

  * Use `-DskipShade` to skip shaded Ozone FS jar file creation. Saves time, but you can't test integration with other software that uses Ozone as a Hadoop-compatible file system.
  * Use `-DskipRecon` to skip building Recon Web UI. It saves about 2 minutes.
  * Use `-Pdist` to build the binary tarball, similar to the one that gets released

## Running Ozone in Docker

Additional requirements for running Ozone in pseudo cluster (including acceptance tests):

* docker
* docker-compose
* [jq](https://stedolan.github.io/jq/) (utility used heavily by acceptance tests)

After building Ozone locally, you can start your first pseudo cluster:

```
cd hadoop-ozone/dist/target/ozone-*-SNAPSHOT/compose/ozone
OZONE_REPLICATION_FACTOR=3 ./run.sh -d
```

See more details in the [README](https://github.com/apache/ozone/blob/master/hadoop-ozone/dist/src/main/compose/ozone/README.md) and in the [docs](https://ozone.apache.org/docs/current/start.html).

## Jira guideline

When creating a new jira for any kind of new feature, improvement or bug, please follow below guideline: 

  1. **Title:** Title should be a one-liner stating the problem.
  2. **Description:**
     * What is the problem? Is it a feature, improvement or bug? Add as many details as possible and related design doc and discussion.
     * For new features, add as many details as possible. If it is part of the big feature, attach parent jira.
     * For improvement, add the value it will bring. Is it an optimization, code simplification or something else?
     * For bugs, add steps to reproduce it. Where the root cause is unknown and needs investigation, it would be great to update the jira description or add the summary once the root cause is identified.
     * If it is follow up of another issue, please link the previous jira to it so that context is preserve.
  3. **Jira examples:** [HDDS-9272](https://issues.apache.org/jira/browse/HDDS-9272), [HDDS-9322](https://issues.apache.org/jira/browse/HDDS-9322), [HDDS-9291](https://issues.apache.org/jira/browse/HDDS-9291), [HDDS-8940](https://issues.apache.org/jira/browse/HDDS-8940), [HDDS-9282](https://issues.apache.org/jira/browse/HDDS-9282)

## New feature development

For large feature development changes, we use a process called "Ozone Enhancement Proposals" (OEP). This process is designed to ensure that major changes to Ozone are well-designed and have community consensus. If you are planning to propose a significant change, please read the [Ozone Enhancement Proposals](https://ozone.apache.org/docs/edge/design/ozone-enhancement-proposals.html) documentation and create a design document before you start coding. Please note that we only accept design documents in Markdown format; PDF or Google Docs are no longer accepted.

## Contribute your modifications

We use GitHub pull requests for contributing changes to the repository. The main workflow is as follows:

  1. Fork [`apache/ozone`](https://github.com/apache/ozone) repository (first time) and clone it to your local machine
  2. Enable the `build-branch` GitHub Actions workflow (defined in `.github/workflows/post-commit.yml`) in your fork
  3. Ensure a Jira issue corresponding to the change exists in the [HDDS project](https://issues.apache.org/jira/projects/HDDS/) (eg. HDDS-1234)
     * Please search Jira before creating a new issue, someone might have already reported the same.
     * If this is your first issue, you might not be able to assign it to yourself.  If so, please make a comment in the issue, indicating that your are working on it.
  4. Create a local branch for your contribution (eg. `git checkout -b HDDS-1234`)
  5. Make your changes locally.
     * For complex changes, committing each logical part is recommended.
  6. Push your changes to your fork of Ozone
  7. Wait for the `build-branch` workflow to complete successfully for your commit.
  8. Create a pull request for your changes
     * Please include the Jira link, problem description and testing instruction (follow the [template](https://github.com/apache/ozone/blob/master/.github/pull_request_template.md))
  9. Set the Jira issue to "Patch Available" state
  10. Address any review comments if applicable
      * Create new, incremental commits in your branch.  This makes it easy for reviewers to only review the new changes. The committer will take care to squash all your commits when merging the pull request.
      * Push your commits in a batch, when no more changes are expected.  This reduces the burden on automated CI checks.
      * If you need to bring your PR up-to-date with the base branch (usually `master`), e.g. to resolve conflicts, please do so by merge, not rebase: `git merge --no-edit origin/master`.
      * In general, please try to avoid force-push when updating the PR.  Here are some great articles that explain why:
        * https://developers.mattermost.com/blog/submitting-great-prs/#4-avoid-force-pushing
        * https://www.freecodecamp.org/news/optimize-pull-requests-for-reviewer-happiness#request-a-review
## Code convention and tests

Basic code conventions followed by Ozone:

 * 2 spaces indentation
 * 120-char line length limit
 * Apache license header required in most files
 * no `@author` tags, authorship is indicated by Git history

These are checked by tools like Checkstyle and RAT.

Ozone code style is shared via `.editorconfig` configuration file and will be automatically imported when the project is opened.

See https://www.jetbrains.com/help/idea/configure-project-settings.html#share-project-through-vcs for detailed instructions.

### Check your contribution

The [`hadoop-ozone/dev-support/checks` directory](https://github.com/apache/ozone/tree/master/hadoop-ozone/dev-support/checks) contains scripts to build and check Ozone.  Most of these are executed by CI for every commit and pull request.  Running them before creating a pull request is strongly recommended.  This can be achieved by enabling the `build-branch` workflow in your fork and letting GitHub run all of the checks, but most of the checks can also be run locally.

 1. `build.sh`: compiles Ozone
 2. quick checks (less than 2 minutes)
    * `author.sh`: checks for `@author` tags
    * `bats.sh`: unit test for shell scripts
    * `rat.sh`: checks for Apache license header
    * `docs.sh`: sanity checks for [Ozone documentation](https://github.com/apache/ozone/tree/master/hadoop-hdds/docs)
    * `dependency.sh`: compares list of jars in build output with known list
    * `checkstyle.sh`: Checkstyle
    * `pmd.sh`: PMD
 3. moderate (around 10 minutes)
    * `findbugs.sh`: SpotBugs
    * `kubernetes.sh`: very limited set of tests run in Kubernetes environment
 4. slow (around 1 hour or more)
    * `unit.sh`: pure unit tests
    * `integration.sh`: Java-based tests using single JVM "mini cluster"
    * `acceptance.sh`: rather complete set of tests in Docker Compose-based environment

The set of tests run by `integration` and `acceptance` may be limited via arguments, please see the scripts for details.  This is used by CI to run them in multiple splits to avoid taking too much time.

Some scripts require third-party tools, but most of these are installed during the first run, if needed.

Most scripts (except `build.sh`) output results in `target/<name>`, e.g. `target/docs`.

## Using IDE

As Ozone uses Apache Maven it can be developed from any IDE.  IntelliJ IDEA is a common choice, here are some suggestions to use it for Ozone development.

### Run Ozone from IntelliJ

Ozone components depend on maven classpath. We generate classpath descriptor from the maven pom.xml files to use exactly the same classpath at runtime.

As a result, it's straightforward to start _all_ the components from IDE as the right classpath (without provided scope) has already been set.

Ozone project has pre-defined run configurations shared via standard IDE folder for run configurations:

```
.run
```

They will be automatically added to the IDE on project import.

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

#### Too large generated classes

IntelliJ may not pick up protoc generated classes as they can be very huge. If the protoc files can't be compiled try the following:

1. Open _Help_ -> _Edit custom properties_ menu.
2. Add `idea.max.intellisense.filesize=10000` entry
3. Restart your IDE

#### Bad class file

Sometimes during incremental build IDEA encounters the following error:

`bad class file: hadoop-hdds/common/target/classes/org/apache/hadoop/ozone/common/ChunkBufferImplWithByteBufferList$1.class`

Usually this can be fixed by removing the class file (outside of the IDE), but sometimes only by a full Rebuild.


## CI

The Ozone project uses Github Actions for its CI system.  The configuration is described in detail [here](.github/ci.md).
