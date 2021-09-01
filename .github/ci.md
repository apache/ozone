# OZONE CI with Github Actions

The Ozone project uses Github Actions, (GA), for its CI system.  GA are implemented with workflows, which ar gre groups of *jobs* combined to accomplish a CI task, all defined in a single yaml file.  The Ozone workflow yaml files are [here](./workflows).  

## Workflows

### build-branch (post-commit.yml)
[This](./workflows/post-commit.yml) is the most important workflow.  It runs the tests that verify the latest commit.  It is triggered each time a pull request is created or pushed to, as well as each time a branch in your fork is pushed to.  It also "scheduled" on the master branch twice a day, (00:30 and 12:30).  (So all build-branch runs marked "scheduled" without a branch label [here](https://github.com/apache/ozone/actions/workflows/post-commit.yml) are run on the master branch.)

The ones marked "Pull request #xxxx synchronize" are triggered by an open PR being updated with more commits, (the remote branch is being synchronized.)

The post-commit workflow, (which also has the name "build-branch" on the GA page,) is divded into a number of different jobs, most of which run in parallel.  Each of the jobs, (except "build-info" and "compile",) runs a subset of the test suite.

#### build-info

The build-info job runs before the others and determines which of the other jobs are to be run.  If the workflow was caused by some event other than a PR, then all jobs/tests are run.  Otherwise, it decides which jobs to run, by generating a list of files that are changed by the pr.  It compares that list against a series of regex's, each of which is associated with a different job.  It sets the appropriate flag for each match, which is subsequently used by the workflow to decide if the corresponding job should be run.

In addition, if the PR has a label containing the following string, "full tests needed", all jobs are run.

For example, the following regex is used to determine if the Kubernetes tests should be run:
```
    local pattern_array=(
        "^hadoop-ozone/dev-support/checks/kubernetes.sh"
        "^hadoop-ozone/dist/src/main/k8s"
    )
```
If so, this flag is set: "needs-kubernetes-tests"


#### compile
Builds the Java 8 and 11 versions of the jars, and saves the java 8 version for some of the subsequent jobs.

#### basic
Runs a subset of the following jobs depending on what was selected by build-info

##### author
Verifies none of the Java files contain the @author annotation

##### bats
Checks bash scripts, (using the [Bash Automated Testing System](https://github.com/sstephenson/bats))

##### checkstyle
Runs mvn checkstyle plugin to confirm source abides by Ozone coding conventions

##### docs
Builds website with [Hugo](https://gohugo.io/)

##### findbugs
Runs spotbugs static analysis on bytecode

##### rat (release audit tool) 
Make sure source files include licenses

##### unit
Runs mvn test for all non integration tests

#### dependency
Confirms hadoop-ozone/dist/src/main/license/bin/LICENSE.txt is up to date, (references latest jar files)

#### acceptance
smoketests using robot framework and real docker compose cluster

#### kubernetes
[k8s](./hadoop-ozone/dist/src/main/k8s/examples) tests

#### integration
mvn test for all integration/minicluster tests split into 3 subjobs:
- client
- filesystem-hdds
- ozone

#### coverage
merges the coverage of the following jobs that were run earlier
- acceptance
- basic
- integration

### Cancelling (cancel-ci.yaml)
[This](./workflows/cancel-ci.yaml) workflow is triggered each time a build-branch workflow is triggered.  It monitors that run for failure and cancel any continuing jobs after one fails.  This allows it to fail fast.

### close-prs (close-pending.yaml)
[This](./workflows/close-pending.yaml) workflow is scheduled each day at midnight; it closes PR's that have not been updated in the last 21 days, while letting the author know they are free to reopen.

### comment-commands (comments.yaml)
[This](./workflows/comments.yaml) workflow is triggered each time a PR's comment is added/edited.  It checks to see if one of the following [commands](./comment-commands) has been issued, and invokes it if so:
- /close : Close pending pull request temporary
- /help : Show all the available comment commands
- /label : add new label to the issue: /label <label>
- /pending : Add a REQUESTED_CHANGE type review to mark issue non-mergeable: /pending <reason>
- /ready : Dismiss all the blocking reviews by github-actions bot
- /retest : provide help on how to trigger new CI build


## Old/Deprecated Workflows
The following workflows are no longer exist but still exist on the [actions](https://github.com/apache/ozone/actions) page for historical reasons:
- [Build](https://github.com/apache/ozone/actions/workflows/main.yml)
- [build-branch](https://github.com/apache/ozone/actions/workflows/chaos.yml)
- [pr-check](https://github.com/apache/ozone/actions/workflows/pr.yml)

Note the the deprecated [build-branch](https://github.com/apache/ozone/actions/workflows/chaos.yml) has the same name as the current [build-branch](https://github.com/apache/ozone/actions/workflows/post-commit.yml), (but can be distinguished by the URL.)


## Tips

- When a master build fails, it's artifacts are stored (here)[https://elek.github.io/ozone-build-results/].
- To trigger rerunning the tests push a commit like this to your PR: ```git commit --allow-empty -m 'trigger new CI check'```
- [This wiki](https://cwiki.apache.org/confluence/display/OZONE/Running+Ozone+Smoke+Tests+and+Unit+Tests) contains tips on running tests locally.
- [This wiki](https://cwiki.apache.org/confluence/display/OZONE/Github+Actions+tips+and+tricks) contains tips on interacting with the CI system, such as "Executing one test multiple times", or ssh'ing in to the vm running the tests while they workflow is running.
