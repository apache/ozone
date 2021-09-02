# Ozone CI with Github Actions

The Ozone project uses Github Actions, (GA), for its CI system.  GA are implemented with workflows, which are groups of *jobs* combined to accomplish a CI task, all defined in a single yaml file.  The Ozone workflow yaml files are [here](./workflows).

## Workflows

### build-branch Workflow
[This](./workflows/post-commit.yml) is the most important workflow.  It runs the tests that verify the latest commit.  It is triggered each time a pull request is created or pushed to, as well as each time a branch in your fork is pushed to.  It also "scheduled" on the master branch twice a day, (00:30 and 12:30).  (So all build-branch runs [here](https://github.com/apache/ozone/actions/workflows/post-commit.yml?query=event%3Aschedule++) which are marked "scheduled", and have no branch label, are run on the master branch.)

The ones marked "Pull request #xxxx synchronize" are triggered by an open PR being updated with more commits, (i.e. the remote branch is being synchronized.)

This workflow, (which has the name "build-branch" on the GA page,) is divided into a number of different jobs, most of which run in parallel.  Each of the jobs, (except "build-info" and "compile",) runs a subset of the test suite.  They are all described below.

#### build-info

[The build-info job script](../dev-support/ci/selective_ci_checks.sh) runs before the others and determines which of the other jobs are to be run.  If the workflow was triggered by some event other than a PR, then all jobs/tests are run.  All jobs are also run if the PR has a label containing the following string, "full tests needed".

Otherwise, build-info first generates a list of files that were changed by the PR.  It matches that list against a series of regex's, each of which is associated with a different job.  It sets the appropriate flag for each match.  Before starting each of the subsequent jobs, the workflow checks the corresponding flag is set.

As an example, a regex like the following is used to determine if the Kubernetes tests should be run:
```
    local pattern_array=(
        "^hadoop-ozone/dev-support/checks/kubernetes.sh"
        "^hadoop-ozone/dist/src/main/k8s"
    )
```



#### compile
[Builds](../hadoop-ozone/dev-support/checks/build.sh) the Java 8 and 11 versions of the jars, and saves the java 8 version for some of the subsequent jobs.

#### basic
Runs a subset of the following subjobs depending on what was selected by build-info
- author: [Verifies](../hadoop-ozone/dev-support/checks/author.sh) none of the Java files contain the @author annotation
- bats: [Checks](../hadoop-ozone/dev-support/checks/bats.sh) bash scripts, (using the [Bash Automated Testing System](https://github.com/bats-core/bats-core#bats-core-bash-automated-testing-system-2018))
- checkstyle: [Runs](../hadoop-ozone/dev-support/checks/checkstyle.sh) 'mvn checkstyle' plugin to confirm Java source abides by Ozone coding conventions
- docs: [Builds](../hadoop-ozone/dev-support/checks/docs.sh) website with [Hugo](https://gohugo.io/)
- findbugs: [Runs](../hadoop-ozone/dev-support/checks/findbugs.sh) spotbugs static analysis on bytecode
- rat (release audit tool): [Confirms](../hadoop-ozone/dev-support/checks/rat.sh) source files include licenses
- unit: [Runs](../hadoop-ozone/dev-support/checks/unit.sh) 'mvn test' for all non integration tests

#### dependency
[Confirms](../hadoop-ozone/dev-support/checks/dependency.sh) hadoop-ozone/dist/src/main/license/bin/LICENSE.txt is up to date, (includes references to all the latest jar files).

#### acceptance
[Runs](../hadoop-ozone/dev-support/checks/acceptance.sh) smoketests using robot framework and a real docker compose cluster

#### kubernetes
[Runs](../hadoop-ozone/dev-support/checks/kubernetes.sh) k8s tests

#### integration
[Runs](../hadoop-ozone/dev-support/checks/integration.sh) 'mvn test' for all integration/minicluster tests, split into 3 subjobs:
- client
- filesystem-hdds
- ozone

#### coverage
[Merges](../hadoop-ozone/dev-support/checks/coverage.sh) the coverage data from the following jobs that were run earlier:
- acceptance
- basic
- integration

### Cancelling Workflow
[This](./workflows/cancel-ci.yaml) workflow is triggered each time a [build-branch](ci.md#build-branch-workflow) workflow is triggered.  It monitors that workflow run for failure and cancels any continuing jobs after one fails.  This allows reduces our GA usage.

### close-prs Workflow
[This](./workflows/close-pending.yaml) workflow is scheduled each night at midnight; it closes PR's that have not been updated in the last 21 days, while letting the author know they are free to reopen.

### comment-commands Workflow
[This](./workflows/comments.yaml) workflow is triggered each time a comment is added/edited to a PR.  It checks to see if the body of the comment begins with one of the following strings and, if so, invokes the corresponding command.
- /close : [Close](./comment-commands/close.sh) pending pull request (with message saying author is free to reopen.)
- /help : [Show](./comment-commands/help.sh) all the available comment commands
- /label : [Add](./comment-commands/label.sh) new label to the issue: /label "label"
- /pending : [Add](./comment-commands/pending.sh) a REQUESTED_CHANGE type review to mark issue non-mergeable: /pending "reason"
- /ready : [Dismiss](./comment-commands/ready.sh) all the blocking reviews
- /retest : [Provide](./comment-commands/retest.sh) help on how to trigger new CI build


## Old/Deprecated Workflows
The following workflows no longer run but still exist on the [actions](https://github.com/apache/ozone/actions) page for historical reasons:
- [Build](https://github.com/apache/ozone/actions/workflows/main.yml)
- [build-branch](https://github.com/apache/ozone/actions/workflows/chaos.yml)
- [pr-check](https://github.com/apache/ozone/actions/workflows/pr.yml)

Note the the deprecated [build-branch](https://github.com/apache/ozone/actions/workflows/chaos.yml) has the same name as the current [build-branch](https://github.com/apache/ozone/actions/workflows/post-commit.yml).  (They can be distinguished by the URL.)


## Tips

- When a build of the Ozone master branch fails, it's artifacts are stored [here](https://elek.github.io/ozone-build-results/).
- To trigger rerunning the tests, push a commit like this to your PR: ```git commit --allow-empty -m 'trigger new CI check'```
- [This wiki](https://cwiki.apache.org/confluence/display/OZONE/Running+Ozone+Smoke+Tests+and+Unit+Tests) contains tips on running tests locally.
- [This wiki](https://cwiki.apache.org/confluence/display/OZONE/Github+Actions+tips+and+tricks) contains tips on special handling of the CI system, such as "Executing one test multiple times", or "ssh'ing in to the CI machine while the tests are running".
