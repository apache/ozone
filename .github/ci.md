# Ozone CI with Github Actions

The Ozone project uses Github Actions, (GA), for its CI system.  GA are implemented with workflows, which are groups of *jobs* combined to accomplish a CI task, all defined in a single yaml file.  The Ozone workflow yaml files are [here](./workflows).

## Workflows

### full-ci Workflow
This is the most important [workflow](./workflows/ci.yml).  It runs the tests that verify the latest commits.

It is triggered each time a pull request is created or synchronized (i.e. when the remote branch is pushed to).  These trigger events are defined in the [build-branch workflow](./workflows/post-commit.yml).

The full-ci workflow is divided into a number of different jobs, most of which run in parallel.  Each job is described below.

Some of the jobs are defined using GA's "build matrix" feature.  This allows you define similar jobs with a single job definition. Any differences are specified by a list of values for a specific key.  For example, the "compile" job uses the matrix feature to generate the images with different versions of java.  There, the matrix is specified by the "java" key which has a list of values describing which version of java to use, (8 or 11.)

The jobs currently using the "build matrix" feature are: "compile", "basic", "unit", "acceptance" and "integration".  These jobs also use GA's fail-fast flag to cancel the other jobs in the same matrix, if one fails. For example, in the "compile" job, if the java 8 build fails, the java 11 build will be cancelled due to this flag, but the other jobs outside the "compile" matrix are unaffected. 

While the fail-fast flag only works within a matrix job, the "Cancelling" workflow, (described below,) works across jobs.


#### build-info job

[The build-info job script](../dev-support/ci/selective_ci_checks.sh) runs before the others and determines which of the other jobs are to be run.  If the workflow was triggered by some event other than a PR, then all jobs/tests are run.  They are also all run if the PR has a label containing the following string, "full tests needed".

Otherwise, *build-info* first generates a list of files that were changed by the PR.  It matches that list against a series of regex's, each of which is associated with a different job.  It sets the appropriate flag for each match.  Those boolean flags are used later in the run to decide whether the corresponding job should be run

For example, a regex like the following is used to determine if the Kubernetes flag should be set.
```
    local pattern_array=(
        "^hadoop-ozone/dev-support/checks/kubernetes.sh"
        "^hadoop-ozone/dist/src/main/k8s"
    )
```



#### compile job
[Builds](../hadoop-ozone/dev-support/checks/build.sh) the Java 8 and 11 versions of the jars, and saves the java 8 version for some of the subsequent jobs.

#### basic job
Runs a subset of the following subjobs depending on what was selected by build-info
- author: [Verifies](../hadoop-ozone/dev-support/checks/author.sh) none of the Java files contain the @author annotation
- bats: [Checks](../hadoop-ozone/dev-support/checks/bats.sh) bash scripts, (using the [Bash Automated Testing System](https://github.com/bats-core/bats-core#bats-core-bash-automated-testing-system-2018))
- checkstyle: [Runs](../hadoop-ozone/dev-support/checks/checkstyle.sh) 'mvn checkstyle' plugin to confirm Java source abides by Ozone coding conventions
- docs: [Builds](../hadoop-ozone/dev-support/checks/docs.sh) website with [Hugo](https://gohugo.io/)
- findbugs: [Runs](../hadoop-ozone/dev-support/checks/findbugs.sh) spotbugs static analysis on bytecode
- pmd: [Runs](../hadoop-ozone/dev-support/checks/pmd.sh) PMD static analysis on project's source code
- rat (release audit tool): [Confirms](../hadoop-ozone/dev-support/checks/rat.sh) source files include licenses


#### unit job
Performs unit tests (if necessary) in two parts:
- unit: [Runs](../hadoop-ozone/dev-support/checks/unit.sh) 'mvn test' for all non integration tests
- native: [Runs](../hadoop-ozone/dev-support/checks/native.sh) 'mvn test' for all tests that require RocksDB native library to be built (few tests but longer build process)

#### dependency job
[Confirms](../hadoop-ozone/dev-support/checks/dependency.sh) that the list of jars included in the current build matches the expected ones defined [here](../hadoop-ozone/dist/src/main/license/jar-report.txt)

If they don't match, it describes how to make the updates to include the changes, (if they are intentional).  Otherwise, the changes should be removed.

#### acceptance job
[Runs](../hadoop-ozone/dev-support/checks/acceptance.sh) smoketests using robot framework and a real docker compose cluster.  There are three iterations, "secure", "unsecure", and "misc", each running in parallel, as different matrix configs.

#### kubernetes job
[Runs](../hadoop-ozone/dev-support/checks/kubernetes.sh) k8s tests

#### integration job
[Runs](../hadoop-ozone/dev-support/checks/integration.sh) 'mvn test' for all integration/minicluster tests, split into multiple subjobs, by a matrix config.

#### coverage job
[Merges](../hadoop-ozone/dev-support/checks/coverage.sh) the coverage data from the following jobs that were run earlier:
- acceptance
- basic
- integration

### close-stale-prs Workflow
[This](./workflows/close-stale-prs.yml) workflow is scheduled each night at midnight and uses the [actions/stale](https://github.com/actions/stale) to automatically manage inactive PRs. It marks PRs as stale after 21 days of inactivity and closes them 7 days later. If a stale PR receives any updates or comments, the stale label is automatically removed.

### comment-commands Workflow
[This](./workflows/comments.yaml) workflow is triggered each time a comment is added/edited to a PR.  It checks to see if the body of the comment begins with one of the following strings and, if so, invokes the corresponding command.
- /help : [Show](./comment-commands/help.sh) all the available comment commands
- /label : [Add](./comment-commands/label.sh) new label to the issue: /label "label"
- /retest : [Provide](./comment-commands/retest.sh) help on how to trigger new CI build


## Old/Deprecated Workflows
The following workflows no longer run but still exist on the [actions](https://github.com/apache/ozone/actions) page for historical reasons:
- [Build](https://github.com/apache/ozone/actions/workflows/main.yml)
- [build-branch](https://github.com/apache/ozone/actions/workflows/chaos.yml)
- [pr-check](https://github.com/apache/ozone/actions/workflows/pr.yml)

Note that the deprecated [build-branch](https://github.com/apache/ozone/actions/workflows/chaos.yml) has the same name as the current [build-branch](https://github.com/apache/ozone/actions/workflows/post-commit.yml).  (They can be distinguished by the URL.)


## Tips

- When a build of the Ozone master branch fails, its artifacts are stored [here](https://elek.github.io/ozone-build-results/).
- To trigger rerunning the tests, push a commit like this to your PR: ```git commit --allow-empty -m 'trigger new CI check'```
- [This wiki](https://cwiki.apache.org/confluence/display/OZONE/Running+Ozone+Smoke+Tests+and+Unit+Tests) contains tips on running tests locally.
- [This wiki](https://cwiki.apache.org/confluence/display/OZONE/Github+Actions+tips+and+tricks) contains tips on special handling of the CI system, such as "Executing one test multiple times", or "ssh'ing in to the CI machine while the tests are running".
