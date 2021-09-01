# OZONE CI with Github Actions

The Ozone project uses Github Actions, (GA), for its CI system.  GA are implemented with workflows, which ar gre groups of *jobs* combined to accomplish a CI task, all defined in a single yaml file.  The Ozone workflow yaml files are [here](./workflows).  

## Workflows

### post-commit
[This](./workflows/post-commit.yml) is the most important workflow.  It runs the tests that verify the latest commit.  It runs each time a pull request is created or pushed to, as well as each time a branch in your fork is pushed to.  It also runs on the master branch twice a day, (00:30 and 12:30).

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

#### bats
Checks bash scripts, (using the [Bash Automated Testing System](https://github.com/sstephenson/bats))

#### checkstyle
Runs mvn checkstyle plugin to confirm source abides by Ozone coding conventions





