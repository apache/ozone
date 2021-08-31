# OZONE CI with Github Actions

The Ozone project uses Github Actions, (GA), for its CI system.  GA are implemented with workflows, which ar gre groups of *jobs* combined to accomplish a CI task, all defined in a single yaml file.  The Ozone workflow yaml files are [here](./workflows).  

## Workflows

### post-commit
[This](./workflows/post-commit.yml) is the most important workflow.  It runs the tests that verify the latest commit.  It runs each time a pull request is created or pushed to, as well as each time a branch in your fork is pushed to.  It also runs on the master branch twice a day, (00:30 and 12:30).

The post-commit workflow, (which also has the name "build-branch" on the GA page,) is divded into a number of different jobs, most of which run in parallel.  Each of the jobs, (except "build-info" and "compile",) runs a subset of the test suite.

#### build-info

The build-info job runs before the others and determines which of the other jobs are to be run.  If the workflow was caused by some event other than a PR, then all jobs/tests are run.  It makes that determination by generating a list of files that are changed by the pr.  


