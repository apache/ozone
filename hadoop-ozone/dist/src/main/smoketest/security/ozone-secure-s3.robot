#suite: ozonesecure

*** Settings ***
Suite Setup       Generate S3 Credentials
Test Teardown     Cleanup S3 Credentials
Resource          ./common.robot

*** Test Cases ***
SetSecret Success
    ${output} =         Execute          ozone sh s3 setsecret --secret=newsecret
    Should Contain    ${output}    AWS_ACCESS_KEY_ID
    Should Contain    ${output}    AWS_SECRET_ACCESS_KEY