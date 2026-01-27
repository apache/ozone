*** Settings ***
Documentation       Test multiraft shell commands
Library             OperatingSystem
Library             BuiltIn
Resource            ../commonlib.robot
Resource            ../ozone-lib/shell.robot
Test Timeout        20 minutes
Suite Setup         Prepare For Tests

*** Variables ***
${VOLUME}           multiraft-volume
${BUCKET}           multiraft-bucket
${TESTFILE}         testfilemultiraft

*** Keywords ***
Prepare For Tests
    Execute                 ozone sh volume create /${VOLUME}
    Execute                 ozone sh bucket create /${VOLUME}/${BUCKET}-1
    Execute                 ozone sh bucket create /${VOLUME}/${BUCKET}-2
    Execute                 ozone sh bucket create /${VOLUME}/${BUCKET}-3
    Execute                 ozone sh bucket create /${VOLUME}/${BUCKET}-4
    Create File             /tmp/${TESTFILE}
    Execute                 echo "This is a decommissioning test" > /tmp/${TESTFILE}

Create Multiple Keys
    [arguments]             ${BUCKET_NAME}    ${NUM_KEYS}
    FOR     ${INDEX}        IN RANGE                ${NUM_KEYS}
            ${fileName} =           Set Variable            ${TESTFILE}-${INDEX}.txt
            ${key} =    Set Variable    /${VOLUME}/${BUCKET_NAME}/${fileName}
            Create Key    ${key}    /tmp/${TESTFILE}
            Log To Console             Key created ${key}
            Key Should Match Local File    ${key}      /tmp/${TESTFILE}
            Log To Console             Key matched to local file ${key} /tmp/${TESTFILE}
    END

*** Test Cases ***
Test multiraft record files

    Create Multiple Keys    ${BUCKET}-1    10
    Create Multiple Keys    ${BUCKET}-2    10
    Create Multiple Keys    ${BUCKET}-3    10
    Create Multiple Keys    ${BUCKET}-4    10

Test multiraft rerecord files

    Create Multiple Keys    ${BUCKET}-1    10
    Create Multiple Keys    ${BUCKET}-1    10