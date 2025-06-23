*** Variables ***
${RULE}    /opt/hadoop/compose/common/byteman-scripts/skip-put-block.btm
${vol}     vol1
${buck}    buck1
${key}     key1
${SECURITY_ENABLED}  true

*** Settings ***
Resource            BytemanKeywords.robot
Resource            os.robot
Resource            ../lib/os.robot
Resource            ../commonlib.robot
Resource            ../ozone-lib/shell.robot
Suite Setup         Setup Suite
Suite Teardown      Teardown Suite


*** Keywords ***
Setup Suite
    Setup All Byteman Agents
    Inject Fault Into Component    datanode1    ${RULE}
    Log To Console      Kinit
    Execute And Ignore Error     kinit -kt /etc/security/keytabs/testuser.keytab testuser/om


Teardown Suite
    Remove Fault From Component    datanode1    ${RULE}

*** Test Cases ***
Print Byteman Port
    ${BYTEMAN_PORT} =    Get Environment Variable    BYTEMAN_PORT
    Log    ${BYTEMAN_PORT}
