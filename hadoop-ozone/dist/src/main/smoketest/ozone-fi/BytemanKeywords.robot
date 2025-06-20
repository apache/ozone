*** Settings ***
Library    BytemanLibrary

*** Variables ***
${DATANODE1_BYTEMAN_HOST_PORT}    172.25.0.102:9090
${DATANODE2_BYTEMAN_HOST_PORT}    172.25.0.103:9091
${DATANODE3_BYTEMAN_HOST_PORT}    172.25.0.104:9092
${OM1_BYTEMAN_HOST_PORT}          172.25.0.111:9093
${OM2_BYTEMAN_HOST_PORT}          172.25.0.112:9094
${OM3_BYTEMAN_HOST_PORT}          172.25.0.113:9095
${RECON_BYTEMAN_HOST_PORT}        172.25.0.115:9096
${SCM1_BYTEMAN_HOST_PORT}         172.25.0.116:9097
${SCM2_BYTEMAN_HOST_PORT}         172.25.0.117:9098
${SCM3_BYTEMAN_HOST_PORT}         172.25.0.118:9099
${HTTPFS_BYTEMAN_HOST_PORT}       172.25.0.119:9100
${S3G_BYTEMAN_HOST_PORT}          172.25.0.120:9101

*** Keywords ***
Setup Byteman For Component
    [Arguments]    ${component}    ${host_port}
    ${host}    ${port} =    Split String    ${host_port}    :
    Connect To Byteman Agent    ${component}    ${host}    ${port}
    
Setup All Byteman Agents
    Setup Byteman For Component    datanode1   ${DATANODE1_BYTEMAN_HOST_PORT}
    Setup Byteman For Component    datanode2   ${DATANODE2_BYTEMAN_HOST_PORT}
    Setup Byteman For Component    datanode3   ${DATANODE3_BYTEMAN_HOST_PORT}
    Setup Byteman For Component    om1         ${OM1_BYTEMAN_HOST_PORT}
    Setup Byteman For Component    om2         ${OM2_BYTEMAN_HOST_PORT}
    Setup Byteman For Component    om3         ${OM3_BYTEMAN_HOST_PORT}
    Setup Byteman For Component    recon       ${RECON_BYTEMAN_HOST_PORT}
    Setup Byteman For Component    scm1        ${SCM1_BYTEMAN_HOST_PORT}
    Setup Byteman For Component    scm2        ${SCM2_BYTEMAN_HOST_PORT}
    Setup Byteman For Component    scm3        ${SCM3_BYTEMAN_HOST_PORT}
    Setup Byteman For Component    https       ${HTTPFS_BYTEMAN_HOST_PORT}
    Setup Byteman For Component    s3g         ${S3G_BYTEMAN_HOST_PORT}

Inject Fault Into Component
    [Arguments]    ${component}    ${rule_file}
    Add Byteman Rule    ${component}    ${rule_file}
    
Remove Fault From Component
    [Arguments]    ${component}    ${rule_file}
    Remove Byteman Rule    ${component}    ${rule_file}

Verify Byteman Rules Active
    [Arguments]    ${component}
    ${rules} =    List all Byteman Rules    ${component}
    Should Not Be Empty    ${rules}