*** Settings ***
Library    ../lib/BytemanLibrary.py
Library    String

*** Variables ***
${BYTEMAN_PORT}                   9091
${DATANODE1_BYTEMAN_HOST_PORT}    datanode1:${BYTEMAN_PORT}
${DATANODE2_BYTEMAN_HOST_PORT}    datanode2:${BYTEMAN_PORT}
${DATANODE3_BYTEMAN_HOST_PORT}    datanode3:${BYTEMAN_PORT}
${OM1_BYTEMAN_HOST_PORT}          om1:${BYTEMAN_PORT}
${OM2_BYTEMAN_HOST_PORT}          om2:${BYTEMAN_PORT}
${OM3_BYTEMAN_HOST_PORT}          om3:${BYTEMAN_PORT}
${RECON_BYTEMAN_HOST_PORT}        recon:${BYTEMAN_PORT}
${SCM1_BYTEMAN_HOST_PORT}         scm1.org:${BYTEMAN_PORT}
${SCM2_BYTEMAN_HOST_PORT}         scm2.org:${BYTEMAN_PORT}
${SCM3_BYTEMAN_HOST_PORT}         scm3:${BYTEMAN_PORT}
${HTTPFS_BYTEMAN_HOST_PORT}       httpfs:${BYTEMAN_PORT}
${S3G_BYTEMAN_HOST_PORT}          s3g:${BYTEMAN_PORT}

*** Keywords ***
Setup Byteman For Component
    [Arguments]    ${component}    ${host_port}
    ${host}    ${port} =    Split String    ${host_port}    :
    Connect To Byteman Agent    ${component}    ${host}    ${port}
    
Setup All Byteman Agents
    Log   Inside Setup All Byteman Agents
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