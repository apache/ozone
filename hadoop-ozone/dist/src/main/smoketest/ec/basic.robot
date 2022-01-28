*** Settings ***
Documentation       Test EC shell commands
Library             OperatingSystem
Resource            ../commonlib.robot
Suite Setup         Generate prefix

*** Variables ***
${SCM}       scm

*** Keywords ***

Generate prefix
    ${random} =         Generate Random String  5  [NUMBERS]
    Set Suite Variable  ${prefix}  ${random}
    Execute             dd if=/dev/urandom of=/tmp/1mb bs=1048576 count=1
    Execute             dd if=/dev/urandom of=/tmp/2mb bs=1048576 count=2
    Execute             dd if=/dev/urandom of=/tmp/3mb bs=1048576 count=3
    Execute             dd if=/dev/urandom of=/tmp/100mb bs=1048576 count=100

*** Test Cases ***

Test Bucket Creation
    ${result} =     Execute             ozone sh volume create /${prefix}vol1
                    Should not contain  ${result}       Failed
    ${result} =     Execute             ozone sh bucket create /${prefix}vol1/${prefix}ratis
                    Should not contain  ${result}       Failed
    ${result} =     Execute             ozone sh bucket list /${prefix}vol1 | jq -r '.[] | select(.name | contains("${prefix}ratis")) | .replicationConfig.replicationType'
                    Should contain      ${result}       RATIS
    ${result} =     Execute             ozone sh bucket create --replication rs-3-2-1024k --type EC /${prefix}vol1/${prefix}ec
                    Should not contain  ${result}       Failed
    ${result} =     Execute             ozone sh bucket list /${prefix}vol1 | jq -r '.[] | select(.name | contains("${prefix}ec")) | .replicationConfig.replicationType, .replicationConfig.codec, .replicationConfig.data, .replicationConfig.parity, .replicationConfig.ecChunkSize'
                    Should Match Regexp      ${result}       ^(?m)EC$
                    Should Match Regexp      ${result}       ^(?m)RS$
                    Should Match Regexp      ${result}       ^(?m)3$
                    Should Match Regexp      ${result}       ^(?m)2$
                    Should Match Regexp      ${result}       ^(?m)1048576$

Test key Creation
    ${result} =     Run and Return RC            ozone sh key put /${prefix}vol1/${prefix}ec/${prefix}1mb /tmp/1mb
                    Should Be Equal As Integers  ${result}    0
    ${result} =     Run and Return RC            ozone sh key put /${prefix}vol1/${prefix}ec/${prefix}2mb /tmp/2mb
                    Should Be Equal As Integers  ${result}    0
    ${result} =     Run and Return RC            ozone sh key put /${prefix}vol1/${prefix}ec/${prefix}3mb /tmp/3mb
                    Should Be Equal As Integers  ${result}    0
    ${result} =     Run and Return RC            ozone sh key put /${prefix}vol1/${prefix}ec/${prefix}100mb /tmp/100mb
                    Should Be Equal As Integers  ${result}    0

    # Now Get the files and check they match
    ${result} =     Run and Return RC            ozone sh key get /${prefix}vol1/${prefix}ec/${prefix}1mb /tmp/${prefix}1mb
                    Should Be Equal As Integers  ${result}    0
		    Compare files                /tmp/${prefix}1mb  /tmp/1mb
    ${result} =     Run and Return RC            ozone sh key get /${prefix}vol1/${prefix}ec/${prefix}2mb /tmp/${prefix}2mb
                    Should Be Equal As Integers  ${result}    0
		    Compare files                /tmp/${prefix}2mb  /tmp/2mb
    ${result} =     Run and Return RC            ozone sh key get /${prefix}vol1/${prefix}ec/${prefix}3mb /tmp/${prefix}3mb
                    Should Be Equal As Integers  ${result}    0
		    Compare files                /tmp/${prefix}3mb  /tmp/3mb
    ${result} =     Run and Return RC            ozone sh key get /${prefix}vol1/${prefix}ec/${prefix}100mb /tmp/${prefix}100mb
                    Should Be Equal As Integers  ${result}    0
		    Compare files                /tmp/${prefix}100mb  /tmp/100mb

    # Check one key has the correct replication details
    ${result}       Execute                      ozone sh key info /${prefix}vol1/${prefix}ec/${prefix}1mb | jq -r '.replicationConfig.replicationType, .replicationConfig.codec, .replicationConfig.data, .replicationConfig.parity, .replicationConfig.ecChunkSize'
                    Should Match Regexp      ${result}       ^(?m)EC$
                    Should Match Regexp      ${result}       ^(?m)RS$
                    Should Match Regexp      ${result}       ^(?m)3$
                    Should Match Regexp      ${result}       ^(?m)2$
                    Should Match Regexp      ${result}       ^(?m)1048576$

Test Ratis Key EC Bucket
    ${result} =     Run and Return RC            ozone sh key put --replication=THREE --type=RATIS /${prefix}vol1/${prefix}ec/${prefix}1mbRatis /tmp/1mb
                    Should Be Equal As Integers  ${result}    0
    ${result} =     Run and Return RC            ozone sh key get /${prefix}vol1/${prefix}ec/${prefix}1mbRatis /tmp/${prefix}1mbRatis
                    Should Be Equal As Integers  ${result}    0
                    Compare files                /tmp/${prefix}1mbRatis  /tmp/1mb
    ${result}       Execute                      ozone sh key info /${prefix}vol1/${prefix}ec/${prefix}1mbRatis | jq -r '.replicationConfig.replicationType'
                    Should Match Regexp          ${result}       ^(?m)RATIS$

Test EC Key Ratis Bucket
    ${result} =     Run and Return RC            ozone sh key put --replication=rs-3-2-1024k --type=EC /${prefix}vol1/${prefix}ratis/${prefix}1mbEC /tmp/1mb
                    Should Be Equal As Integers  ${result}    0
    ${result} =     Run and Return RC            ozone sh key get /${prefix}vol1/${prefix}ratis/${prefix}1mbEC /tmp/${prefix}1mbEC
                    Should Be Equal As Integers  ${result}    0
		    Compare files                /tmp/${prefix}1mbEC  /tmp/1mb
    ${result}       Execute                      ozone sh key info /${prefix}vol1/${prefix}ratis/${prefix}1mbEC | jq -r '.replicationConfig.replicationType, .replicationConfig.codec, .replicationConfig.data, .replicationConfig.parity, .replicationConfig.ecChunkSize'
                    Should Match Regexp      ${result}       ^(?m)EC$
                    Should Match Regexp      ${result}       ^(?m)RS$
                    Should Match Regexp      ${result}       ^(?m)3$
                    Should Match Regexp      ${result}       ^(?m)2$
                    Should Match Regexp      ${result}       ^(?m)1048576$
