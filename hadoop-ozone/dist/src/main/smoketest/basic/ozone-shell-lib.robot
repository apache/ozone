# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

*** Settings ***
Documentation       Test ozone shell CLI usage
Library             OperatingSystem
Resource            ../commonlib.robot

*** Variables ***
${prefix}    generated

*** Keywords ***

Generate prefix
   ${random} =         Generate Random String  5  [NUMBERS]
   Set Suite Variable  ${prefix}  ${random}

Test ozone shell
    [arguments]     ${protocol}         ${server}       ${volume}
    ${result} =     Execute And Ignore Error    ozone sh volume info ${protocol}${server}/${volume}
                    Should contain      ${result}       VOLUME_NOT_FOUND
    ${result} =     Execute             ozone sh volume create ${protocol}${server}/${volume} --space-quota 100TB --namespace-quota 100
                    Should not contain  ${result}       Failed
    ${result} =     Execute             ozone sh volume list ${protocol}${server}/ | jq -r '. | select(.name=="${volume}")'
                    Should contain      ${result}       creationTime
    ${result} =     Execute             ozone sh volume list | jq -r '. | select(.name=="${volume}")'
                    Should contain      ${result}       creationTime
# TODO: Disable updating the owner, acls should be used to give access to other user.
                    Execute             ozone sh volume setquota ${protocol}${server}/${volume} --space-quota 10TB --namespace-quota 100
#    ${result} =     Execute             ozone sh volume info ${protocol}${server}/${volume} | jq -r '. | select(.volumeName=="${volume}") | .owner | .name'
#                    Should Be Equal     ${result}       bill
    ${result} =     Execute             ozone sh volume info ${protocol}${server}/${volume} | jq -r '. | select(.name=="${volume}") | .quotaInBytes'
                    Should Be Equal     ${result}       10995116277760
                    Execute             ozone sh bucket create ${protocol}${server}/${volume}/bb1 --space-quota 10TB --namespace-quota 100
    ${result} =     Execute             ozone sh bucket info ${protocol}${server}/${volume}/bb1 | jq -r '. | select(.name=="bb1") | .storageType'
                    Should Be Equal     ${result}       DISK
    ${result} =     Execute             ozone sh bucket info ${protocol}${server}/${volume}/bb1 | jq -r '. | select(.name=="bb1") | .quotaInBytes'
                    Should Be Equal     ${result}       10995116277760
    ${result} =     Execute             ozone sh bucket info ${protocol}${server}/${volume}/bb1 | jq -r '. | select(.name=="bb1") | .quotaInNamespace'
                    Should Be Equal     ${result}       100
                    Execute             ozone sh bucket setquota ${protocol}${server}/${volume}/bb1 --space-quota 1TB --namespace-quota 1000
    ${result} =     Execute             ozone sh bucket info ${protocol}${server}/${volume}/bb1 | jq -r '. | select(.name=="bb1") | .quotaInBytes'
                    Should Be Equal     ${result}       1099511627776
    ${result} =     Execute             ozone sh bucket info ${protocol}${server}/${volume}/bb1 | jq -r '. | select(.name=="bb1") | .quotaInNamespace'
                    Should Be Equal     ${result}       1000
    ${result} =     Execute             ozone sh bucket list ${protocol}${server}/${volume}/ | jq -r '. | select(.name=="bb1") | .volumeName'
                    Should Be Equal     ${result}       ${volume}
                    Run Keyword         Test key handling       ${protocol}       ${server}       ${volume}
                    Execute             ozone sh volume clrquota --space-quota ${protocol}${server}/${volume}
    ${result} =     Execute             ozone sh volume info ${protocol}${server}/${volume} | jq -r '. | select(.name=="${volume}") | .quotaInBytes'
                    Should Be Equal     ${result}       -1
                    Execute             ozone sh volume clrquota --namespace-quota ${protocol}${server}/${volume}
    ${result} =     Execute             ozone sh volume info ${protocol}${server}/${volume} | jq -r '. | select(.name=="${volume}") | .quotaInNamespace'
                    Should Be Equal     ${result}       -1
                    Execute             ozone sh bucket clrquota --space-quota ${protocol}${server}/${volume}/bb1
    ${result} =     Execute             ozone sh bucket info ${protocol}${server}/${volume}/bb1 | jq -r '. | select(.name=="bb1") | .quotaInBytes'
                    Should Be Equal     ${result}       -1
                    Execute             ozone sh bucket clrquota --namespace-quota ${protocol}${server}/${volume}/bb1
    ${result} =     Execute             ozone sh bucket info ${protocol}${server}/${volume}/bb1 | jq -r '. | select(.name=="bb1") | .quotaInNamespace'
                    Should Be Equal     ${result}       -1
                    Execute             ozone sh bucket delete ${protocol}${server}/${volume}/bb1
                    Execute             ozone sh volume delete ${protocol}${server}/${volume}
                    Execute             ozone sh volume create ${protocol}${server}/${volume}
    ${result} =     Execute             ozone sh volume info ${protocol}${server}/${volume} | jq -r '. | select(.name=="${volume}") | .quotaInBytes'
                    Should Be Equal     ${result}       -1
    ${result} =     Execute             ozone sh volume info ${protocol}${server}/${volume} | jq -r '. | select(.name=="${volume}") | .quotaInNamespace'
                    Should Be Equal     ${result}       -1
                    Execute             ozone sh bucket create ${protocol}${server}/${volume}/bb1
    ${result} =     Execute             ozone sh bucket info ${protocol}${server}/${volume}/bb1 | jq -r '. | select(.name=="bb1") | .quotaInBytes'
                    Should Be Equal     ${result}       -1
    ${result} =     Execute             ozone sh bucket info ${protocol}${server}/${volume}/bb1 | jq -r '. | select(.name=="bb1") | .quotaInNamespace'
                    Should Be Equal     ${result}       -1
                    Execute             ozone sh volume setquota ${protocol}${server}/${volume} --space-quota 0TB --namespace-quota 0
    ${result} =     Execute             ozone sh volume info ${protocol}${server}/${volume} | jq -r '. | select(.name=="${volume}") | .quotaInBytes'
                    Should Be Equal     ${result}       -1
    ${result} =     Execute             ozone sh volume info ${protocol}${server}/${volume} | jq -r '. | select(.name=="${volume}") | .quotaInNamespace'
                    Should Be Equal     ${result}       -1
                    Execute             ozone sh bucket setquota ${protocol}${server}/${volume}/bb1 --space-quota 0TB --namespace-quota 0
    ${result} =     Execute             ozone sh bucket info ${protocol}${server}/${volume}/bb1 | jq -r '. | select(.name=="bb1") | .quotaInBytes'
                    Should Be Equal     ${result}       -1
    ${result} =     Execute             ozone sh bucket info ${protocol}${server}/${volume}/bb1 | jq -r '. | select(.name=="bb1") | .quotaInNamespace'
                    Should Be Equal     ${result}       -1
                    Execute             ozone sh bucket delete ${protocol}${server}/${volume}/bb1
                    Execute             ozone sh volume delete ${protocol}${server}/${volume}

Test Volume Acls
    [arguments]     ${protocol}         ${server}       ${volume}
    Execute         ozone sh volume create ${protocol}${server}/${volume}
    ${result} =     Execute             ozone sh volume getacl ${protocol}${server}/${volume}
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \".*\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"ALL\" .
    ${result} =     Execute             ozone sh volume addacl ${protocol}${server}/${volume} -a user:superuser1:rwxy[DEFAULT]
    ${result} =     Execute             ozone sh volume getacl ${protocol}${server}/${volume}
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \"superuser1*\",\n.*\"aclScope\" : \"DEFAULT\",\n.*\"aclList\" : . \"READ\", \"WRITE\", \"READ_ACL\", \"WRITE_ACL\" .
    ${result} =     Execute             ozone sh volume removeacl ${protocol}${server}/${volume} -a user:superuser1:xy
    ${result} =     Execute             ozone sh volume getacl ${protocol}${server}/${volume}
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \"superuser1\",\n.*\"aclScope\" : \"DEFAULT\",\n.*\"aclList\" : . \"READ\", \"WRITE\", \"READ_ACL\", \"WRITE_ACL\" .
    ${result} =     Execute             ozone sh volume setacl ${protocol}${server}/${volume} -al user:superuser1:rwxy,group:superuser1:a,user:testuser/scm@EXAMPLE.COM:rwxyc,group:superuser1:a[DEFAULT]
    ${result} =     Execute             ozone sh volume getacl ${protocol}${server}/${volume}
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \"superuser1*\",\n.*\"aclScope\" : \"DEFAULT\",\n.*\"aclList\" : . \"READ\", \"WRITE\", \"READ_ACL\", \"WRITE_ACL\" .
    Should Match Regexp                 ${result}       \"type\" : \"GROUP\",\n.*\"name\" : \"superuser1\",\n.*\"aclScope\" : \"DEFAULT\",\n.*\"aclList\" : . \"ALL\" .

Test Bucket Acls
    [arguments]     ${protocol}         ${server}       ${volume}
    Execute             ozone sh bucket create ${protocol}${server}/${volume}/bb1
    ${result} =     Execute             ozone sh bucket getacl ${protocol}${server}/${volume}/bb1
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \".*\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"ALL\" .
    ${result} =     Execute             ozone sh bucket addacl ${protocol}${server}/${volume}/bb1 -a user:superuser1:rwxy
    ${result} =     Execute             ozone sh bucket getacl ${protocol}${server}/${volume}/bb1
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \"superuser1*\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"READ\", \"WRITE\", \"READ_ACL\", \"WRITE_ACL\"
    ${result} =     Execute             ozone sh bucket removeacl ${protocol}${server}/${volume}/bb1 -a user:superuser1:xy
    ${result} =     Execute             ozone sh bucket getacl ${protocol}${server}/${volume}/bb1
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \"superuser1\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"READ\", \"WRITE\"
    ${result} =     Execute             ozone sh bucket setacl ${protocol}${server}/${volume}/bb1 -al user:superuser1:rwxy,group:superuser1:a,user:testuser/scm@EXAMPLE.COM:rwxyc,group:superuser1:a[DEFAULT]
    ${result} =     Execute             ozone sh bucket getacl ${protocol}${server}/${volume}/bb1
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \"superuser1*\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"READ\", \"WRITE\", \"READ_ACL\", \"WRITE_ACL\"
    Should Match Regexp                 ${result}       \"type\" : \"GROUP\",\n.*\"name\" : \"superuser1\",\n.*\"aclScope\" : \"DEFAULT\",\n.*\"aclList\" : . \"ALL\" .


Test key handling
    [arguments]     ${protocol}         ${server}       ${volume}
                    Execute             ozone sh key put ${protocol}${server}/${volume}/bb1/key1 /opt/hadoop/NOTICE.txt
                    Execute             rm -f /tmp/NOTICE.txt.1
                    Execute             ozone sh key get ${protocol}${server}/${volume}/bb1/key1 /tmp/NOTICE.txt.1
                    Execute             diff -q /opt/hadoop/NOTICE.txt /tmp/NOTICE.txt.1

                    Execute             ozone sh key put -t RATIS ${protocol}${server}/${volume}/bb1/key1_RATIS /opt/hadoop/NOTICE.txt
                    Execute             rm -f /tmp/key1_RATIS
                    Execute             ozone sh key get ${protocol}${server}/${volume}/bb1/key1_RATIS /tmp/key1_RATIS
                    Execute             diff -q /opt/hadoop/NOTICE.txt /tmp/key1_RATIS
    ${result} =     Execute             ozone sh key info ${protocol}${server}/${volume}/bb1/key1_RATIS | jq -r '. | select(.name=="key1_RATIS")'
                    Should contain      ${result}       RATIS
                    Execute             ozone sh key delete ${protocol}${server}/${volume}/bb1/key1_RATIS

                    Execute             ozone sh key cp ${protocol}${server}/${volume}/bb1 key1 key1-copy
                    Execute             rm -f /tmp/key1-copy
                    Execute             ozone sh key get ${protocol}${server}/${volume}/bb1/key1-copy /tmp/key1-copy
                    Execute             diff -q /opt/hadoop/NOTICE.txt /tmp/key1-copy
                    Execute             ozone sh key delete ${protocol}${server}/${volume}/bb1/key1-copy

    ${result} =     Execute And Ignore Error    ozone sh key get ${protocol}${server}/${volume}/bb1/key1 /tmp/NOTICE.txt.1
                    Should Contain      ${result}       NOTICE.txt.1 exists
    ${result} =     Execute             ozone sh key get --force ${protocol}${server}/${volume}/bb1/key1 /tmp/NOTICE.txt.1
                    Should Not Contain  ${result}       NOTICE.txt.1 exists
    ${result} =     Execute             ozone sh key info ${protocol}${server}/${volume}/bb1/key1 | jq -r '. | select(.name=="key1")'
                    Should contain      ${result}       creationTime
    ${result} =     Execute             ozone sh key list ${protocol}${server}/${volume}/bb1 | jq -r '. | select(.name=="key1") | .name'
                    Should Be Equal     ${result}       key1
                    Execute             ozone sh key rename ${protocol}${server}/${volume}/bb1 key1 key2
    ${result} =     Execute             ozone sh key list ${protocol}${server}/${volume}/bb1 | jq -r '.name'
                    Should Be Equal     ${result}       key2
                    Execute             ozone sh key delete ${protocol}${server}/${volume}/bb1/key2

Test key Acls
    [arguments]     ${protocol}         ${server}       ${volume}
    Execute         ozone sh key put ${protocol}${server}/${volume}/bb1/key2 /opt/hadoop/NOTICE.txt
    ${result} =     Execute             ozone sh key getacl ${protocol}${server}/${volume}/bb1/key2
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \".*\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"ALL\" .
    ${result} =     Execute             ozone sh key addacl ${protocol}${server}/${volume}/bb1/key2 -a user:superuser1:rwxy
    ${result} =     Execute             ozone sh key getacl ${protocol}${server}/${volume}/bb1/key2
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \"superuser1\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"READ\", \"WRITE\", \"READ_ACL\", \"WRITE_ACL\"
    ${result} =     Execute             ozone sh key removeacl ${protocol}${server}/${volume}/bb1/key2 -a user:superuser1:xy
    ${result} =     Execute             ozone sh key getacl ${protocol}${server}/${volume}/bb1/key2
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \"superuser1\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"READ\", \"WRITE\"
    ${result} =     Execute             ozone sh key setacl ${protocol}${server}/${volume}/bb1/key2 -al user:superuser1:rwxy,group:superuser1:a,user:testuser/scm@EXAMPLE.COM:rwxyc
    ${result} =     Execute             ozone sh key getacl ${protocol}${server}/${volume}/bb1/key2
    Should Match Regexp                 ${result}       \"type\" : \"USER\",\n.*\"name\" : \"superuser1\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"READ\", \"WRITE\", \"READ_ACL\", \"WRITE_ACL\"
    Should Match Regexp                 ${result}       \"type\" : \"GROUP\",\n.*\"name\" : \"superuser1\",\n.*\"aclScope\" : \"ACCESS\",\n.*\"aclList\" : . \"ALL\" .
