*** Settings ***
Resource    BytemanKeywords.robot
Suite Setup    Setup All Byteman Agents

*** Test Cases ***
Test Skip Put Block on the datanode
    Inject Fault Into Component    datanode1    /opt/byteman/scripts/skip-put-block.btm
    
    # Run your Ozone operations that should be affected by delay
    Execute Ozone Command    ozone sh volume create /vol1
    Execute Ozone Command    ozone sh bucket create /vol1/buck1
    Execute Ozone Command    ozone sh volume key put /vol1/buck1/key1 /opt/byteman/scripts/skip-put-block.btm
    
    # Verify the put block was skipped
    
    Remove Fault From Component    datanode1    /opt/byteman/scripts/skip-put-block.btm
