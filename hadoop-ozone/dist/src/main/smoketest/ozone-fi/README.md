<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Ozone Byteman Fault Injection Testing with Robot Framework

This directory contains Robot Framework test suites for performing fault injection testing in Apache Ozone using Byteman.

## Overview

Byteman is a Java bytecode manipulation tool that allows you to inject faults, delays, and other behaviors into running Java applications without modifying the source code. This testing framework uses Robot Framework to orchestrate Byteman operations across Ozone cluster components.

## Prerequisites

- Docker and Docker Compose
- Apache Ozone cluster running with Byteman agents enabled
- Robot Framework (automatically installed in the test containers)
- Byteman tools (bmsubmit) available in the test environment

## Architecture

### Components

The fault injection framework consists of:

1. **BytemanLibrary.py** - Python library providing Byteman operations
2. **BytemanKeywords.robot** - Robot Framework keywords for common operations
3. **Test files** - Specific test scenarios using the keywords

### Supported Components

- **Datanodes**: `datanode1`, `datanode2`, `datanode3`
- **OzoneManagers**: `om1`, `om2`, `om3`
- **StorageContainerManagers**: `scm1`, `scm2`, `scm3`
- **Other services**: `recon`, `s3g`, `httpfs`

## Usage

### Basic Test Structure

```robot
*** Settings ***
Resource    BytemanKeywords.robot
Suite Setup    Setup Test Environment
Suite Teardown    Cleanup Test Environment

*** Variables ***
${RULE_FILE}    /opt/hadoop/share/ozone/byteman/my-rule.btm

*** Test Cases ***
My Fault Injection Test
    Add Byteman Rule          datanode1     ${RULE_FILE}
    # Run your test operations here
    Remove Byteman Rule       datanode1     ${RULE_FILE}
```

### Group Operations
| Keyword | Description | Example |
|---------|-------------|---------|
| `Inject Fault Into All Components` | Inject rule into all components | `Inject Fault Into All Components    ${rule}` |
| `Remove Fault From All Components` | Remove rule from all components | `Remove Fault From All Components    ${rule}` |
| `Inject Fault Into Datanodes Only` | Inject rule into datanodes only | `Inject Fault Into Datanodes Only    ${rule}` |
| `Inject Fault Into OMs Only` | Inject rule into OMs only | `Inject Fault Into OMs Only    ${rule}` |
| `Inject Fault Into SCMs Only` | Inject rule into SCMs only | `Inject Fault Into SCMs Only    ${rule}` |

### Individual Operations
| Keyword | Description | Example |
|---------|-------------|---------|
| `Add Byteman Rule` | Add rule to specific component | `Add Byteman Rule    datanode1    ${rule}` |
| `Remove Byteman Rule` | Remove rule from specific component | `Remove Byteman Rule    datanode1    ${rule}` |
| `List Byteman Rules` | List active rules for component | `List Byteman Rules    datanode1` |
| `Remove All Byteman Rules` | Remove all rules from component | `Remove All Byteman Rules    datanode1` |

### Bulk Operations
| Keyword | Description | Example |
|---------|-------------|---------|
| `List Byteman Rules for All Components` | List rules for all components | `List Byteman Rules for All Components` |
| `Remove All Rules From All Components` | Remove all rules from all components | `Remove All Rules From All Components` |


## Byteman Rules

### Rule Location

Byteman rules are stored in: `/opt/hadoop/share/ozone/byteman/`

### Available Rules

- `skip-put-block.btm` - Blocks putBlock operations in BlockManagerImpl
- `skip-notify-group-remove.btm` - Skips notifyGroupRemove in ContainerStateMachine
- Custom rules can be added to this directory

### Environment Variables

- `BYTEMAN_PORT` - Port for Byteman agent communication (default: 9091)
- `BYTEMAN_HOME` - Byteman installation directory

### Component Variables

The framework defines component groups:

```robot
@{ALL_COMPONENTS}       datanode1 datanode2 datanode3 om1 om2 om3 recon scm1 scm2 scm3 s3g
@{DATANODE_COMPONENTS}  datanode1 datanode2 datanode3
@{OM_COMPONENTS}        om1 om2 om3
@{SCM_COMPONENTS}       scm1 scm2 scm3
```

## Running Tests

### Local Development

```bash
# Navigate to compose directory
cd hadoop-ozone/dist/src/main/compose/ozonesecure-ha

# Start cluster with Byteman enabled
export COMPOSE_FILE=docker-compose.yaml:byteman.yaml
docker-compose up -d

# Run fault injection tests
./test-byteman.sh
```

### CI/CD Integration

```bash
# Run specific test suite
execute_robot_test om1 ozone-fi/byteman_faults_sample.robot
```

## Troubleshooting

### Common Issues

1. **bmsubmit not found**
   - Ensure Byteman is properly installed in the container
   - Check PATH includes `/usr/local/bin`

2. **Connection refused**
   - Verify Byteman agents are running on target components
   - Check BYTEMAN_PORT configuration

3. **Rule file not found**
   - Ensure rule files are mounted in the container
   - Verify path `/opt/hadoop/share/ozone/byteman/`

4. **Classpath errors**
   - Rebuild Ozone distribution: `mvn clean install -DskipTests`
   - Ensure all required JARs are present

### Debugging

Enable verbose logging:

```robot
*** Settings ***
Library    BytemanLibrary.py    WITH NAME    Byteman
Library    OperatingSystem

*** Test Cases ***
Debug Byteman Operations
    ${output} =    Execute    bmsubmit -p 9091 -h datanode1 -l
    Log    ${output}
```

### Validation

Check if Byteman agents are running:

```bash
# Check if agent is listening
docker exec datanode1 netstat -ln | grep 9091

# List active rules
docker exec datanode1 bmsubmit -p 9091 -l
```

## Best Practices

1. **Always clean up** - Use Suite Teardown to remove injected faults
2. **Use specific components** - Target specific services rather than all components when possible
3. **Error handling** - Use `Run Keyword And Continue On Failure` for bulk operations
4. **Documentation** - Document expected behavior and fault scenarios
5. **Isolation** - Ensure tests don't interfere with each other

## Contributing

When adding new test cases:

1. Follow the existing keyword naming conventions
2. Add appropriate documentation strings
3. Include both positive and negative test scenarios
4. Ensure proper cleanup in teardown methods
5. Test with different cluster configurations

## References

- [Byteman Documentation](http://byteman.jboss.org/)
- [Robot Framework User Guide](https://robotframework.org/robotframework/latest/RobotFrameworkUserGuide.html)
- [Apache Ozone Documentation](https://ozone.apache.org/) 