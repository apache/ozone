---
title: "Container Replica Debugger Tool"
date: 2025-05-19
jira: HDDS-12579
summary: Processing and querying container log files from Ozone datanodes to track state transitions. 
menu: debug
weight: 7
---
<!--
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

The tool processes container log files from Ozone datanodes to track state transitions. It provides CLI commands for querying containers based on different attributes.

## Background
Containers are the most important part of ozone. Most of the ozone operations happen on them. Containers in ozone go through different states in their lifecycle.
In the past we have seen different issues on container state. To debug problems we always use manual steps and identify problems w.r.t containers and this takes a lot of time. To optimize debugability we can show historical timeline and other information w.r.t containers.

## Solution Approach
An offline tool that would analyse the dn-container log files and help us find issues related to the containers across the datanodes in the cluster.

### Component 1: Parser
This component is responsible for processing log files, extracting relevant log entries, and storing them in batches to be inserted into the database.

**The command is as follows:**

`ozone debug log container --db=<path to db> parse --path=<path to logs folder> --thread-count=<n>`

#### Log File Parsing and Validation
- **Directory Traversal**: It recursively scans a specified directory for container log files,only files with names matching the pattern `dn-container-<roll>.log.<datanodeId>` are considered.Log files are parsed concurrently using multiple threads for efficient processing.
- **Line-by-Line Processing**: Each log line is split using a pipe delimiter and parsed into key-value components.
- **Field Extraction**: Key fields include:
  - **Timestamp**
  - **logLevel**: INFO, WARN, ERROR.
  - **ID, BCSID, State, and Index**: Extracted from key-value pairs.
- **Error Message Capture**: Any remaining unstructured part of the log line is stored as `errorMessage`, especially relevant for WARN or ERROR level logs.
- **Replication Index Filtering**: Only log entries with Index = 0 are processed. This limits current processing to Ratis-replicated containers. Future improvements may support EC replication.

### Component 2: Database
The tool uses a temporary SQLite database to store and manage information extracted from container logs. This helps organize the data so that both the complete history and the latest status of each container replica on a datanode can be analyzed easily.

#### There are 2 major tables created/maintained by the tool:
1. **DatanodeContainerLogTable — Detailed Log History**  
   This table stores a complete history of state changes for each container replica on every datanode.
   - Container ID
   - Datanode ID
   - State (such as OPEN, CLOSED, etc.)
   - BCSID (Block Commit Sequence ID)
   - Timestamp
   - Log Level
   - Error Message (if any)
   - Index Value

   The data in this table shows a complete timeline of state transitions and BCSID changes, helping to trace how each container replica evolved over time.


2. **ContainerLogTable — Latest Status Summary**  
   This table contains only the most recent state and BCSID for each unique container and datanode pair.
   - Container ID
   - Datanode ID
   - Latest State
   - Latest BCSID

   This table provides a simplified view of the current state and BCSID of all container replicas, which helps with quick status checks and summaries.

### **Component 3: CLI Commands**

#### **To List Containers Having Duplicate Open States**
This Ozone debug CLI command helps to identify containers that were opened more than the required number (for Ratis, it is 3) by tracking the first three "OPEN" states and flagging any subsequent "OPEN" state as problematic if it occurs on the same datanode or on a different datanode after the initial events.

This command displays the **Container ID** along with the count of replicas in the 'OPEN' state for each listed container. It also provides the total number of containers that have duplicate 'OPEN' state entries.

**The command is as follows:**

`ozone debug log container --db=<path to db> duplicate-open`

**Sample output:**

```
Container ID: 2187256 - OPEN state count: 4
.
.
.
Container ID: 12377064 - OPEN state count: 5
Container ID: 12377223 - OPEN state count: 5
Container ID: 12377631 - OPEN state count: 4
Container ID: 12377904 - OPEN state count: 5
Container ID: 12378161 - OPEN state count: 4
Container ID: 12378352 - OPEN state count: 5
Container ID: 12378789 - OPEN state count: 5
Container ID: 12379337 - OPEN state count: 5
Container ID: 12379489 - OPEN state count: 5
Container ID: 12380526 - OPEN state count: 5
Container ID: 12380898 - OPEN state count: 5
Container ID: 12642718 - OPEN state count: 4
Container ID: 12644806 - OPEN state count: 4
Total containers that might have duplicate OPEN state : 1579
```
<br>

#### **To Display Details of a Single Container Along with Analysis**

This ozone debug CLI command provides a complete state transition history for each replica of the container whose ID is provided via the command.  
The command also provides analysis over the container such as if the container has issues like:

  - Duplicate OPEN states
  - Mismatched replication
  - Under-replication or over-replication
  - Unhealthy replicas
  - Open-unhealthy
  - Quasi-closed stuck containers

The command provides key details such as:

  - Datanode id
  - Container id
  - BCSID
  - TimeStamp
  - Index Value
  - Message (if any associated with that particular replica)

**The command is as follows:**

`ozone debug log container --db=<path to database> info <containerID>`

**Sample output:**

```
Timestamp               | Container ID | Datanode ID | Container State  | BCSID   | Message             | Index Value
----------------------------------------------------------------------------------------------------------------------
2024-06-04 15:07:55,390 | 700          | 100         | QUASI_CLOSED     | 353807 | No error             | 0           
2024-06-04 14:50:18,177 | 700          | 150         | QUASI_CLOSED     | 353807 | No error             | 0           
2024-04-04 10:32:29,026 | 700          | 250         | OPEN             | 0      | No error             | 0           
2024-06-04 14:44:28,126 | 700          | 250         | CLOSING          | 353807 | No error             | 0           
2024-06-04 14:47:59,893 | 700          | 250         | QUASI_CLOSED     | 353807 | Ratis group removed  | 0           
2024-06-04 14:50:17,038 | 700          | 250         | QUASI_CLOSED     | 353807 | No error             | 0           
2024-06-04 14:50:18,184 | 700          | 250         | QUASI_CLOSED     | 353807 | No error             | 0           
2024-04-04 10:32:29,026 | 700          | 400         | OPEN             | 0      | No error             | 0           
Container 700 might be QUASI_CLOSED_STUCK.
```
<br>

#### **To List Containers Based on Health State**

This ozone debug CLI command lists all the containers which are either UNDER-REPLICATED, OVER-REPLICATED, UNHEALTHY, or QUASI_CLOSED stuck.

This command displays the **Container ID** along with the count of replicas in the specified health state for each listed container.

**The command options used are:**

- List containers by health type: this by default provides only 100 rows in the result

`ozone debug log container --db=<path to db> list --health=<type>`

- List containers by health type with a specified row limit:

`ozone debug log container --db=<path to db> list --health=<type>  --length=<limit>`

- List all containers by health type, overriding the row limit:

`ozone debug log container --db=<path to db> list --health=<type>  --all`

**Sample output:**

`ozone debug log container --db=path/to/db list --health=UNHEALTHY --all`

```
Container ID = 6002 - Count = 1
Container ID = 6201 - Count = 1
.
.
.
Container ID = 136662 - Count = 3
Container ID = 136837 - Count = 3
Container ID = 199954 - Count = 3
Container ID = 237747 - Count = 3
Container ID = 2579099 - Count = 1
Container ID = 2626888 - Count = 1
Container ID = 2627711 - Count = 1
Container ID = 2627751 - Count = 2
Number of containers listed: 25085
```
<br>

#### **To List Containers Based on Final State**

This ozone debug CLI command helps to list replicas of all the containers which have the state provided via the CLI command as their latest state.

The command provides key details such as:

  - Datanode id
  - Container id
  - BCSID
  - TimeStamp - The most recent timestamp associated with the container replica state
  - Index Value
  - Message (if any associated with that particular replica)

**The command options used are:**

- List containers by state: this by default provides only 100 rows in the result

`ozone debug log container --db=<path to db> list --lifecycle=<state>`
- List containers by state with a specified row limit:

`ozone debug log container --db=<path to db> list --lifecycle=<state> --length=<limit>`
- List all containers by state, overriding the row limit:

`ozone debug log container --db=<path to db> list --lifecycle=<state> --all`

**Sample output:**

`ozone debug log container --db=path/to/db list --lifecycle=CLOSED --all`

```
Timestamp                 | Datanode ID | Container ID | BCSID  | Message        | Index Value
---------------------------------------------------------------------------------------------------
2024-07-23 12:02:12,981   | 360         | 1            | 75654  | No error       | 0
2024-07-23 11:56:21,106   | 365         | 1            | 75654  | Volume failure | 0
2024-07-23 11:56:21,106   | 365         | 1            | 75654  | Volume failure | 0
2024-08-29 14:11:32,879   | 415         | 1            | 30     | No error       | 0
2024-08-29 14:11:17,533   | 430         | 1            | 30     | No error       | 0
2024-06-20 11:50:09,496   | 460         | 1            | 75654  | No error       | 0
2024-07-23 12:02:11,633   | 500         | 1            | 75654  | No error       | 0
2024-06-20 12:03:24,230   | 505         | 2            | 83751  | No error       | 0
2024-07-10 04:00:33,131   | 540         | 2            | 83751  | No error       | 0
2024-07-10 04:00:46,825   | 595         | 2            | 83751  | No error       | 0
```
<br>

### NOTE

- This tool assumes that all dn-container log files are already extracted from the datanodes and placed into a directory.
- For the parsing command, if the DB path is not provided, a new database will be created in the current working directory with the default filename `container_datanode.db`.  
- For all other commands, if the DB path is not provided, it will look for a default database (`container_datanode.db`) file in the current directory. If it doesn’t exist, it will throw an error asking to provide a valid path.
