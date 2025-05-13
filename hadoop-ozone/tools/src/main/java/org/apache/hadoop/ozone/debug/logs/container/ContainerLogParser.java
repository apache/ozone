/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.debug.logs.container;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.ozone.debug.logs.container.utils.ContainerDatanodeDatabase;
import org.apache.hadoop.ozone.debug.logs.container.utils.ContainerLogFileParser;
import org.apache.hadoop.ozone.debug.logs.container.utils.SQLDBConstants;
import picocli.CommandLine;

/**
 * Parses container logs, processes them, and updates the database accordingly.
 */

@CommandLine.Command(
    name = "parse",
    description = "Parse container logs and extract key details such as datanode ID, container ID, state, " +
            "BCSID, timestamp, log level, index value, and messages (if any)."
)
public class ContainerLogParser extends AbstractSubcommand implements Callable<Void> {
  private static final int DEFAULT_THREAD_COUNT = 10;
  
  @CommandLine.Option(names = {"--path"},
      description = "Path to the folder which contains container log files to be parsed.",
      required = true)
  private String path;

  @CommandLine.Option(names = {"--thread-count"},
      description = "Thread count for concurrent log file processing.",
      defaultValue = "10")
  private int threadCount;

  @CommandLine.ParentCommand
  private ContainerLogController parent;

  @Override
  public Void call() throws Exception {
    if (threadCount <= 0) {
      out().println("Invalid threadCount value provided (" + threadCount + "). Using default value: "
          + DEFAULT_THREAD_COUNT);
      threadCount = DEFAULT_THREAD_COUNT;
    }
    
    Path logPath = Paths.get(path);
    if (!Files.exists(logPath) || !Files.isDirectory(logPath)) {
      err().println("Invalid path provided: " + path);
      return null;
    }
    
    Path providedDbPath;
    if (parent.getDbPath() == null) {
      providedDbPath = Paths.get(System.getProperty("user.dir"), SQLDBConstants.DEFAULT_DB_FILENAME);

      out().println("No database path provided. Creating new database at: " + providedDbPath);
    } else {
      providedDbPath = Paths.get(parent.getDbPath());
      Path parentDir = providedDbPath.getParent();

      if (parentDir != null && !Files.exists(parentDir)) {
        err().println("The parent directory of the provided database path does not exist: " + parentDir);
        return null;
      }
    }
    
    ContainerDatanodeDatabase cdd = new ContainerDatanodeDatabase(providedDbPath.toString());
    ContainerLogFileParser parser = new ContainerLogFileParser();
    
    cdd.createDatanodeContainerLogTable();

    parser.processLogEntries(path, cdd, threadCount);

    cdd.insertLatestContainerLogData();
    cdd.createIndexes();
    out().println("Successfully parsed the log files and updated the respective tables");

    return null;
  }

}
