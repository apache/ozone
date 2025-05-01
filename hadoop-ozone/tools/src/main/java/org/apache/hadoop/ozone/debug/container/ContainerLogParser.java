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

package org.apache.hadoop.ozone.debug.container;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import org.apache.hadoop.ozone.containerlog.parser.ContainerDatanodeDatabase;
import org.apache.hadoop.ozone.containerlog.parser.ContainerLogFileParser;
import picocli.CommandLine;

/**
 * Parses container logs, processes them, and updates the database accordingly.
 */

@CommandLine.Command(
    name = "container_log_parse",
    description = "parse the container logs"
)
public class ContainerLogParser implements Callable<Void> {
  private static final int DEFAULT_THREAD_COUNT = 10;
  
  @CommandLine.Option(names = {"--parse"},
      description = "path to the dir which contains log files")
  private String path;

  @CommandLine.Option(names = {"--thread-count"},
      description = "Thread count for concurrent processing.",
      defaultValue = "10")
  private int threadCount;

  @CommandLine.ParentCommand
  private ContainerLogController parent;

  @Override
  public Void call() throws Exception {
    if (threadCount <= 0) {
      System.out.println("Invalid threadCount value provided (" + threadCount + "). Using default value: "
          + DEFAULT_THREAD_COUNT);
      threadCount = DEFAULT_THREAD_COUNT;
    }
    
    if (path != null) {
      Path logPath = Paths.get(path);
      if (!Files.exists(logPath) || !Files.isDirectory(logPath)) {
        System.err.println("Invalid path provided: " + path);
        return null;
      }

      ContainerDatanodeDatabase cdd = new ContainerDatanodeDatabase();
      ContainerLogFileParser parser = new ContainerLogFileParser();

      try {

        cdd.createDatanodeContainerLogTable();

        parser.processLogEntries(path, cdd, threadCount);

        cdd.insertLatestContainerLogData();
        System.out.println("Successfully parsed the log files and updated the respective tables");

      } catch (SQLException e) {
        System.err.println("Error occurred while processing logs or inserting data into the database: "
            + e.getMessage());
      } catch (Exception e) {
        System.err.println("An unexpected error occurred: " + e.getMessage());
      }

    } else {
      System.out.println("path to logs folder not provided");
    }

    return null;
  }

}
