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

import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
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
    if (path != null) {

      ContainerDatanodeDatabase cdd = new ContainerDatanodeDatabase();
      ContainerLogFileParser parser = new ContainerLogFileParser();
      AtomicBoolean isProcessingSuccessful = new AtomicBoolean(true);

      try {
        // Try creating the table, if this fails, mark the process as unsuccessful
        try {
          cdd.createDatanodeContainerLogTable();
        } catch (SQLException e) {
          isProcessingSuccessful.set(false);
          System.err.println("Error occurred while creating the Datanode Container Log Table: " + e.getMessage());
        }

        // If the table creation was successful, proceed with processing the log entries
        if (isProcessingSuccessful.get()) {
          try {
            parser.processLogEntries(path, cdd, threadCount);
          } catch (Exception e) {
            isProcessingSuccessful.set(false);
            System.err.println("Error occurred during processing logs: " + e.getMessage());
          }

          // Insert the latest log data, if processing was successful
          if (isProcessingSuccessful.get()) {
            try {
              cdd.insertLatestContainerLogData();
            } catch (SQLException e) {
              isProcessingSuccessful.set(false);
              System.err.println("Error occurred while inserting container log data: " + e.getMessage());
            }
          }

          if (isProcessingSuccessful.get()) {
            System.out.println("Successfully parsed the log files and updated the respective tables");
          } else {
            System.err.println("Log processing failed.");
          }
        }
      } catch (Exception e) {
        isProcessingSuccessful.set(false);
        System.err.println("An unexpected error occurred: " + e.getMessage());
      }

    } else {
      System.out.println("path to logs folder not provided");
    }

    return null;
  }

}
