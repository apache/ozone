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

package org.apache.hadoop.ozone.debug.logs.container.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Parses container log files and stores container details into a database.
 * Uses multithreading to process multiple log files concurrently.
 */

public class ContainerLogFileParser {

  private static final int MAX_OBJ_IN_LIST = 5000;

  private static final String LOG_FILE_MARKER = ".log.";
  private static final String LOG_LINE_SPLIT_REGEX = " \\| ";
  private static final String KEY_VALUE_SPLIT_REGEX = "=";
  private static final String KEY_ID = "ID";
  private static final String KEY_BCSID = "BCSID";
  private static final String KEY_STATE = "State";
  private static final String KEY_INDEX = "Index";
  private final AtomicBoolean hasErrorOccurred = new AtomicBoolean(false);
  
  /**
   * Scans the specified log directory, processes each file in a separate thread.
   * Expects each log filename to follow the format: dn-container-<roll over number>.log.<datanodeId>
   *
   * @param logDirectoryPath Path to the directory containing container log files.
   * @param dbstore Database object used to persist parsed container data.
   * @param threadCount Number of threads to use for parallel processing.
   */

  public void processLogEntries(String logDirectoryPath, ContainerDatanodeDatabase dbstore, int threadCount)
      throws SQLException, IOException, InterruptedException {
    try (Stream<Path> paths = Files.walk(Paths.get(logDirectoryPath))) {

      List<Path> files = paths.filter(Files::isRegularFile).collect(Collectors.toList());

      ExecutorService executorService = Executors.newFixedThreadPool(threadCount);

      CountDownLatch latch = new CountDownLatch(files.size());
      for (Path file : files) {
        Path fileNamePath = file.getFileName();
        String fileName = (fileNamePath != null) ? fileNamePath.toString() : "";
        
        int pos = fileName.indexOf(LOG_FILE_MARKER);
        if (pos == -1) {
          System.out.println("Filename format is incorrect (missing .log.): " + fileName);
          continue;
        }
        
        String datanodeId = fileName.substring(pos + 5);
        
        if (datanodeId.isEmpty()) {
          System.out.println("Filename format is incorrect, datanodeId is missing or empty: " + fileName);
          continue;
        }
        
        executorService.submit(() -> {

          String threadName = Thread.currentThread().getName();
          try {
            System.out.println(threadName + " is starting to process file: " + file.toString());
            processFile(file.toString(), dbstore, datanodeId);
          } catch (Exception e) {
            System.err.println("Thread " + threadName + " is stopping to process the file: " + file.toString() +
                " due to SQLException: " + e.getMessage());
            hasErrorOccurred.set(true);
          } finally {
            try {
              latch.countDown();
              System.out.println(threadName + " finished processing file: " + file.toString() +
                  ", Latch count after countdown: " + latch.getCount());
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        });
      }
      latch.await();

      executorService.shutdown();
      
      if (hasErrorOccurred.get()) {
        throw new SQLException("Log file processing failed.");
      }

    }
  }

  /**
   * Processes a single container log file and extracts container details.
   * Parses, batches, and writes valid container log entries into the database.
   *
   * @param logFilePath Path to the log file.
   * @param dbstore Database object used to persist parsed container data.
   * @param datanodeId Datanode ID derived from the log filename.
   */
  
  private void processFile(String logFilePath, ContainerDatanodeDatabase dbstore, String datanodeId) 
      throws SQLException, IOException {
    List<DatanodeContainerInfo> batchList = new ArrayList<>(MAX_OBJ_IN_LIST + 100);

    try (BufferedReader reader = Files.newBufferedReader(Paths.get(logFilePath), StandardCharsets.UTF_8)) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] parts = line.split(LOG_LINE_SPLIT_REGEX);
        String timestamp = parts[0].trim();
        String logLevel = parts[1].trim();
        String id = null, index = null;
        String errorMessage = "No error";

        DatanodeContainerInfo.Builder builder = new DatanodeContainerInfo.Builder()
            .setDatanodeId(datanodeId)
            .setTimestamp(timestamp)
            .setLogLevel(logLevel);

        for (int i = 2; i < parts.length; i++) {
          String part = parts[i].trim();

          if (part.contains(KEY_VALUE_SPLIT_REGEX)) {
            String[] keyValue = part.split(KEY_VALUE_SPLIT_REGEX, 2);
            if (keyValue.length == 2) {
              String key = keyValue[0].trim();
              String value = keyValue[1].trim();

              switch (key) {
              case KEY_ID:
                id = value;
                builder.setContainerId(Long.parseLong(value));
                break;
              case KEY_BCSID:
                builder.setBcsid(Long.parseLong(value));
                break;
              case KEY_STATE:
                builder.setState(value.replace("|", "").trim());
                break;
              case KEY_INDEX:
                index = value;
                builder.setIndexValue(Integer.parseInt(value));
                break;
              default:
                break;
              }
            }
          } else {
            if (!part.isEmpty()) {
              errorMessage = part.replace("|", "").trim();
            }
          }
        }
        builder.setErrorMessage(errorMessage);

        if (index == null || !index.equals("0")) {
          continue; //Currently only ratis replicated containers are considered.
        }

        if (id != null) {
          try {
            batchList.add(builder.build());

            if (batchList.size() >= MAX_OBJ_IN_LIST) {
              dbstore.insertContainerDatanodeData(batchList);
              batchList.clear();
            }
          } catch (SQLException e) {
            throw e;
          }
        } else {
          System.err.println("Log line does not have all required fields: " + line);
        }
      }
      if (!batchList.isEmpty()) {
        dbstore.insertContainerDatanodeData(batchList);
        batchList.clear();
      }

    }
  }
}
