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

package org.apache.hadoop.ozone.containerlog.parser;

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
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Container log file Parsing logic.
 */

public class ContainerLogFileParser {

  private ExecutorService executorService;
  private static final int MAX_OBJ_IN_LIST = 5000;

  private static final String FILENAME_PARTS_REGEX = "-";
  private static final String DATANODE_ID_REGEX = "\\.";
  private static final String LOG_LINE_SPLIT_REGEX = " \\| ";
  private static final String KEY_VALUE_SPLIT_REGEX = "=";
  private static final String KEY_ID = "ID";
  private static final String KEY_BCSID = "BCSID";
  private static final String KEY_STATE = "State";
  private static final String KEY_INDEX = "Index";

  public void processLogEntries(String logDirectoryPath, ContainerDatanodeDatabase dbstore, int threadCount)
      throws SQLException {
    try (Stream<Path> paths = Files.walk(Paths.get(logDirectoryPath))) {

      List<Path> files = paths.filter(Files::isRegularFile).collect(Collectors.toList());

      executorService = Executors.newFixedThreadPool(threadCount);

      CountDownLatch latch = new CountDownLatch(files.size());
      for (Path file : files) {
        Path fileNamePath = file.getFileName();
        String fileName = (fileNamePath != null) ? fileNamePath.toString() : "";
        String[] parts = fileName.split(FILENAME_PARTS_REGEX);

        if (parts.length < 3) {
          System.out.println("Filename format is incorrect (not enough parts): " + fileName);
          continue;
        }

        String datanodeIdStr = parts[2];
        if (datanodeIdStr.contains(".log")) {
          datanodeIdStr = datanodeIdStr.split(DATANODE_ID_REGEX)[0];
        }

        long datanodeId = Long.parseLong(datanodeIdStr);

        executorService.submit(() -> {

          String threadName = Thread.currentThread().getName();
          try {
            System.out.println(threadName + " is starting to process file: " + file.toString());
            processFile(file.toString(), dbstore, datanodeId);
          } catch (Exception e) {
            System.err.println("Thread " + threadName + " is stopping to process the file: " + file.toString() +
                " due to SQLException: " + e.getMessage());
            executorService.shutdown();
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

    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    } catch (NumberFormatException e) {
      System.err.println("Invalid datanode ID");
    }
  }

  private void processFile(String logFilePath, ContainerDatanodeDatabase dbstore, long datanodeId) throws SQLException {
    List<DatanodeContainerInfo> batchList = new ArrayList<>(5100);

    try (BufferedReader reader = Files.newBufferedReader(Paths.get(logFilePath), StandardCharsets.UTF_8)) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] parts = line.split(LOG_LINE_SPLIT_REGEX);
        String timestamp = parts[0].trim();
        String logLevel = parts[1].trim();
        String id = null, index = null;

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
              builder.setErrorMessage(part.replace("|", "").trim());
            } else {
              builder.setErrorMessage("No error");
            }
          }
        }

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
            throw new SQLException(e.getMessage());
          } catch (Exception e) {
            System.err.println(
                "Error processing the batch for container: " + id + " at datanode: " + datanodeId);
            e.printStackTrace();
          }
        } else {
          System.err.println("Log line does not have all required fields: " + line);
        }
      }
      if (!batchList.isEmpty()) {
        dbstore.insertContainerDatanodeData(batchList);
        batchList.clear();
      }

    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
