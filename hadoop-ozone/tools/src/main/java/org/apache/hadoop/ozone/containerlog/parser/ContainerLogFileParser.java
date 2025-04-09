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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Container log file Parsing logic.
 */

public class ContainerLogFileParser {

  private  ExecutorService executorService;
  private final Map<String, List<DatanodeContainerInfo>> batchMap = new HashMap<>(500);
  private final Map<String, ReentrantLock> locks = new HashMap<>(500);
  private final ReentrantLock globalLock = new ReentrantLock();
  private static final int MAX_KEYS_IN_MAP = 400;
  private final AtomicBoolean isGlobalLockHeld = new AtomicBoolean(false);
  private final Object globalLockNotifier = new Object();

  private static final String FILENAME_PARTS_REGEX = "-";
  private static final String DATANODE_ID_REGEX = "\\.";
  private static final String LOG_LINE_SPLIT_REGEX = " \\| ";
  private static final String KEY_VALUE_SPLIT_REGEX = "=";

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

        long datanodeId = 0;
        String datanodeIdStr = parts[2];
        if (datanodeIdStr.contains(".log")) {
          datanodeIdStr = datanodeIdStr.split(DATANODE_ID_REGEX)[0];
        }

        try {
          datanodeId = Long.parseLong(datanodeIdStr);

        } catch (NumberFormatException e) {
          System.out.println("Invalid datanode ID in filename: " + fileName);
          continue;
        }

        long finalDatanodeId = datanodeId;

        executorService.submit(() -> {

          String threadName = Thread.currentThread().getName();
          try {
            System.out.println(threadName + " is starting to process file: " + file.toString());
            processFile(file.toString(), dbstore, finalDatanodeId);
          } catch (Exception e) {
            System.out.println("Thread " + threadName + " is stopping to process the file: " + file.toString() +
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


      if (!batchMap.isEmpty()) {
        processAndClearAllBatches(dbstore);
      }

      executorService.shutdown();

    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    } catch (SQLException e) {
      System.out.println("Thread " + Thread.currentThread().getName() +
          " is stopping due to SQLException: " + e.getMessage());
      executorService.shutdown();
      throw e;
    }
  }


  private synchronized void  processAndClearAllBatches(ContainerDatanodeDatabase dbstore) throws SQLException {
    globalLock.lock();
    try {
      isGlobalLockHeld.set(true);
      for (Map.Entry<String, List<DatanodeContainerInfo>> entry : batchMap.entrySet()) {
        String key = entry.getKey();
        List<DatanodeContainerInfo> transitionList = entry.getValue();

        dbstore.insertContainerDatanodeData(key, transitionList);
        transitionList.clear();
      }
      batchMap.clear();
      locks.clear();

    }  finally {
      globalLock.unlock();
      isGlobalLockHeld.set(false);
      synchronized (globalLockNotifier) {
        globalLockNotifier.notifyAll();
      }
    }
  }

  public void processFile(String logFilePath, ContainerDatanodeDatabase dbstore, long datanodeId) throws SQLException {
    try (BufferedReader reader = Files.newBufferedReader(Paths.get(logFilePath), StandardCharsets.UTF_8)) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] parts = line.split(LOG_LINE_SPLIT_REGEX);
        String timestamp = parts[0];
        String logLevel = parts[1];
        String id = null, bcsid = null, state = null, index = null;

        for (String part : parts) {
          part = part.trim();

          if (part.contains(KEY_VALUE_SPLIT_REGEX)) {
            String[] keyValue = part.split(KEY_VALUE_SPLIT_REGEX);
            if (keyValue.length == 2) {
              String key = keyValue[0].trim();
              String value = keyValue[1].trim();

              switch (key) {
              case "ID":
                id = value;
                break;
              case "BCSID":
                bcsid = value;
                break;
              case "State":
                state = value.replace("|", "").trim();
                break;
              case "Index":
                index = value;
                break;
              default:
                System.out.println("Unexpected key: " + key);
                break;
              }
            }
          }

        }

        if (index == null || !index.equals("0")) {
          continue;
        }

        String errorMessage = "No error";
        if (line.contains("ERROR") || line.contains("WARN")) {
          errorMessage = null;
          for (int i = parts.length - 1; i >= 0; i--) {
            String part = parts[i].trim();

            if (part.isEmpty()) {
              continue;
            }

            if (part.startsWith("State=") || part.startsWith("ID=") || part.startsWith("BCSID=") ||
                part.startsWith("Index=")  || part.equals("ERROR") || part.equals("INFO")
                || part.equals("WARN") || part.equals(timestamp)) {
              continue;
            }
            if (part.endsWith("|")) {
              part = part.replace("|", "").trim();
            }
            errorMessage = part;

            break;
          }
        }

        if (id != null && bcsid != null && state != null) {
          try {
            long containerId = Long.parseLong(id);
            long bcsidValue = Long.parseLong(bcsid);

            String key = containerId + "#" + datanodeId;

            try {
              synchronized (globalLockNotifier) {
                while (isGlobalLockHeld.get()) {
                  try {
                    globalLockNotifier.wait();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                }

                if (!isGlobalLockHeld.get()) {
                  ReentrantLock lock = locks.computeIfAbsent(key, k -> new ReentrantLock());
                  lock.lock();

                  try {
                    List<DatanodeContainerInfo> currentBatch = batchMap.computeIfAbsent(key, k -> new ArrayList<>());

                    currentBatch.add(new DatanodeContainerInfo(timestamp, state, bcsidValue, errorMessage,
                        logLevel, Integer.parseInt(index)));

                    if (batchMap.size() >= MAX_KEYS_IN_MAP) {
                      processAndClearAllBatches(dbstore);
                    }
                  } finally {
                    lock.unlock();
                  }
                }
              }
            } catch (SQLException e) {
              throw new SQLException(e.getMessage());
            } catch (Exception e) {
              System.out.println("Error processing the batch for key: " + key);
              e.printStackTrace();
            }
          } catch (NumberFormatException e) {
            System.out.println("Error parsing ID or BCSID as Long: " + line);
          }
        } else {
          System.out.println("Log line does not have all required fields: " + line);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
