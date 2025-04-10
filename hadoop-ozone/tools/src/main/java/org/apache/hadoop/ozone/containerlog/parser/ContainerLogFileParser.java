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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Container log file Parsing logic.
 */

public class ContainerLogFileParser {

  private ExecutorService executorService;
  private final List<DatanodeContainerInfo> batchList = new ArrayList<>(500);
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

      if (!batchList.isEmpty()) {
        processAndClearAllBatches(dbstore);
      }

      executorService.shutdown();

    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    } catch (NumberFormatException e) {
      System.err.println("Invalid datanode ID");
    } catch (SQLException e) {
      System.err.println("Thread " + Thread.currentThread().getName() +
          " is stopping due to SQLException: " + e.getMessage());
      executorService.shutdown();
      throw e;
    }
  }

  private synchronized void processAndClearAllBatches(ContainerDatanodeDatabase dbstore) throws SQLException {
    List<DatanodeContainerInfo> localBatchList = null;

    globalLock.lock();
    try {
      isGlobalLockHeld.set(true);

      localBatchList = new ArrayList<>(batchList);
      batchList.clear();

    } finally {
      globalLock.unlock();
      isGlobalLockHeld.set(false);
      synchronized (globalLockNotifier) {
        globalLockNotifier.notifyAll();
      }
    }

    if (localBatchList != null && !localBatchList.isEmpty()) {
      dbstore.insertContainerDatanodeData(localBatchList);
      localBatchList.clear();
    }
  }


  private void processFile(String logFilePath, ContainerDatanodeDatabase dbstore, long datanodeId) throws SQLException {
    try (BufferedReader reader = Files.newBufferedReader(Paths.get(logFilePath), StandardCharsets.UTF_8)) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] parts = line.split(LOG_LINE_SPLIT_REGEX);
        String timestamp = parts[0].trim();
        String logLevel = parts[1].trim();
        String id = null, bcsid = null, state = null, index = null;
        String errorMessage = "No error";

        for (int i = 2; i < parts.length; i++) {
          String part = parts[i].trim();

          if (part.contains(KEY_VALUE_SPLIT_REGEX)) {
            String[] keyValue = part.split(KEY_VALUE_SPLIT_REGEX, 2);
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
                  break;
              }
            }
          } else {
            if (!part.isEmpty()) {
              errorMessage = part.replace("|", "").trim();
            }
          }
        }

          if (index == null || !index.equals("0")) {
            continue; //Currently only ratis replicated containers are considered.
          }

          if (id != null && bcsid != null && state != null) {
            try {
              long containerId = Long.parseLong(id);
              long bcsidValue = Long.parseLong(bcsid);

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

                    batchList.add(new DatanodeContainerInfo(containerId, datanodeId, timestamp, state, bcsidValue, errorMessage, logLevel, Integer.parseInt(index)));

                    if (batchList.size() >= MAX_KEYS_IN_MAP) {
                      processAndClearAllBatches(dbstore);
                    }
                  }
                }
              } catch (SQLException e) {
                throw new SQLException(e.getMessage());
              } catch (Exception e) {
                System.err.println("Error processing the batch for container: " + containerId + " at datanode: " + datanodeId);
                e.printStackTrace();
              }
            } catch (NumberFormatException e) {
              System.err.println("Error parsing ID or BCSID as Long: " + line);
            }
          } else {
            System.err.println("Log line does not have all required fields: " + line);
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
    }
  }
  }
