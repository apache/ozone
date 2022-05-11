/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.freon;

import com.codahale.metrics.Timer;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;

/**
 * Abstract class for OmBucketReadWriteFileOps/KeyOps Freon class
 * implementations.
 */
public abstract class AbstractOmBucketReadWriteOps extends BaseFreonGenerator
    implements Callable<Void> {

  protected static final Logger LOG =
      LoggerFactory.getLogger(AbstractOmBucketReadWriteOps.class);

  protected abstract String createPath(String path) throws IOException;

  protected int readOperations(int readThreadCount, int numOfReadOperations,
                               int keyCountForRead, int length)
      throws Exception {

    // Create keyCountForRead/fileCountForRead (defaultValue = 1000) keys/files
    // under rootPath/readPath
    String readPath = createPath("readPath");
    create(readPath, keyCountForRead, length);

    // Start readThreadCount (defaultValue = 90) concurrent read threads
    // performing numOfReadOperations (defaultValue = 50) iterations
    // of read operations (bucket.listKeys(readPath) or
    // fileSystem.listStatus(rootPath/readPath))
    ExecutorService readService = Executors.newFixedThreadPool(readThreadCount);
    CompletionService<Integer> readExecutorCompletionService =
        new ExecutorCompletionService<>(readService);
    List<Future<Integer>> readFutures = new ArrayList<>();
    for (int i = 0; i < readThreadCount; i++) {
      readFutures.add(readExecutorCompletionService.submit(() -> {
        int readCount = 0;
        try {
          for (int j = 0; j < numOfReadOperations; j++) {
            readCount = getReadCount(readCount, "readPath");
          }
        } catch (IOException e) {
          LOG.warn("Exception while listing keys/files ", e);
        }
        return readCount;
      }));
    }

    int readResult = 0;
    for (int i = 0; i < readFutures.size(); i++) {
      try {
        readResult += readExecutorCompletionService.take().get();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
    }
    readService.shutdown();

    return readResult;
  }

  protected abstract int getReadCount(int readCount, String readPath)
      throws IOException;

  protected int writeOperations(int writeThreadCount, int numOfWriteOperations,
                                int keyCountForWrite, int length)
      throws Exception {

    // Start writeThreadCount (defaultValue = 10) concurrent write threads
    // performing numOfWriteOperations (defaultValue = 10) iterations
    // of write operations (createKeys(writePath) or
    // createFiles(rootPath/writePath))
    String writePath = createPath("writePath");

    ExecutorService writeService =
        Executors.newFixedThreadPool(writeThreadCount);
    CompletionService<Integer> writeExecutorCompletionService =
        new ExecutorCompletionService<>(writeService);
    List<Future<Integer>> writeFutures = new ArrayList<>();
    for (int i = 0; i < writeThreadCount; i++) {
      writeFutures.add(writeExecutorCompletionService.submit(() -> {
        int writeCount = 0;
        try {
          for (int j = 0; j < numOfWriteOperations; j++) {
            create(writePath, keyCountForWrite, length);
            writeCount++;
          }
        } catch (IOException e) {
          LOG.warn("Exception while creating keys/files ", e);
        }
        return writeCount;
      }));
    }

    int writeResult = 0;
    for (int i = 0; i < writeFutures.size(); i++) {
      try {
        writeResult += writeExecutorCompletionService.take().get();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
    }
    writeService.shutdown();

    return writeResult;
  }

  protected abstract Timer getTimer();

  protected abstract ContentGenerator getContentGenerator();

  protected void create(String path, int keyCount, int length)
      throws Exception {
    for (int i = 0; i < keyCount; i++) {
      String keyName = path.concat(OzoneConsts.OM_KEY_PREFIX)
          .concat(RandomStringUtils.randomAlphanumeric(length));
      if (LOG.isDebugEnabled()) {
        LOG.debug("KeyName : {}", keyName);
      }
      getTimer().time(() -> {
        try (OutputStream stream = create(keyName)) {
          getContentGenerator().write(stream);
          stream.flush();
        }
        return null;
      });
    }
  }

  protected abstract OutputStream create(String pathName) throws IOException;
}
