/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.utils;

import java.io.IOException;

import org.apache.log4j.AsyncAppender;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.spi.LoggingEvent;

/**
 * The AsyncRollingFileAppender shall take the required parameters for supplying
 * RollingFileAppender to AsyncAppender.
 */
public class AsyncRollingFileAppender extends AsyncAppender {

  private String maxFileSize = String.valueOf(10 * 1024 * 1024);

  private int maxBackupIndex = 1;

  private String fileName = null;

  private String conversionPattern = null;

  private boolean blocking = true;

  private int bufferSize = DEFAULT_BUFFER_SIZE;

  private RollingFileAppender rollingFileAppender = null;

  private volatile boolean isRollingFileAppenderAssigned = false;

  @Override
  public void append(LoggingEvent event) {
    if (rollingFileAppender == null) {
      createRollingFileAppender();
    }
    super.append(event);
  }

  private synchronized void createRollingFileAppender() {
    if (!isRollingFileAppenderAssigned) {
      PatternLayout patternLayout;
      if (conversionPattern != null) {
        patternLayout = new PatternLayout(conversionPattern);
      } else {
        patternLayout = new PatternLayout();
      }
      try {
        rollingFileAppender =
            new RollingFileAppender(patternLayout, fileName, true);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      rollingFileAppender.setMaxBackupIndex(maxBackupIndex);
      rollingFileAppender.setMaxFileSize(maxFileSize);
      this.addAppender(rollingFileAppender);
      isRollingFileAppenderAssigned = true;
      super.setBlocking(blocking);
      super.setBufferSize(bufferSize);
    }
  }

  public synchronized String getMaxFileSize() {
    return maxFileSize;
  }

  public synchronized void setMaxFileSize(String maxFileSize) {
    this.maxFileSize = maxFileSize;
  }

  public synchronized int getMaxBackupIndex() {
    return maxBackupIndex;
  }

  public synchronized void setMaxBackupIndex(int maxBackupIndex) {
    this.maxBackupIndex = maxBackupIndex;
  }

  public synchronized String getFileName() {
    return fileName;
  }

  public synchronized void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public synchronized String getConversionPattern() {
    return conversionPattern;
  }

  public synchronized void setConversionPattern(String conversionPattern) {
    this.conversionPattern = conversionPattern;
  }

  public boolean isBlocking() {
    return blocking;
  }

  public synchronized void setBlocking(boolean blocking) {
    this.blocking = blocking;
  }

  public synchronized int getBufferSize() {
    return bufferSize;
  }

  public synchronized void setBufferSize(int bufferSize) {
    this.bufferSize = bufferSize;
  }
}
