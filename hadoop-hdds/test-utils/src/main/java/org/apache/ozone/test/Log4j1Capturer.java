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
package org.apache.ozone.test;

/**
 * Capture Log4j1 logs.
 */
class Log4j1Capturer extends GenericTestUtils.LogCapturer {
  private final org.apache.log4j.Appender appender;
  private final org.apache.log4j.Logger logger;

  private static org.apache.log4j.Layout getDefaultLayout() {
    org.apache.log4j.Logger rootLogger =
        org.apache.log4j.Logger.getRootLogger();

    org.apache.log4j.Appender defaultAppender =
        rootLogger.getAppender("stdout");
    if (defaultAppender == null) {
      defaultAppender = rootLogger.getAppender("console");
    }

    return (defaultAppender == null)
        ? new org.apache.log4j.PatternLayout()
        : defaultAppender.getLayout();
  }

  Log4j1Capturer(org.apache.log4j.Logger logger) {
    this(logger, getDefaultLayout());
  }

  Log4j1Capturer(org.apache.log4j.Logger logger,
      org.apache.log4j.Layout layout) {
    this.logger = logger;
    this.appender = new org.apache.log4j.WriterAppender(layout, writer());
    logger.addAppender(appender);
  }

  public void stopCapturing() {
    logger.removeAppender(appender);
  }
}
