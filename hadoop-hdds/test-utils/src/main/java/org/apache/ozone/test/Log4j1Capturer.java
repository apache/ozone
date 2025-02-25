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

package org.apache.ozone.test;

import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;

/**
 * Capture Log4j1 logs.
 */
class Log4j1Capturer extends GenericTestUtils.LogCapturer {
  private final Appender appender;
  private final Logger logger;

  private static Layout getDefaultLayout() {
    Logger rootLogger = Logger.getRootLogger();

    Appender defaultAppender = rootLogger.getAppender("stdout");
    if (defaultAppender == null) {
      defaultAppender = rootLogger.getAppender("console");
    }

    return defaultAppender == null
        ? new PatternLayout()
        : defaultAppender.getLayout();
  }

  Log4j1Capturer(Logger logger) {
    this(logger, getDefaultLayout());
  }

  Log4j1Capturer(Logger logger, Layout layout) {
    this.logger = logger;
    this.appender = new WriterAppender(layout, writer());
    logger.addAppender(appender);
  }

  @Override
  public void stopCapturing() {
    logger.removeAppender(appender);
  }
}
