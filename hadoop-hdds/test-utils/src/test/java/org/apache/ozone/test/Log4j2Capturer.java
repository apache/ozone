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

import java.io.Writer;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;

/**
 * Capture Log4j2 logs.
 */
final class Log4j2Capturer extends GenericTestUtils.LogCapturer {
  private static final String APPENDER_NAME = "capture";

  private static final Log4j2Capturer INSTANCE = new Log4j2Capturer();

  public static Log4j2Capturer getInstance() {
    return INSTANCE;
  }

  private Log4j2Capturer() {
    addAppender(writer());
  }

  @Override
  public void stopCapturing() {
    final LoggerContext context = LoggerContext.getContext(false);
    final Configuration config = context.getConfiguration();
    removeAppender(config);
  }

  private void addAppender(final Writer writer) {
    final LoggerContext context = LoggerContext.getContext(false);
    final Configuration config = context.getConfiguration();
    final PatternLayout layout = PatternLayout.createDefaultLayout(config);
    final Appender appender = WriterAppender.createAppender(layout, null,
        writer, APPENDER_NAME, false, true);
    appender.start();
    config.addAppender(appender);
    addAppender(appender, config);
  }

  private void addAppender(Appender appender, Configuration config) {
    for (final LoggerConfig loggerConfig : config.getLoggers().values()) {
      loggerConfig.addAppender(appender, null, null);
    }
    config.getRootLogger().addAppender(appender, null, null);
  }

  private void removeAppender(final Configuration config) {
    for (final LoggerConfig loggerConfig : config.getLoggers().values()) {
      loggerConfig.removeAppender(APPENDER_NAME);
    }
    config.getRootLogger().removeAppender(APPENDER_NAME);
  }
}
