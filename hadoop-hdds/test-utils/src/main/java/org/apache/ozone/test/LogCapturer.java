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

import java.io.StringWriter;

/**
 * Class to capture logs for doing assertions.
 */
public abstract class LogCapturer {
  private final StringWriter sw = new StringWriter();

  public static LogCapturer captureLogs(org.apache.log4j.Logger logger) {
    return new Log4j1Capturer(logger);
  }

  public static LogCapturer captureLogs(org.slf4j.Logger logger) {
    return new Log4j1Capturer(GenericTestUtils.toLog4j(logger));
  }

  // TODO: let Log4j2Capturer capture only specific logger's logs
  public static LogCapturer log4j2(String ignoredLoggerName) {
    return Log4j2Capturer.getInstance();
  }

  public abstract void stopCapturing();

  protected StringWriter writer() {
    return sw;
  }

  public String getOutput() {
    return writer().toString();
  }

  public void clearOutput() {
    writer().getBuffer().setLength(0);
  }
}
