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

package org.apache.hadoop.ozone;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Utility class to check standard output.
 */
public class StandardOutputTestBase {
  private final ByteArrayOutputStream outContent =
      new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent =
      new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  /**
   * Set up fresh output and error streams before test.
   *
   * @throws UnsupportedEncodingException
   */
  @BeforeEach
  public void setUpStreams() throws UnsupportedEncodingException {
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  /**
   * Restore original error and output streams after test.
   */
  @AfterEach
  public void restoreStreams() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  public String getOutContentString()
      throws UnsupportedEncodingException {
    return getOutContentString(DEFAULT_ENCODING);
  }

  public String getErrContentString()
      throws UnsupportedEncodingException {
    return getErrContentString(DEFAULT_ENCODING);
  }

  public String getOutContentString(String encoding)
      throws UnsupportedEncodingException {
    return outContent.toString(encoding);
  }

  public String getErrContentString(String encoding)
      throws UnsupportedEncodingException {
    return errContent.toString(encoding);
  }

  public String getDefaultEncoding() {
    return DEFAULT_ENCODING;
  }

}
