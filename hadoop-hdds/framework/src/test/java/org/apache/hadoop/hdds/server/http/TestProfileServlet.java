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

package org.apache.hadoop.hdds.server.http;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.hdds.server.http.ProfileServlet.Event;
import org.apache.hadoop.hdds.server.http.ProfileServlet.Output;
import org.junit.jupiter.api.Test;

/**
 * Test prometheus Sink.
 */
public class TestProfileServlet {

  @Test
  public void testNameValidation() {
    ProfileServlet.validateFileName(
        ProfileServlet.generateFileName(1, Output.FLAMEGRAPH, Event.ALLOC));
    ProfileServlet.validateFileName(
        ProfileServlet.generateFileName(1, Output.SVG, Event.ALLOC));
    ProfileServlet.validateFileName(
        ProfileServlet.generateFileName(23, Output.COLLAPSED,
            Event.L1_DCACHE_LOAD_MISSES));
  }

  @Test
  public void testNameValidationWithNewLine() {
    assertThrows(IllegalArgumentException.class,
        () -> ProfileServlet.validateFileName("test\n" +
            ProfileServlet.generateFileName(1, Output.FLAMEGRAPH,
                Event.ALLOC)));
    assertThrows(IllegalArgumentException.class,
        () -> ProfileServlet.validateFileName("test\n" +
            ProfileServlet.generateFileName(1, Output.SVG, Event.ALLOC)));
  }

  @Test
  public void testNameValidationWithSlash() {
    assertThrows(IllegalArgumentException.class,
        () -> ProfileServlet.validateFileName("../" +
            ProfileServlet.generateFileName(1, Output.FLAMEGRAPH,
                Event.ALLOC)));
    assertThrows(IllegalArgumentException.class,
        () -> ProfileServlet.validateFileName("../" +
            ProfileServlet.generateFileName(1, Output.SVG, Event.ALLOC)));
  }

}
