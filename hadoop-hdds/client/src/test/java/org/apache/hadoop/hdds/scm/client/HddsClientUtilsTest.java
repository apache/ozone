/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.scm.client;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ConnectException;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class HddsClientUtilsTest {

  @Test
  void testContainsException() {
    Exception ex1 = new ConnectException();
    Exception ex2 = new IOException(ex1);
    Exception ex3 = new IllegalArgumentException(ex2);

    assertSame(ex1,
        HddsClientUtils.containsException(ex3, ConnectException.class));
    assertSame(ex2,
        HddsClientUtils.containsException(ex3, IOException.class));
    assertSame(ex3,
        HddsClientUtils.containsException(ex3, IllegalArgumentException.class));
    assertNull(
        HddsClientUtils.containsException(ex3, IllegalStateException.class));
  }
}
