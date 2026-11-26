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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Filter output stream that allows assertions on behavior.
 */
public class SpyOutputStream extends FilterOutputStream {

  private final AtomicInteger closed = new AtomicInteger();

  public SpyOutputStream(OutputStream out) {
    super(out);
  }

  @Override
  public void close() throws IOException {
    closed.incrementAndGet();
    super.close();
  }

  public void assertClosedExactlyOnce() {
    assertEquals(1, closed.get());
  }
}
