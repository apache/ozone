/**
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
package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.hdds.scm.FetchMetrics;
import org.junit.jupiter.api.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;

class TestFetchMetrics {
  private static FetchMetrics fetchMetrics = new FetchMetrics();

  @Test
  public void testFetchAll() {
    String result = fetchMetrics.getMetrics(null);
    Pattern p = Pattern.compile("beans", Pattern.MULTILINE);
    Matcher m = p.matcher(result);
    assertTrue(m.find());
  }

  @Test
  public void testFetchFiltered() {
    String result = fetchMetrics.getMetrics("Hadoop:service=StorageContainerManager,name=NodeDecommissionMetrics");
    Pattern p = Pattern.compile("beans", Pattern.MULTILINE);
    Matcher m = p.matcher(result);
    assertTrue(m.find());
  }
}
