/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.ha;

import org.junit.Assert;
import org.junit.Test;

public class TestSequenceIDGenerator {
  @Test
  public void testSequenceIDGen() throws Exception {
    SequenceIDGenerator idGenerator = new SequenceIDGenerator(0);
    Assert.assertEquals(0L, idGenerator.nextID());
    Assert.assertEquals(1L, idGenerator.nextID());
    Assert.assertEquals(2L, idGenerator.nextID());

    idGenerator = new SequenceIDGenerator(1);
    Assert.assertEquals(17179869184L, idGenerator.nextID());
    Assert.assertEquals(17179869185L, idGenerator.nextID());
    Assert.assertEquals(17179869186L, idGenerator.nextID());

    idGenerator = new SequenceIDGenerator(100);
    Assert.assertEquals(1717986918400L, idGenerator.nextID());
    Assert.assertEquals(1717986918401L, idGenerator.nextID());
    Assert.assertEquals(1717986918402L, idGenerator.nextID());
  }
}
