/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.ha;

import java.io.IOException;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests on {@link org.apache.hadoop.hdds.scm.metadata.Replicate}.
 */
public class TestReplicationAnnotation {
  private SCMHAInvocationHandler scmhaInvocationHandler;

  @Before
  public void setup() {
    scmhaInvocationHandler = new SCMHAInvocationHandler(
        RequestType.CONTAINER,
        null,
        new MockRatisServer());
  }

  @Test(expected = IOException.class)
  public void testReplicateAnnotationBasic() throws Throwable {
    // test whether replicatedOperation will hit the Ratis based replication
    // code path in SCMHAInvocationHandler. Expect to see a IOException cause
    // no handler is added yet.
    scmhaInvocationHandler.invoke(new Object(),
        this.getClass().getMethod("replicatedOperation"), new Object[0]);
  }
}
