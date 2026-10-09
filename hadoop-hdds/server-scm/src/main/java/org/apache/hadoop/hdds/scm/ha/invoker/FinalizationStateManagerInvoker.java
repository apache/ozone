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

package org.apache.hadoop.hdds.scm.ha.invoker;

import java.io.IOException;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationStateManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.ratis.protocol.Message;

/** Code generated for {@link FinalizationStateManager}.  Do not modify. */
public class FinalizationStateManagerInvoker extends ScmInvoker<FinalizationStateManager> {
  enum ReplicateMethod implements NameAndParameterTypes {
    finalizeUpgrade(new Class<?>[][] {
        new Class<?>[] {}
    });

    private final Class<?>[][] parameterTypes;

    ReplicateMethod(Class<?>[][] parameterTypes) {
      this.parameterTypes = parameterTypes;
    }

    @Override
    public Class<?>[] getParameterTypes(int numArgs) {
      return parameterTypes[numArgs];
    }
  }

  public FinalizationStateManagerInvoker(FinalizationStateManager impl, SCMRatisServer ratis) {
    super(impl, FinalizationStateManagerInvoker::newProxy, ratis);
  }

  @Override
  public Class<FinalizationStateManager> getApi() {
    return FinalizationStateManager.class;
  }

  static FinalizationStateManager newProxy(ScmInvoker<FinalizationStateManager> invoker) {
    return new FinalizationStateManager() {

      @Override
      public void finalizeUpgrade() throws IOException {
        final Object[] args = {};
        invoker.invokeReplicateDirect(ReplicateMethod.finalizeUpgrade, args);
      }

      @Override
      public void reinitialize(Table arg0) throws IOException {
        invoker.getImpl().reinitialize(arg0);
      }
    };
  }

  @SuppressWarnings("unchecked")
  @Override
  public Message invokeLocal(String methodName, Object[] p) throws Exception {
    switch (methodName) {
    case "finalizeUpgrade":
      getImpl().finalizeUpgrade();
      return Message.EMPTY;

    case "reinitialize":
      final Table arg0 = p.length > 0 ? (Table) p[0] : null;
      getImpl().reinitialize(arg0);
      return Message.EMPTY;

    default:
      throw new IllegalArgumentException("Method not found: " + methodName + " in FinalizationStateManager");
    }
  }
}
