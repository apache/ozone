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
import org.apache.hadoop.hdds.scm.ha.SCMRatisResponse;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationCheckpoint;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationStateManager;
import org.apache.hadoop.hdds.scm.server.upgrade.SCMUpgradeFinalizationContext;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.ratis.protocol.Message;

/** Code generated for {@link FinalizationStateManager}.  Do not modify. */
public class FinalizationStateManagerInvoker extends ScmInvoker<FinalizationStateManager> {
  enum ReplicateMethod implements NameAndParameterTypes {
    addFinalizingMark(new Class<?>[][] {
        new Class<?>[] {}
    }),
    finalizeLayoutFeature(new Class<?>[][] {
        null,
        new Class<?>[] {Integer.class}
    }),
    removeFinalizingMark(new Class<?>[][] {
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
      public void addFinalizingMark() throws IOException {
        final Object[] args = {};
        invoker.invokeReplicateDirect(ReplicateMethod.addFinalizingMark, args);
      }

      @Override
      public boolean crossedCheckpoint(FinalizationCheckpoint arg0) {
        return invoker.getImpl().crossedCheckpoint(arg0);
      }

      @Override
      public void finalizeLayoutFeature(Integer arg0) throws IOException {
        final Object[] args = {arg0};
        invoker.invokeReplicateDirect(ReplicateMethod.finalizeLayoutFeature, args);
      }

      @Override
      public FinalizationCheckpoint getFinalizationCheckpoint() {
        return invoker.getImpl().getFinalizationCheckpoint();
      }

      @Override
      public void reinitialize(Table arg0) throws IOException {
        invoker.getImpl().reinitialize(arg0);
      }

      @Override
      public void removeFinalizingMark() throws IOException {
        final Object[] args = {};
        invoker.invokeReplicateDirect(ReplicateMethod.removeFinalizingMark, args);
      }

      @Override
      public void setUpgradeContext(SCMUpgradeFinalizationContext arg0) {
        invoker.getImpl().setUpgradeContext(arg0);
      }
    };
  }

  @SuppressWarnings("unchecked")
  @Override
  public Message invokeLocal(String methodName, Object[] p) throws Exception {
    final Class<?> returnType;
    final Object returnValue;
    switch (methodName) {
    case "addFinalizingMark":
      getImpl().addFinalizingMark();
      return Message.EMPTY;

    case "crossedCheckpoint":
      final FinalizationCheckpoint arg0 = p.length > 0 ? (FinalizationCheckpoint) p[0] : null;
      returnType = boolean.class;
      returnValue = getImpl().crossedCheckpoint(arg0);
      break;

    case "finalizeLayoutFeature":
      final Integer arg1 = p.length > 0 ? (Integer) p[0] : null;
      getImpl().finalizeLayoutFeature(arg1);
      return Message.EMPTY;

    case "getFinalizationCheckpoint":
      returnType = FinalizationCheckpoint.class;
      returnValue = getImpl().getFinalizationCheckpoint();
      break;

    case "reinitialize":
      final Table arg2 = p.length > 0 ? (Table) p[0] : null;
      getImpl().reinitialize(arg2);
      return Message.EMPTY;

    case "removeFinalizingMark":
      getImpl().removeFinalizingMark();
      return Message.EMPTY;

    case "setUpgradeContext":
      final SCMUpgradeFinalizationContext arg3 = p.length > 0 ? (SCMUpgradeFinalizationContext) p[0] : null;
      getImpl().setUpgradeContext(arg3);
      return Message.EMPTY;

    default:
      throw new IllegalArgumentException("Method not found: " + methodName + " in FinalizationStateManager");
    }

    return SCMRatisResponse.encode(returnValue, returnType);
  }
}
