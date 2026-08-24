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
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerStateManager;
import org.apache.hadoop.hdds.scm.ha.SCMRatisResponse;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.ratis.protocol.Message;

/** Code generated for {@link ContainerStateManager}.  Do not modify. */
public class ContainerStateManagerInvoker extends ScmInvoker<ContainerStateManager> {
  enum ReplicateMethod implements NameAndParameterTypes {
    addContainer(new Class<?>[][] {
        null,
        new Class<?>[] {ContainerInfoProto.class}
    }),
    removeContainer(new Class<?>[][] {
        null,
        new Class<?>[] {HddsProtos.ContainerID.class}
    }),
    transitionDeletingOrDeletedToTargetState(new Class<?>[][] {
        null,
        null,
        new Class<?>[] {HddsProtos.ContainerID.class, LifeCycleState.class}
    }),
    updateContainerInfo(new Class<?>[][] {
        null,
        new Class<?>[] {ContainerInfoProto.class}
    }),
    updateContainerStateWithSequenceId(new Class<?>[][] {
        null,
        null,
        null,
        new Class<?>[] {HddsProtos.ContainerID.class, LifeCycleEvent.class, Long.class}
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

  public ContainerStateManagerInvoker(ContainerStateManager impl, SCMRatisServer ratis) {
    super(impl, ContainerStateManagerInvoker::newProxy, ratis);
  }

  @Override
  public Class<ContainerStateManager> getApi() {
    return ContainerStateManager.class;
  }

  static ContainerStateManager newProxy(ScmInvoker<ContainerStateManager> invoker) {
    return new ContainerStateManager() {

      @Override
      public void addContainer(ContainerInfoProto arg0) throws IOException {
        final Object[] args = {arg0};
        invoker.invokeReplicateDirect(ReplicateMethod.addContainer, args);
      }

      @Override
      public boolean contains(ContainerID arg0) {
        return invoker.getImpl().contains(arg0);
      }

      @Override
      public ContainerInfo getContainer(ContainerID arg0) {
        return invoker.getImpl().getContainer(arg0);
      }

      @Override
      public int getContainerCount(LifeCycleState arg0) {
        return invoker.getImpl().getContainerCount(arg0);
      }

      @Override
      public List<ContainerID> getContainerIDs(LifeCycleState arg0, ContainerID arg1, int arg2) {
        return invoker.getImpl().getContainerIDs(arg0, arg1, arg2);
      }

      @Override
      public List<ContainerInfo> getContainerInfos(ReplicationType arg0) {
        return invoker.getImpl().getContainerInfos(arg0);
      }

      @Override
      public List<ContainerInfo> getContainerInfos(LifeCycleState arg0) {
        return invoker.getImpl().getContainerInfos(arg0);
      }

      @Override
      public List<ContainerInfo> getContainerInfos(ContainerID arg0, int arg1) {
        return invoker.getImpl().getContainerInfos(arg0, arg1);
      }

      @Override
      public List<ContainerInfo> getContainerInfos(LifeCycleState arg0, ContainerID arg1, int arg2) {
        return invoker.getImpl().getContainerInfos(arg0, arg1, arg2);
      }

      @Override
      public Set<ContainerReplica> getContainerReplicas(ContainerID arg0) {
        return invoker.getImpl().getContainerReplicas(arg0);
      }

      @Override
      public ContainerInfo getMatchingContainer(long arg0, String arg1, PipelineID arg2, NavigableSet arg3) {
        return invoker.getImpl().getMatchingContainer(arg0, arg1, arg2, arg3);
      }

      @Override
      public void reinitialize(Table arg0) throws IOException {
        invoker.getImpl().reinitialize(arg0);
      }

      @Override
      public void removeContainer(HddsProtos.ContainerID arg0) throws IOException {
        final Object[] args = {arg0};
        invoker.invokeReplicateDirect(ReplicateMethod.removeContainer, args);
      }

      @Override
      public void removeContainerReplica(ContainerReplica arg0) {
        invoker.getImpl().removeContainerReplica(arg0);
      }

      @Override
      public void transitionDeletingOrDeletedToTargetState(HddsProtos.ContainerID arg0, LifeCycleState arg1) throws
          IOException {
        final Object[] args = {arg0, arg1};
        invoker.invokeReplicateDirect(ReplicateMethod.transitionDeletingOrDeletedToTargetState, args);
      }

      @Override
      public void updateContainerInfo(ContainerInfoProto arg0) throws IOException {
        final Object[] args = {arg0};
        invoker.invokeReplicateDirect(ReplicateMethod.updateContainerInfo, args);
      }

      @Override
      public void updateContainerReplica(ContainerReplica arg0) {
        invoker.getImpl().updateContainerReplica(arg0);
      }

      @Override
      public void updateContainerStateWithSequenceId(HddsProtos.ContainerID arg0, LifeCycleEvent arg1, Long arg2) throws
          IOException, InvalidStateTransitionException {
        final Object[] args = {arg0, arg1, arg2};
        invoker.invokeReplicateDirect(ReplicateMethod.updateContainerStateWithSequenceId, args);
      }

      @Override
      public void updateDeleteTransactionId(Map arg0) throws IOException {
        invoker.getImpl().updateDeleteTransactionId(arg0);
      }
    };
  }

  @SuppressWarnings("unchecked")
  @Override
  public Message invokeLocal(String methodName, Object[] p) throws Exception {
    final Class<?> returnType;
    final Object returnValue;
    switch (methodName) {
    case "addContainer":
      final ContainerInfoProto arg0 = p.length > 0 ? (ContainerInfoProto) p[0] : null;
      getImpl().addContainer(arg0);
      return Message.EMPTY;

    case "contains":
      final ContainerID arg1 = p.length > 0 ? (ContainerID) p[0] : null;
      returnType = boolean.class;
      returnValue = getImpl().contains(arg1);
      break;

    case "getContainer":
      final ContainerID arg2 = p.length > 0 ? (ContainerID) p[0] : null;
      returnType = ContainerInfo.class;
      returnValue = getImpl().getContainer(arg2);
      break;

    case "getContainerCount":
      final LifeCycleState arg3 = p.length > 0 ? (LifeCycleState) p[0] : null;
      returnType = int.class;
      returnValue = getImpl().getContainerCount(arg3);
      break;

    case "getContainerIDs":
      final LifeCycleState arg4 = p.length > 0 ? (LifeCycleState) p[0] : null;
      final ContainerID arg5 = p.length > 1 ? (ContainerID) p[1] : null;
      final int arg6 = p.length > 2 ? (int) p[2] : 0;
      returnType = List.class;
      returnValue = getImpl().getContainerIDs(arg4, arg5, arg6);
      break;

    case "getContainerInfos":
      if (p.length == 1 && (p[0] == null || ReplicationType.class.isInstance(p[0]))) {
        final ReplicationType arg7 = (ReplicationType) p[0];
        returnType = List.class;
        returnValue = getImpl().getContainerInfos(arg7);
        break;
      }
      if (p.length == 1 && (p[0] == null || LifeCycleState.class.isInstance(p[0]))) {
        final LifeCycleState arg8 = (LifeCycleState) p[0];
        returnType = List.class;
        returnValue = getImpl().getContainerInfos(arg8);
        break;
      }
      if (p.length == 2 && (p[0] == null || ContainerID.class.isInstance(p[0])) && p[1] instanceof Integer) {
        final ContainerID arg9 = (ContainerID) p[0];
        final int arg10 = (int) p[1];
        returnType = List.class;
        returnValue = getImpl().getContainerInfos(arg9, arg10);
        break;
      }
      if (p.length == 3 && (p[0] == null || LifeCycleState.class.isInstance(p[0])) && (p[1] == null ||
          ContainerID.class.isInstance(p[1])) && p[2] instanceof Integer) {
        final LifeCycleState arg11 = (LifeCycleState) p[0];
        final ContainerID arg12 = (ContainerID) p[1];
        final int arg13 = (int) p[2];
        returnType = List.class;
        returnValue = getImpl().getContainerInfos(arg11, arg12, arg13);
        break;
      }
      throw new IllegalArgumentException("Method not found: " + methodName + " in ContainerStateManager");

    case "getContainerReplicas":
      final ContainerID arg14 = p.length > 0 ? (ContainerID) p[0] : null;
      returnType = Set.class;
      returnValue = getImpl().getContainerReplicas(arg14);
      break;

    case "getMatchingContainer":
      final long arg15 = p.length > 0 ? (long) p[0] : 0L;
      final String arg16 = p.length > 1 ? (String) p[1] : null;
      final PipelineID arg17 = p.length > 2 ? (PipelineID) p[2] : null;
      final NavigableSet arg18 = p.length > 3 ? (NavigableSet) p[3] : null;
      returnType = ContainerInfo.class;
      returnValue = getImpl().getMatchingContainer(arg15, arg16, arg17, arg18);
      break;

    case "reinitialize":
      final Table arg19 = p.length > 0 ? (Table) p[0] : null;
      getImpl().reinitialize(arg19);
      return Message.EMPTY;

    case "removeContainer":
      final HddsProtos.ContainerID arg20 = p.length > 0 ? (HddsProtos.ContainerID) p[0] : null;
      getImpl().removeContainer(arg20);
      return Message.EMPTY;

    case "removeContainerReplica":
      final ContainerReplica arg21 = p.length > 0 ? (ContainerReplica) p[0] : null;
      getImpl().removeContainerReplica(arg21);
      return Message.EMPTY;

    case "transitionDeletingOrDeletedToTargetState":
      final HddsProtos.ContainerID arg22 = p.length > 0 ? (HddsProtos.ContainerID) p[0] : null;
      final LifeCycleState arg23 = p.length > 1 ? (LifeCycleState) p[1] : null;
      getImpl().transitionDeletingOrDeletedToTargetState(arg22, arg23);
      return Message.EMPTY;

    case "updateContainerInfo":
      final ContainerInfoProto arg24 = p.length > 0 ? (ContainerInfoProto) p[0] : null;
      getImpl().updateContainerInfo(arg24);
      return Message.EMPTY;

    case "updateContainerReplica":
      final ContainerReplica arg25 = p.length > 0 ? (ContainerReplica) p[0] : null;
      getImpl().updateContainerReplica(arg25);
      return Message.EMPTY;

    case "updateContainerStateWithSequenceId":
      final HddsProtos.ContainerID arg26 = p.length > 0 ? (HddsProtos.ContainerID) p[0] : null;
      final LifeCycleEvent arg27 = p.length > 1 ? (LifeCycleEvent) p[1] : null;
      final Long arg28 = p.length > 2 ? (Long) p[2] : null;
      getImpl().updateContainerStateWithSequenceId(arg26, arg27, arg28);
      return Message.EMPTY;

    case "updateDeleteTransactionId":
      final Map arg29 = p.length > 0 ? (Map) p[0] : null;
      getImpl().updateDeleteTransactionId(arg29);
      return Message.EMPTY;

    default:
      throw new IllegalArgumentException("Method not found: " + methodName + " in ContainerStateManager");
    }

    return SCMRatisResponse.encode(returnValue, returnType);
  }
}
