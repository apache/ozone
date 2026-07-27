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
import java.util.Collection;
import java.util.List;
import java.util.NavigableSet;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StorageTier;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.ha.SCMRatisResponse;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.pipeline.DuplicatedPipelineIdException;
import org.apache.hadoop.hdds.scm.pipeline.InvalidPipelineStateException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineStateManager;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.ratis.protocol.Message;

/** Code generated for {@link PipelineStateManager}.  Do not modify. */
public class PipelineStateManagerInvoker extends ScmInvoker<PipelineStateManager> {
  enum ReplicateMethod implements NameAndParameterTypes {
    addPipeline(new Class<?>[][] {
        null,
        new Class<?>[] {HddsProtos.Pipeline.class}
    }),
    removePipeline(new Class<?>[][] {
        null,
        new Class<?>[] {HddsProtos.PipelineID.class}
    }),
    updatePipelineState(new Class<?>[][] {
        null,
        null,
        new Class<?>[] {HddsProtos.PipelineID.class, HddsProtos.PipelineState.class}
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

  public PipelineStateManagerInvoker(PipelineStateManager impl, SCMRatisServer ratis) {
    super(impl, PipelineStateManagerInvoker::newProxy, ratis);
  }

  @Override
  public Class<PipelineStateManager> getApi() {
    return PipelineStateManager.class;
  }

  static PipelineStateManager newProxy(ScmInvoker<PipelineStateManager> invoker) {
    return new PipelineStateManager() {

      @Override
      public void addContainerToPipeline(PipelineID arg0, ContainerID arg1) throws PipelineNotFoundException,
          InvalidPipelineStateException {
        invoker.getImpl().addContainerToPipeline(arg0, arg1);
      }

      @Override
      public void addContainerToPipelineForce(PipelineID arg0, ContainerID arg1) throws PipelineNotFoundException {
        invoker.getImpl().addContainerToPipelineForce(arg0, arg1);
      }

      @Override
      public void addPipeline(HddsProtos.Pipeline arg0) throws IOException {
        final Object[] args = {arg0};
        invoker.invokeReplicateDirect(ReplicateMethod.addPipeline, args);
      }

      @Override
      public void close() {
        invoker.getImpl().close();
      }

      @Override
      public NavigableSet<ContainerID> getContainers(PipelineID arg0) throws PipelineNotFoundException {
        return invoker.getImpl().getContainers(arg0);
      }

      @Override
      public int getNumberOfContainers(PipelineID arg0) throws PipelineNotFoundException {
        return invoker.getImpl().getNumberOfContainers(arg0);
      }

      @Override
      public Pipeline getPipeline(PipelineID arg0) throws PipelineNotFoundException {
        return invoker.getImpl().getPipeline(arg0);
      }

      @Override
      public int getPipelineCount(ReplicationConfig arg0, Pipeline.PipelineState arg1) {
        return invoker.getImpl().getPipelineCount(arg0, arg1);
      }

      @Override
      public List<Pipeline> getPipelines() {
        return invoker.getImpl().getPipelines();
      }

      @Override
      public List<Pipeline> getPipelines(ReplicationConfig arg0) {
        return invoker.getImpl().getPipelines(arg0);
      }

      @Override
      public List<Pipeline> getPipelines(ReplicationConfig arg0, Pipeline.PipelineState arg1) {
        return invoker.getImpl().getPipelines(arg0, arg1);
      }

      @Override
      public List<Pipeline> getPipelines(ReplicationConfig arg0, StorageTier arg1) {
        return invoker.getImpl().getPipelines(arg0, arg1);
      }

      @Override
      public List<Pipeline> getPipelines(ReplicationConfig arg0, Pipeline.PipelineState arg1, StorageTier arg2) {
        return invoker.getImpl().getPipelines(arg0, arg1, arg2);
      }

      @Override
      public List<Pipeline> getPipelines(ReplicationConfig arg0, Pipeline.PipelineState arg1, Collection arg2,
          Collection arg3, StorageTier arg4) {
        return invoker.getImpl().getPipelines(arg0, arg1, arg2, arg3, arg4);
      }

      @Override
      public void reinitialize(Table arg0) throws RocksDatabaseException, DuplicatedPipelineIdException,
          CodecException {
        invoker.getImpl().reinitialize(arg0);
      }

      @Override
      public void removeContainerFromPipeline(PipelineID arg0, ContainerID arg1) {
        invoker.getImpl().removeContainerFromPipeline(arg0, arg1);
      }

      @Override
      public void removePipeline(HddsProtos.PipelineID arg0) throws IOException {
        final Object[] args = {arg0};
        invoker.invokeReplicateDirect(ReplicateMethod.removePipeline, args);
      }

      @Override
      public void updatePipelineState(HddsProtos.PipelineID arg0, HddsProtos.PipelineState arg1) throws IOException {
        final Object[] args = {arg0, arg1};
        invoker.invokeReplicateDirect(ReplicateMethod.updatePipelineState, args);
      }
    };
  }

  @SuppressWarnings("unchecked")
  @Override
  public Message invokeLocal(String methodName, Object[] p) throws Exception {
    final Class<?> returnType;
    final Object returnValue;
    switch (methodName) {
    case "addContainerToPipeline":
      final PipelineID arg0 = p.length > 0 ? (PipelineID) p[0] : null;
      final ContainerID arg1 = p.length > 1 ? (ContainerID) p[1] : null;
      getImpl().addContainerToPipeline(arg0, arg1);
      return Message.EMPTY;

    case "addContainerToPipelineForce":
      final PipelineID arg2 = p.length > 0 ? (PipelineID) p[0] : null;
      final ContainerID arg3 = p.length > 1 ? (ContainerID) p[1] : null;
      getImpl().addContainerToPipelineForce(arg2, arg3);
      return Message.EMPTY;

    case "addPipeline":
      final HddsProtos.Pipeline arg4 = p.length > 0 ? (HddsProtos.Pipeline) p[0] : null;
      getImpl().addPipeline(arg4);
      return Message.EMPTY;

    case "close":
      getImpl().close();
      return Message.EMPTY;

    case "getContainers":
      final PipelineID arg5 = p.length > 0 ? (PipelineID) p[0] : null;
      returnType = NavigableSet.class;
      returnValue = getImpl().getContainers(arg5);
      break;

    case "getNumberOfContainers":
      final PipelineID arg6 = p.length > 0 ? (PipelineID) p[0] : null;
      returnType = int.class;
      returnValue = getImpl().getNumberOfContainers(arg6);
      break;

    case "getPipeline":
      final PipelineID arg7 = p.length > 0 ? (PipelineID) p[0] : null;
      returnType = Pipeline.class;
      returnValue = getImpl().getPipeline(arg7);
      break;

    case "getPipelineCount":
      final ReplicationConfig arg8 = p.length > 0 ? (ReplicationConfig) p[0] : null;
      final Pipeline.PipelineState arg9 = p.length > 1 ? (Pipeline.PipelineState) p[1] : null;
      returnType = int.class;
      returnValue = getImpl().getPipelineCount(arg8, arg9);
      break;

    case "getPipelines":
      if (p.length == 0) {
        returnType = List.class;
        returnValue = getImpl().getPipelines();
        break;
      }
      if (p.length == 1 && (p[0] == null || ReplicationConfig.class.isInstance(p[0]))) {
        final ReplicationConfig arg10 = (ReplicationConfig) p[0];
        returnType = List.class;
        returnValue = getImpl().getPipelines(arg10);
        break;
      }
      if (p.length == 2 && (p[0] == null || ReplicationConfig.class.isInstance(p[0])) && (p[1] == null ||
          Pipeline.PipelineState.class.isInstance(p[1]))) {
        final ReplicationConfig arg11 = (ReplicationConfig) p[0];
        final Pipeline.PipelineState arg12 = (Pipeline.PipelineState) p[1];
        returnType = List.class;
        returnValue = getImpl().getPipelines(arg11, arg12);
        break;
      }
      if (p.length == 2 && (p[0] == null || ReplicationConfig.class.isInstance(p[0])) && (p[1] == null ||
          StorageTier.class.isInstance(p[1]))) {
        final ReplicationConfig arg13 = (ReplicationConfig) p[0];
        final StorageTier arg14 = (StorageTier) p[1];
        returnType = List.class;
        returnValue = getImpl().getPipelines(arg13, arg14);
        break;
      }
      if (p.length == 3 && (p[0] == null || ReplicationConfig.class.isInstance(p[0])) && (p[1] == null ||
          Pipeline.PipelineState.class.isInstance(p[1])) && (p[2] == null || StorageTier.class.isInstance(p[2]))) {
        final ReplicationConfig arg15 = (ReplicationConfig) p[0];
        final Pipeline.PipelineState arg16 = (Pipeline.PipelineState) p[1];
        final StorageTier arg17 = (StorageTier) p[2];
        returnType = List.class;
        returnValue = getImpl().getPipelines(arg15, arg16, arg17);
        break;
      }
      if (p.length == 5 && (p[0] == null || ReplicationConfig.class.isInstance(p[0])) && (p[1] == null ||
          Pipeline.PipelineState.class.isInstance(p[1])) && (p[2] == null || Collection.class.isInstance(p[2])) && (p[3]
          == null || Collection.class.isInstance(p[3])) && (p[4] == null || StorageTier.class.isInstance(p[4]))) {
        final ReplicationConfig arg18 = (ReplicationConfig) p[0];
        final Pipeline.PipelineState arg19 = (Pipeline.PipelineState) p[1];
        final Collection arg20 = (Collection) p[2];
        final Collection arg21 = (Collection) p[3];
        final StorageTier arg22 = (StorageTier) p[4];
        returnType = List.class;
        returnValue = getImpl().getPipelines(arg18, arg19, arg20, arg21, arg22);
        break;
      }
      throw new IllegalArgumentException("Method not found: " + methodName + " in PipelineStateManager");

    case "reinitialize":
      final Table arg23 = p.length > 0 ? (Table) p[0] : null;
      getImpl().reinitialize(arg23);
      return Message.EMPTY;

    case "removeContainerFromPipeline":
      final PipelineID arg24 = p.length > 0 ? (PipelineID) p[0] : null;
      final ContainerID arg25 = p.length > 1 ? (ContainerID) p[1] : null;
      getImpl().removeContainerFromPipeline(arg24, arg25);
      return Message.EMPTY;

    case "removePipeline":
      final HddsProtos.PipelineID arg26 = p.length > 0 ? (HddsProtos.PipelineID) p[0] : null;
      getImpl().removePipeline(arg26);
      return Message.EMPTY;

    case "updatePipelineState":
      final HddsProtos.PipelineID arg27 = p.length > 0 ? (HddsProtos.PipelineID) p[0] : null;
      final HddsProtos.PipelineState arg28 = p.length > 1 ? (HddsProtos.PipelineState) p[1] : null;
      getImpl().updatePipelineState(arg27, arg28);
      return Message.EMPTY;

    default:
      throw new IllegalArgumentException("Method not found: " + methodName + " in PipelineStateManager");
    }

    return SCMRatisResponse.encode(returnValue, returnType);
  }
}
