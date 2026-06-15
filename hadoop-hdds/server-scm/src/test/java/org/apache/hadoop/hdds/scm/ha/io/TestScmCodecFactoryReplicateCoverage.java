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

package org.apache.hadoop.hdds.scm.ha.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.StringJoiner;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogStateManager;
import org.apache.hadoop.hdds.scm.container.ContainerStateManager;
import org.apache.hadoop.hdds.scm.ha.SCMRatisRequest;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.ha.StatefulServiceStateManager;
import org.apache.hadoop.hdds.scm.metadata.Replicate;
import org.apache.hadoop.hdds.scm.pipeline.PipelineStateManager;
import org.apache.hadoop.hdds.scm.security.RootCARotationHandler;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationStateManager;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyState;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;

/**
 * HA Ratis payloads use {@link ScmCodecFactory} inside {@link SCMRatisRequest}.
 * Stub-based tests often skip {@link SCMRatisRequest#encode()}, so codec gaps go unnoticed.
 * <p>
 * Scan every {@link Replicate} method on known SCM handlers and asserts each parameter type resolves to a codec. 
 * Add new handler interfaces to {@link #replicateHandlerTypes()} when they gain {@link Replicate} APIs.
 */
public class TestScmCodecFactoryReplicateCoverage {

  private static final Class<?>[] REPLICATE_HANDLER_TYPES =
      replicateHandlerTypes();

  /**
   * Handler types that declare {@link Replicate} methods.
   * Extend when a new SCM component exposes {@link Replicate}-annotated APIs.
   */
  private static Class<?>[] replicateHandlerTypes() {
    return new Class<?>[] {
        CertificateStore.class,
        ContainerStateManager.class,
        DeletedBlockLogStateManager.class,
        FinalizationStateManager.class,
        PipelineStateManager.class,
        RootCARotationHandler.class,
        SecretKeyState.class,
        SequenceIdGenerator.StateManager.class,
        StatefulServiceStateManager.class,
    };
  }

  @Test
  public void replicateApisRegisterParameterCodecsInScmCodecFactory() {
    ScmCodecFactory factory = ScmCodecFactory.getInstance();
    List<String> errors = new ArrayList<>();
    for (Class<?> handler : REPLICATE_HANDLER_TYPES) {
      for (Method m : handler.getMethods()) {
        if (!m.isAnnotationPresent(Replicate.class)) {
          continue;
        }
        int i = 0;
        for (Parameter p : m.getParameters()) {
          try {
            assertTypeResolvable(factory, p.getParameterizedType());
          } catch (InvalidProtocolBufferException e) {
            errors.add(handler.getSimpleName() + "#" + m.getName()
                + " param " + i + " (" + p.getParameterizedType().getTypeName()
                + "): " + e.getMessage());
          }
          i++;
        }
      }
    }
    if (!errors.isEmpty()) {
      StringJoiner sj = new StringJoiner(System.lineSeparator());
      sj.add("ScmCodecFactory missing codecs for @Replicate parameters:");
      errors.forEach(sj::add);
      fail(sj.toString());
    }
  }

  @Test
  public void scmRatisRequestTransitionDeletingOrDeletedToTargetState() throws Exception {
    HddsProtos.ContainerID id = HddsProtos.ContainerID.newBuilder().setId(1L).build();
    HddsProtos.LifeCycleState state = HddsProtos.LifeCycleState.CLOSED;
    SCMRatisRequest req = SCMRatisRequest.of(
        RequestType.CONTAINER,
        "transitionDeletingOrDeletedToTargetState",
        new Class<?>[] {
            HddsProtos.ContainerID.class,
            HddsProtos.LifeCycleState.class},
        id, state);

    Message encoded = req.encode();
    SCMRatisRequest decoded = SCMRatisRequest.decode(encoded);
    assertEquals(id, decoded.getArguments()[0]);
    assertEquals(state, decoded.getArguments()[1]);
  }

  private static void assertTypeResolvable(ScmCodecFactory factory, Type type)
      throws InvalidProtocolBufferException {
    if (type instanceof Class<?>) {
      Class<?> c = (Class<?>) type;
      Class<?> resolved = factory.resolve(c);
      factory.getCodec(resolved);
      return;
    }
    if (type instanceof ParameterizedType) {
      ParameterizedType pt = (ParameterizedType) type;
      Type raw = pt.getRawType();
      if (!(raw instanceof Class<?>)) {
        return;
      }
      Class<?> rawClass = (Class<?>) raw;
      if (Collection.class.isAssignableFrom(rawClass)) {
        for (Type typeArg : pt.getActualTypeArguments()) {
          assertTypeResolvable(factory, typeArg);
        }
      }
      Class<?> resolved = factory.resolve(rawClass);
      factory.getCodec(resolved);
      return;
    }
    if (type instanceof WildcardType) {
      WildcardType wt = (WildcardType) type;
      for (Type bound : wt.getUpperBounds()) {
        if (!bound.equals(Object.class)) {
          assertTypeResolvable(factory, bound);
        }
      }
    }
  }
}
