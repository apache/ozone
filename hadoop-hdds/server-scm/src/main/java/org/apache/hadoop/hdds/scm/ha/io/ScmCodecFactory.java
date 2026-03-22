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

import com.google.protobuf.Message;
import com.google.protobuf.ProtocolMessageEnum;
import java.math.BigInteger;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntFunction;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionSummary;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.Pipeline;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.PipelineID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.PipelineState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

/**
 * Maps types to the corresponding {@link ScmCodec} implementation.
 */
public final class ScmCodecFactory {

  private final Map<Class<?>, ScmCodec<?>> codecs = new HashMap<>();
  private final ClassResolver resolver;
  private static final ScmCodecFactory INSTANCE = new ScmCodecFactory();

  private ScmCodecFactory() {
    putProto(ContainerID.getDefaultInstance());
    putProto(PipelineID.getDefaultInstance());
    putProto(Pipeline.getDefaultInstance());
    putProto(ContainerInfoProto.getDefaultInstance());
    putProto(DeletedBlocksTransaction.getDefaultInstance());
    putProto(DeletedBlocksTransactionSummary.getDefaultInstance());

    codecs.put(Integer.class, new ScmIntegerCodec());
    codecs.put(Long.class, new ScmLongCodec());
    codecs.put(String.class, new ScmStringCodec());
    codecs.put(Boolean.class, new ScmBooleanCodec());
    codecs.put(BigInteger.class, new ScmBigIntegerCodec());
    codecs.put(X509Certificate.class, new ScmX509CertificateCodec());
    codecs.put(com.google.protobuf.ByteString.class, new ScmNonShadedByteStringCodec());
    codecs.put(ByteString.class, new ScmByteStringCodec());
    codecs.put(ManagedSecretKey.class, new ScmManagedSecretKeyCodec());

    putEnum(LifeCycleEvent.class, LifeCycleEvent::forNumber);
    putEnum(PipelineState.class, PipelineState::forNumber);
    putEnum(NodeType.class, NodeType::forNumber);

    // Must be the last one
    resolver = new ClassResolver(codecs.keySet());
    codecs.put(List.class, new ScmListCodec(resolver));
  }

  private <T extends Message> void putProto(T proto) {
    final Class<? extends Message> clazz = proto.getClass();
    codecs.put(clazz, new ScmNonShadedGeneratedMessageCodec<>(clazz.getSimpleName(), proto.getParserForType()));
  }

  private <T extends Enum<T> & ProtocolMessageEnum> void putEnum(
      Class<T> enumClass, IntFunction<T> forNumber) {
    codecs.put(enumClass, new ScmEnumCodec<>(enumClass, forNumber));
  }

  public static ScmCodecFactory getInstance() {
    return INSTANCE;
  }

  public Class<?> resolve(String className)
      throws InvalidProtocolBufferException {
    return resolver.get(className);
  }

  public ScmCodec getCodec(Class<?> resolved) throws InvalidProtocolBufferException {
    final ScmCodec<?> codec = codecs.get(resolved);
    if (codec != null) {
      return codec;
    }

    throw new InvalidProtocolBufferException("Codec not found for " + resolved);
  }

  /** Resolve the codec class from a given class. */
  static class ClassResolver {
    private final Map<String, Class<?>> provided;
    private final Map<String, Class<?>> resolved = new ConcurrentHashMap<>();

    ClassResolver(Collection<Class<?>> provided) {
      final Map<String, Class<?>> map = new TreeMap<>();
      for (Class<?> c : provided) {
        map.put(c.getName(), c);
      }
      map.put(List.class.getName(), List.class);
      this.provided = Collections.unmodifiableMap(map);
    }

    Class<?> get(String className) throws InvalidProtocolBufferException {
      return get(null, className);
    }

    Class<?> get(Class<?> clazz) throws InvalidProtocolBufferException {
      return get(clazz, clazz.getName());
    }

    Class<?> get(Class<?> clazz, String className) throws InvalidProtocolBufferException {
      Objects.requireNonNull(className, "className == null");
      final Class<?> c = provided.get(className);
      if (c != null) {
        return c;
      }

      final Class<?> found = resolved.get(className);
      if (found != null) {
        return found;
      }

      if (clazz == null) {
        try {
          clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
          throw new InvalidProtocolBufferException("Class not found for " + className, e);
        }
      }

      for (Class<?> base : provided.values()) {
        if (base.isAssignableFrom(clazz)) {
          resolved.put(className, base);
          return base;
        }
      }

      throw new InvalidProtocolBufferException("Failed to resolve " + className);
    }
  }
}
