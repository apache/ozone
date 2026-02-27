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

import java.math.BigInteger;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.ClassUtils;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.thirdparty.com.google.protobuf.Message;

/**
 * Maps types to the corresponding {@link ScmCodec} implementation.
 */
public final class ScmCodecFactory {

  private static Map<Class<?>, ScmCodec> codecs = new HashMap<>();

  static {
    codecs.put(com.google.protobuf.Message.class, new ScmNonShadedGeneratedMessageCodec());
    codecs.put(Message.class, new ScmGeneratedMessageCodec());
    codecs.put(List.class, new ScmListCodec());
    codecs.put(Integer.class, new ScmIntegerCodec());
    codecs.put(Long.class, new ScmLongCodec());
    codecs.put(String.class, new ScmStringCodec());
    codecs.put(Boolean.class, new ScmBooleanCodec());
    codecs.put(BigInteger.class, new ScmBigIntegerCodec());
    codecs.put(X509Certificate.class, new ScmX509CertificateCodec());
    codecs.put(com.google.protobuf.ByteString.class, new ScmNonShadedByteStringCodec());
    codecs.put(ByteString.class, new ScmByteStringCodec());
    codecs.put(HddsProtos.PipelineState.class, new ScmHddsPipelineStateCodec());
    codecs.put(ManagedSecretKey.class, new ScmManagedSecretKeyCodec());
    codecs.put(HddsProtos.LifeCycleEvent.class, new ScmHddsLifeCycleEventCodec());
    codecs.put(HddsProtos.NodeType.class, new ScmHddsNodeTypeCodec());
  }

  private ScmCodecFactory() { }

  public static ScmCodec getCodec(Class<?> type)
      throws InvalidProtocolBufferException {
    final List<Class<?>> classes = new ArrayList<>();
    classes.add(type);
    classes.addAll(ClassUtils.getAllSuperclasses(type));
    classes.addAll(ClassUtils.getAllInterfaces(type));
    for (Class<?> clazz : classes) {
      if (codecs.containsKey(clazz)) {
        return codecs.get(clazz);
      }
    }
    throw new InvalidProtocolBufferException(
        "Codec for " + type + " not found!");
  }
}
