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

package org.apache.hadoop.hdds.scm.ha.io;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ProtocolMessageEnum;

import java.math.BigInteger;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Maps types to the corresponding {@link Codec} implementation.
 */
public final class CodecFactory {

  private static Map<Class<?>, Codec> codecs = new HashMap<>();

  static {
    codecs.put(GeneratedMessage.class, new GeneratedMessageCodec());
    codecs.put(ProtocolMessageEnum.class, new EnumCodec());
    codecs.put(List.class, new ListCodec());
    codecs.put(Long.class, new LongCodec());
    codecs.put(String.class, new StringCodec());
    codecs.put(Boolean.class, new BooleanCodec());
    codecs.put(BigInteger.class, new BigIntegerCodec());
    codecs.put(X509Certificate.class, new X509CertificateCodec());
  }

  private CodecFactory() {}

  public static Codec getCodec(Class<?> type)
      throws InvalidProtocolBufferException {
    final List<Class<?>> classes = new ArrayList<>();
    classes.add(type);
    classes.add(type.getSuperclass());
    classes.addAll(Arrays.asList(type.getInterfaces()));
    for (Class<?> clazz : classes) {
      if (codecs.containsKey(clazz)) {
        return codecs.get(clazz);
      }
    }
    throw new InvalidProtocolBufferException(
        "Codec for " + type + " not found!");
  }
}
