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

package org.apache.hadoop.ipc_;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

class TestProtobufRpcEngineRedaction {

  private static final String CUSTOM_SECRET = "custom-secret-sentinel";
  private static final String PLAINTEXT_SECRET = "plaintext-secret-sentinel";
  private static final String RETRIEVAL_HANDLE = "retrieval-handle-sentinel";
  private static final String ENCRYPTED_SECRET = "encrypted-secret-sentinel";
  private static final String POLICY = "{\"Statement\":\"policy-sentinel\"}";

  @Test
  void safeShortDebugStringRedactsSensitiveFields() throws Exception {
    Descriptors.FileDescriptor fileDescriptor =
        Descriptors.FileDescriptor.buildFrom(testDescriptor(),
            new Descriptors.FileDescriptor[0]);
    Descriptors.Descriptor innerDescriptor =
        fileDescriptor.findMessageTypeByName("Inner");
    Descriptors.Descriptor outerDescriptor =
        fileDescriptor.findMessageTypeByName("Outer");

    DynamicMessage inner = DynamicMessage.newBuilder(innerDescriptor)
        .setField(innerDescriptor.findFieldByName("customSecret"),
            CUSTOM_SECRET)
        .setField(innerDescriptor.findFieldByName("plaintextSecret"),
            PLAINTEXT_SECRET)
        .setField(innerDescriptor.findFieldByName("retrievalHandle"),
            RETRIEVAL_HANDLE)
        .setField(innerDescriptor.findFieldByName("encryptedSecretKey"),
            ByteString.copyFromUtf8(ENCRYPTED_SECRET))
        .setField(innerDescriptor.findFieldByName("policyDocument"), POLICY)
        .build();
    DynamicMessage outer = DynamicMessage.newBuilder(outerDescriptor)
        .setField(outerDescriptor.findFieldByName("inner"), inner)
        .build();

    String debug = safeShortDebugString(outer);

    assertThat(debug).doesNotContain(CUSTOM_SECRET);
    assertThat(debug).doesNotContain(PLAINTEXT_SECRET);
    assertThat(debug).doesNotContain(RETRIEVAL_HANDLE);
    assertThat(debug).doesNotContain(ENCRYPTED_SECRET);
    assertThat(debug).doesNotContain(POLICY);
    assertThat(debug).contains("<redacted>");
  }

  private static String safeShortDebugString(Message message) throws Exception {
    Method method = ProtobufRpcEngine.Invoker.class.getDeclaredMethod(
        "safeShortDebugString", Message.class);
    method.setAccessible(true);
    return (String) method.invoke(null, message);
  }

  private static DescriptorProtos.FileDescriptorProto testDescriptor() {
    DescriptorProtos.DescriptorProto inner =
        DescriptorProtos.DescriptorProto.newBuilder()
            .setName("Inner")
            .addField(stringField("customSecret", 1))
            .addField(stringField("plaintextSecret", 2))
            .addField(stringField("retrievalHandle", 3))
            .addField(bytesField("encryptedSecretKey", 4))
            .addField(stringField("policyDocument", 5))
            .build();
    DescriptorProtos.DescriptorProto outer =
        DescriptorProtos.DescriptorProto.newBuilder()
            .setName("Outer")
            .addField(FieldDescriptorProto.newBuilder()
                .setName("inner")
                .setNumber(1)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName(".ozone.test.Inner"))
            .build();
    return DescriptorProtos.FileDescriptorProto.newBuilder()
        .setName("managed_s3_access_key_redaction_test.proto")
        .setPackage("ozone.test")
        .addMessageType(inner)
        .addMessageType(outer)
        .build();
  }

  private static FieldDescriptorProto stringField(String name, int number) {
    return FieldDescriptorProto.newBuilder()
        .setName(name)
        .setNumber(number)
        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
        .setType(FieldDescriptorProto.Type.TYPE_STRING)
        .build();
  }

  private static FieldDescriptorProto bytesField(String name, int number) {
    return FieldDescriptorProto.newBuilder()
        .setName(name)
        .setNumber(number)
        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
        .setType(FieldDescriptorProto.Type.TYPE_BYTES)
        .build();
  }
}
