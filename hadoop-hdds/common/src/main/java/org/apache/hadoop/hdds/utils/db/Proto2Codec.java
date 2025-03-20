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

package org.apache.hadoop.hdds.utils.db;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.Message;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import com.google.protobuf.WireFormat;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ratis.util.function.CheckedFunction;

/**
 * Codecs to serialize/deserialize Protobuf v2 messages.
 */
public final class Proto2Codec<M extends MessageLite>
    implements Codec<M> {
  private static final ConcurrentMap<Pair<Class<? extends MessageLite>, Set<String>>,
                                     Codec<? extends MessageLite>> CODECS
      = new ConcurrentHashMap<>();

  /**
   * @return the {@link Codec} for the given class.
   */
  public static <T extends MessageLite> Codec<T> get(T t) {
    return get(t, Collections.emptySet());
  }

  /**
   * @return the {@link Codec} for the given class.
   */
  public static <T extends MessageLite> Codec<T> get(T t, Set<String> fieldsToBeSkipped) {
    final Codec<?> codec = CODECS.computeIfAbsent(Pair.of(t.getClass(), fieldsToBeSkipped),
        key -> new Proto2Codec<>(t, fieldsToBeSkipped));
    return (Codec<T>) codec;
  }

  private final Class<M> clazz;
  private final Parser<M> parser;
  private final Descriptors.Descriptor descriptor;
  private final Supplier<Message.Builder> builderSupplier;
  private final Set<String> fieldsToBeSkipped;

  private Proto2Codec(M m, Set<String> fieldsToBeSkipped) {
    this.clazz = (Class<M>) m.getClass();
    this.parser = (Parser<M>) m.getParserForType();
    this.descriptor = ((Message)m).getDescriptorForType();
    this.fieldsToBeSkipped = fieldsToBeSkipped;
    this.builderSupplier = ((Message)m)::newBuilderForType;
  }

  @Override
  public Class<M> getTypeClass() {
    return clazz;
  }

  @Override
  public boolean supportCodecBuffer() {
    return true;
  }

  @Override
  public CodecBuffer toCodecBuffer(@Nonnull M message,
      CodecBuffer.Allocator allocator) throws IOException {
    final int size = message.getSerializedSize();
    return allocator.apply(size).put(writeTo(message, size));
  }

  private CheckedFunction<OutputStream, Integer, IOException> writeTo(
      M message, int size) {
    return out -> {
      message.writeTo(out);
      return size;
    };
  }

  @Override
  public M fromCodecBuffer(@Nonnull CodecBuffer buffer)
      throws IOException {
    try (InputStream in = buffer.getInputStream()) {
      if (this.fieldsToBeSkipped.isEmpty()) {
        return parser.parseFrom(in);
      } else {
        return parse(CodedInputStream.newInstance(in));
      }
    }
  }

  private Object getValue(CodedInputStream input, Descriptors.FieldDescriptor field) throws IOException {
    Object value;
    switch (field.getType()) {
    case DOUBLE:
      value = input.readDouble();
      break;
    case FLOAT:
      value = input.readFloat();
      break;
    case INT64:
      value = input.readInt64();
      break;
    case UINT64:
      value = input.readUInt64();
      break;
    case INT32:
      value = input.readInt32();
      break;
    case FIXED64:
      value = input.readFixed64();
      break;
    case FIXED32:
      value = input.readFixed32();
      break;
    case BOOL:
      value = input.readBool();
      break;
    case STRING:
      value = input.readString();
      break;
    case GROUP:
    case MESSAGE:
      value = DynamicMessage.newBuilder(field.getMessageType());
      input.readMessage((MessageLite.Builder) value,
          ExtensionRegistryLite.getEmptyRegistry());
      value = ((MessageLite.Builder) value).build();
      break;
    case BYTES:
      value = input.readBytes();
      break;
    case UINT32:
      value = input.readUInt32();
      break;
    case ENUM:
      value = field.getEnumType().findValueByNumber(input.readEnum());
      System.out.println(((Descriptors.EnumValueDescriptor)value).getName());
      break;
    case SFIXED32:
      value = input.readSFixed32();
      break;
    case SFIXED64:
      value = input.readSFixed64();
      break;
    case SINT32:
      value = input.readSInt32();
      break;
    case SINT64:
      value = input.readSInt64();
      break;
    default:
      throw new UnsupportedOperationException();
    }
    System.out.println(field.getName() + ": " + value);
    return value;
  }

  private M parse(CodedInputStream codedInputStream) throws IOException {
    Message.Builder builder = this.builderSupplier.get();
    while (!codedInputStream.isAtEnd()) {
      int tag = codedInputStream.readTag();

      if (tag == 0) {
        break;
      }
      int fieldNumber = WireFormat.getTagFieldNumber(tag);

      final Descriptors.FieldDescriptor field = descriptor.findFieldByNumber(fieldNumber);
      if (field != null && !this.fieldsToBeSkipped.contains(field.getName())) {
        try {
          Object value = getValue(codedInputStream, field);
          if (field.isRepeated()) {
            builder.addRepeatedField(field, value);
          } else {
            builder.setField(field, value);
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      } else {
        codedInputStream.skipField(tag);
      }
    }
    return (M) builder.build();
  }

  @Override
  public byte[] toPersistedFormat(M message) {
    return message.toByteArray();
  }


  @Override
  public M fromPersistedFormat(byte[] bytes)
      throws IOException {
    if (fieldsToBeSkipped.isEmpty()) {
      return parser.parseFrom(bytes);
    } else {
      return parse(CodedInputStream.newInstance(bytes));
    }

  }

  @Override
  public M copyObject(M message) {
    // proto messages are immutable
    return message;
  }
}
