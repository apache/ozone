package org.apache.hadoop.hdds.scm.ha.io;

import java.lang.reflect.InvocationTargetException;
import org.apache.hadoop.hdds.scm.ha.ReflectionUtil;
import com.google.protobuf.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

public class GeneratedMessageCodec implements Codec {

  @Override
  public ByteString serialize(Object object)
      throws org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException {
    final byte[] bytes = ((Message) object).toByteString().toByteArray();
    return ByteString.copyFrom(bytes);
  }

  @Override
  public Object deserialize(Class<?> type, ByteString value)
      throws org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException {
    try {
      return ReflectionUtil.getMethod(type, "parseFrom", byte[].class)
          .invoke(null, (Object) value.toByteArray());
    } catch (NoSuchMethodException | IllegalAccessException
             | InvocationTargetException ex) {
      ex.printStackTrace();
      throw new InvalidProtocolBufferException("Message cannot be decoded: " + ex.getMessage());
    }
  }
}
