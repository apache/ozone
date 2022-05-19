package org.apache.hadoop.hdds.scm.ha.io;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class IntegerCodec implements Codec {
  @Override
  public ByteString serialize(Object object)
      throws InvalidProtocolBufferException {
    return ByteString.copyFrom(Ints.toByteArray((Integer) object));
  }

  @Override
  public Object deserialize(Class<?> type, ByteString value)
      throws InvalidProtocolBufferException {
    return Longs.fromByteArray(value.toByteArray());
  }
}
