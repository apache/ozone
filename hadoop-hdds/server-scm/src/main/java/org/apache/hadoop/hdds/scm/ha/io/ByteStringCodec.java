package org.apache.hadoop.hdds.scm.ha.io;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A dummy codec that serializes a ByteString object to ByteString.
 */
public class ByteStringCodec implements Codec {

  @Override
  public ByteString serialize(Object object)
      throws InvalidProtocolBufferException {
    return (ByteString) object;
  }

  @Override
  public Object deserialize(Class<?> type, ByteString value)
      throws InvalidProtocolBufferException {
    return value;
  }
}
