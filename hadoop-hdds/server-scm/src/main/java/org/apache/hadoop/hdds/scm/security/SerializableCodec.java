package org.apache.hadoop.hdds.scm.security;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hdds.scm.ha.io.Codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class SerializableCodec implements Codec {
  @Override
  public ByteString serialize(Object object)
      throws InvalidProtocolBufferException {
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(bos);
      oos.writeObject(object);
      oos.flush();
      return ByteString.copyFrom(bos.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException("Error serializing "
          + object.getClass().getCanonicalName(), e);
    }
  }

  @Override
  public Object deserialize(Class<?> type, ByteString value)
      throws InvalidProtocolBufferException {
    try {
      ByteArrayInputStream bis = new ByteArrayInputStream(value.toByteArray());
      ObjectInputStream ois = new ObjectInputStream(bis);
      return ois.readObject();
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException("Error deserializing "
          + type.getCanonicalName(), e);
    }
  }
}
