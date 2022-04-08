package org.apache.hadoop.hdds.scm.metadata;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hdds.utils.db.Codec;

import java.io.IOException;

/**
 * Codec to serialize/deserialize a {@link ByteString}.
 */
public class ByteStringCodec implements Codec<ByteString> {

  /**
   * Convert object to raw persisted format.
   *
   * @param object The original java object. Should not be null.
   */
  @Override
  public byte[] toPersistedFormat(ByteString object) throws IOException {
    if (object == null) {
      return new byte[0];
    }
    return object.toByteArray();
  }

  /**
   * Convert object from raw persisted format.
   *
   * @param rawData Byte array from the key/value store. Should not be null.
   */
  @Override
  public ByteString fromPersistedFormat(byte[] rawData) throws IOException {
    if (rawData == null) {
      return ByteString.EMPTY;
    }
    return ByteString.copyFrom(rawData);
  }

  /**
   * Copy Object from the provided object, and returns a new object.
   *
   * @param object a ByteString
   */
  @Override
  public ByteString copyObject(ByteString object) {
    if (object == null) {
      return ByteString.EMPTY;
    }
    return ByteString.copyFrom(object.toByteArray());
  }
}
