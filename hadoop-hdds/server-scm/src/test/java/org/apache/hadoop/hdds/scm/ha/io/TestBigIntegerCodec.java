package org.apache.hadoop.hdds.scm.ha.io;

import com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;

public class TestBigIntegerCodec {

  @Test
  public void testCodec() {
    BigIntegerCodec bigIntegerCodec = new BigIntegerCodec();

    BigInteger bigInteger = BigInteger.valueOf(100);
    ByteString byteString = bigIntegerCodec.serialize(bigInteger);

    BigInteger actual =
        (BigInteger) bigIntegerCodec.deserialize(BigInteger.class, byteString);
    Assert.assertEquals(bigInteger, actual);
  }
}
