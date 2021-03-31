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

import com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;

/**
 * Class to test BigIntegerCodec serialize and deserialize.
 */
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
