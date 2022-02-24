/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.codec;

import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.TransactionInfoCodec;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.fail;

/**
 * Class to test {@link TransactionInfoCodec}.
 */
public class TestTransactionInfoCodec {
  @Rule
  public ExpectedException thrown = ExpectedException.none();


  private TransactionInfoCodec codec;

  @Before
  public void setUp() {
    codec = new TransactionInfoCodec();
  }
  @Test
  public void toAndFromPersistedFormat() throws Exception {
    TransactionInfo transactionInfo =
        new TransactionInfo.Builder().setTransactionIndex(100)
            .setCurrentTerm(11).build();

    TransactionInfo convertedTransactionInfo =
        codec.fromPersistedFormat(codec.toPersistedFormat(transactionInfo));

    Assert.assertEquals(transactionInfo, convertedTransactionInfo);

  }
  @Test
  public void testCodecWithNullDataFromTable() throws Exception {
    thrown.expect(NullPointerException.class);
    codec.fromPersistedFormat(null);
  }


  @Test
  public void testCodecWithNullDataFromUser() throws Exception {
    thrown.expect(NullPointerException.class);
    codec.toPersistedFormat(null);
  }


  @Test
  public void testCodecWithIncorrectValues() throws Exception {
    try {
      codec.fromPersistedFormat("random".getBytes(StandardCharsets.UTF_8));
      fail("testCodecWithIncorrectValues failed");
    } catch (IllegalStateException ex) {
      GenericTestUtils.assertExceptionContains("Incorrect TransactionInfo " +
          "value", ex);
    }
  }
}
