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

package org.apache.hadoop.ozone.om.exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.junit.jupiter.api.Test;

/**
 * Test code mappping.
 */
public class TestResultCodes {

  @Test
  public void codeMapping() {
    assertEquals(ResultCodes.values().length,
        Status.values().length);
    for (int i = 0; i < ResultCodes.values().length; i++) {
      ResultCodes codeValue = ResultCodes.values()[i];
      Status protoBufValue = Status.values()[i];
      assertTrue(sameName(codeValue.name(), protoBufValue.name()),
          String.format("Protobuf/Enum constant name mismatch %s %s", codeValue,
              protoBufValue));
      ResultCodes converted = ResultCodes.values()[protoBufValue.ordinal()];
      assertEquals(codeValue, converted);
    }
  }

  private boolean sameName(String codeValue, String protoBufValue) {
    return codeValue.equals(protoBufValue);
  }
}
