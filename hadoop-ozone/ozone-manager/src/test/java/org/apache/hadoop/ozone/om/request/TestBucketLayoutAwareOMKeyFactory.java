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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.request;

import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;

import static org.junit.Assert.fail;

/**
 * Validates functionality of {@link BucketLayoutAwareOMKeyRequestFactory}.
 */
public class TestBucketLayoutAwareOMKeyFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestBucketLayoutAwareOMKeyFactory.class);

  /**
   * Validates instantiation of each OMKeyRequest present inside the
   * (CmdType + BucketLayout) -> RequestClass Mapping.
   */
  @Test
  public void testGetRequestInstanceFromMap() {
    // Iterate over each OMKeyRequest present in the mapping.
    BucketLayoutAwareOMKeyRequestFactory.OM_KEY_REQUEST_CLASSES.forEach(
        (k, v) -> {
          // Check if this key is associated with an FSO class.
          if (k.contains(BucketLayout.FILE_SYSTEM_OPTIMIZED.toString())) {
            try {
              // Get the declared constructor.
              BucketLayoutAwareOMKeyRequestFactory.getRequestInstanceFromMap(
                  getDummyOMRequest(), k, BucketLayout.FILE_SYSTEM_OPTIMIZED);
            } catch (NoSuchMethodException e) {
              fail("No valid constructor found for " + k);
              e.printStackTrace();
            } catch (InvocationTargetException | InstantiationException
                | IllegalAccessException e) {
              fail("Exception while creating instance of " + k);
              e.printStackTrace();
            }
          } else {
            // This is a LEGACY / OBS Request class.
            try {
              BucketLayoutAwareOMKeyRequestFactory.getRequestInstanceFromMap(
                  getDummyOMRequest(), k, BucketLayout.LEGACY);
              BucketLayoutAwareOMKeyRequestFactory.getRequestInstanceFromMap(
                  getDummyOMRequest(), k, BucketLayout.OBJECT_STORE);
            } catch (NoSuchMethodException e) {
              fail("No valid constructor found for " + k);
              e.printStackTrace();
            } catch (InvocationTargetException | InstantiationException
                | IllegalAccessException e) {
              fail("Exception while creating instance of " + k);
              e.printStackTrace();
            }
          }

          LOG.info("Validated request class instantiation for cmdType " + k);
        });
  }

  /**
   * Generates a dummy OMRequest.
   *
   * @return OMRequest
   */
  private OMRequest getDummyOMRequest() {
    return OMRequest.newBuilder()
        // Set random type.
        .setCmdType(Type.CreateKey)
        .setClientId("xyz")
        .build();
  }
}
