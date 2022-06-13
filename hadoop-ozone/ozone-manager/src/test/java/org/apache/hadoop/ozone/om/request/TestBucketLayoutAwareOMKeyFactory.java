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
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.request.key.OMDirectoriesPurgeRequestWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.ozone.om.request.BucketLayoutAwareOMKeyRequestFactory.getKey;
import static org.apache.hadoop.ozone.om.request.BucketLayoutAwareOMKeyRequestFactory.getRequestInstanceFromMap;
import static org.apache.hadoop.ozone.om.request.BucketLayoutAwareOMKeyRequestFactory.OM_KEY_REQUEST_CLASSES;
import static org.apache.hadoop.ozone.om.request.BucketLayoutAwareOMKeyRequestFactory.addRequestClass;
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
    // Lists to count the number of times each OMKeyRequest is instantiated.
    List<OMKeyRequest> omKeyReqsFSO = new ArrayList<>();
    List<OMKeyRequest> omKeyReqsLegacy = new ArrayList<>();
    List<OMKeyRequest> omKeyReqsOBS = new ArrayList<>();

    // Iterate over each OMKeyRequest present in the mapping.
    OM_KEY_REQUEST_CLASSES.forEach(
        (k, v) -> {
          // Check if this key is associated with an FSO class.
          if (k.contains(BucketLayout.FILE_SYSTEM_OPTIMIZED.toString())) {
            try {
              // Get the declared constructor.
              OMKeyRequest omKeyRequest =
                  getRequestInstanceFromMap(
                      getDummyOMRequest(), k,
                      BucketLayout.FILE_SYSTEM_OPTIMIZED);

              Assert.assertEquals(BucketLayout.FILE_SYSTEM_OPTIMIZED,
                  omKeyRequest.getBucketLayout());
              omKeyReqsFSO.add(omKeyRequest);
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
              OMKeyRequest omKeyRequest1 =
                  getRequestInstanceFromMap(
                      getDummyOMRequest(), k, BucketLayout.LEGACY);

              Assert.assertEquals(BucketLayout.LEGACY,
                  omKeyRequest1.getBucketLayout());
              omKeyReqsLegacy.add(omKeyRequest1);

              OMKeyRequest omKeyRequest2 =
                  getRequestInstanceFromMap(
                      getDummyOMRequest(), k, BucketLayout.OBJECT_STORE);

              Assert.assertEquals(BucketLayout.OBJECT_STORE,
                  omKeyRequest2.getBucketLayout());
              omKeyReqsOBS.add(omKeyRequest2);
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

    Assert.assertEquals(12, omKeyReqsFSO.size());
    Assert.assertEquals(13, omKeyReqsLegacy.size());
    Assert.assertEquals(13, omKeyReqsOBS.size());
    // Check if the number of instantiated OMKeyRequest classes is equal to
    // the number of keys in the mapping.
    Assert.assertEquals(
        OM_KEY_REQUEST_CLASSES.size(),
        omKeyReqsFSO.size() + omKeyReqsOBS.size());
  }

  /**
   * Tests behaviour with invalid OMKeyRequest classes.
   *
   * @throws InvocationTargetException if the constructor throws an exception.
   * @throws InstantiationException    if the class is abstract.
   * @throws IllegalAccessException    if the constructor is not accessible.
   */
  @Test
  public void testAddInvalidRequestClass()
      throws InvocationTargetException,
      InstantiationException, IllegalAccessException {
    // Add an OMKeyRequest class that does not have a constructor compatible
    // with the Factory class.
    addRequestClass(Type.PurgeDirectories,
            OMDirectoriesPurgeRequestWithFSO.class,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);
    try {
      // This should fail, since this class does not have a valid constructor -
      // one that takes an OMRequest and a BucketLayout as parameters.
      getRequestInstanceFromMap(
          OMRequest.newBuilder()
              .setCmdType(Type.PurgeKeys)
              .setClientId("xyz")
              .build(),
          getKey(Type.PurgeDirectories, BucketLayout.FILE_SYSTEM_OPTIMIZED),
          BucketLayout.FILE_SYSTEM_OPTIMIZED);
      fail("No exception thrown for invalid OMKeyRequest class");
    } catch (NoSuchMethodException ex) {
      // expected exception.
      LOG.info("Expected exception thrown for invalid OMKeyRequest class", ex);
    }
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
