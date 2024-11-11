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

package org.apache.hadoop.ozone.om.ratis.execution.factory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.ratis.execution.request.OMKeyCommitRequest;
import org.apache.hadoop.ozone.om.ratis.execution.request.OMKeyCreateRequest;
import org.apache.hadoop.ozone.om.ratis.execution.request.OMKeyRequestBase;
import org.apache.hadoop.ozone.om.ratis.execution.request.OmKeyUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory class to instantiate bucket layout aware request classes.
 */
public final class BucketLayoutAwareOMKeyRequestFactory {
  private static final Logger LOG = LoggerFactory.getLogger(BucketLayoutAwareOMKeyRequestFactory.class);

  static final HashMap<String, Class<? extends OMKeyRequestBase>> OM_KEY_REQUEST_CLASSES = new HashMap<>();

  static {
    // CreateKey
    addRequestClass(Type.CreateKey, OMKeyCreateRequest.class, BucketLayout.OBJECT_STORE);

    // CommitKey
    addRequestClass(Type.CommitKey, OMKeyCommitRequest.class, BucketLayout.OBJECT_STORE);
  }

  private BucketLayoutAwareOMKeyRequestFactory() {
    // Utility class.
  }

  /**
   * Generates a request object for the given request based on the bucket
   * layout.
   *
   * @param volumeName   volume name
   * @param bucketName   bucket name
   * @param omRequest    OMRequest object
   * @param omMetadataManager ozone metadata manager instance
   * @return OMKeyRequest object
   * @throws IOException if the request type is not supported.
   */
  public static OMKeyRequestBase createRequest(
      String volumeName, String bucketName, OMRequest omRequest, OMMetadataManager omMetadataManager)
      throws IOException {
    if (StringUtils.isBlank(volumeName)) {
      throw new OMException("Invalid, volume name is empty", OMException.ResultCodes.INVALID_VOLUME_NAME);
    }

    if (StringUtils.isBlank(bucketName)) {
      throw new OMException("Invalid, Bucket name is empty", OMException.ResultCodes.INVALID_BUCKET_NAME);
    }

    // Get the bucket layout of the bucket being accessed by this request.
    // While doing this we make sure we are resolving the real bucket in case of
    // link buckets.
    OmBucketInfo bucketInfo = OmKeyUtils.resolveBucketLink(omMetadataManager, volumeName, bucketName, new HashSet<>());
    BucketLayout bucketLayout = bucketInfo.getBucketLayout();

    // Get the CmdType.
    Type requestType = omRequest.getCmdType();

    // If the request class is associated to FSO bucket layout,
    // we add a suffix to its key in the map.
    String classKey = getKey(requestType, bucketLayout);

    // Check if the key is present in the map.
    if (OM_KEY_REQUEST_CLASSES.containsKey(classKey)) {
      try {
        return getRequestInstanceFromMap(omRequest, classKey, bucketInfo);
      } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
        String errMsg = "Exception while instantiating OMKeyRequest of type " + requestType + " for bucket layout "
            + bucketLayout + ". Please check the OMKeyRequest class constructor.";
        LOG.error(errMsg, e);
        throw new OMException(errMsg, OMException.ResultCodes.INTERNAL_ERROR);
      }
    }

    // We did not find this key in the map, it means this request type is not
    // supported.
    throw new OMException("Request type " + requestType + " not supported with bucket layout " +
        bucketLayout, OMException.ResultCodes.NOT_SUPPORTED_OPERATION);
  }

  /**
   * Helper method to add a request class to the omKeyReqClasses map.
   *
   * @param requestType            type of the request
   * @param requestClass           Request class to be added.
   * @param bucketLayout           bucket layout of the request is associated with.
   */
  static void addRequestClass(Type requestType, Class<? extends OMKeyRequestBase> requestClass,
                              BucketLayout bucketLayout) {
    // If the request class is associated to FSO bucket layout,
    // we add a suffix to its key in the map.
    OM_KEY_REQUEST_CLASSES.put(getKey(requestType, bucketLayout), requestClass);
  }

  /**
   * Finds the Request class associated with the given OMRequest and bucket
   * layout, and returns an instance of the same.
   *
   * @param omRequest    OMRequest object
   * @param classKey     key to be looked up in the map.
   * @param bucketInfo   Bucket info of the bucket associated with the request.
   * @return OMKeyRequest object
   * @throws NoSuchMethodException     if the request class does not have a constructor that takes OMRequest and
   *                                   BucketLayout as arguments.
   * @throws InstantiationException    if the request class is abstract.
   * @throws IllegalAccessException    if the request class is not public.
   * @throws InvocationTargetException if the request class constructor throws an exception.
   */
  static OMKeyRequestBase getRequestInstanceFromMap(OMRequest omRequest, String classKey, OmBucketInfo bucketInfo)
      throws NoSuchMethodException, InstantiationException,
      IllegalAccessException, InvocationTargetException {
    // Get the constructor of the request class.
    // The constructor takes OMRequest and BucketLayout as parameters.
    Constructor<? extends OMKeyRequestBase> declaredConstructor =
        OM_KEY_REQUEST_CLASSES.get(classKey).getDeclaredConstructor(OMRequest.class, OmBucketInfo.class);

    // Invoke the constructor.
    return declaredConstructor.newInstance(omRequest, bucketInfo);
  }

  /**
   * Generates a key name for a request type and bucket layout.
   *
   * @param requestType  type of the request
   * @param bucketLayout Flavor of the request types based on the bucket layout.
   * @return key name for the request type.
   */
  static String getKey(Type requestType, BucketLayout bucketLayout) {
    return requestType.toString() +
        (bucketLayout.isFileSystemOptimized() ? bucketLayout : "");
  }
}
