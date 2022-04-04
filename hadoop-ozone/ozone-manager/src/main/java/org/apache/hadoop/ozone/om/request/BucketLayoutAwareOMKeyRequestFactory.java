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

import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.request.file.OMDirectoryCreateRequest;
import org.apache.hadoop.ozone.om.request.file.OMDirectoryCreateRequestWithFSO;
import org.apache.hadoop.ozone.om.request.file.OMFileCreateRequest;
import org.apache.hadoop.ozone.om.request.file.OMFileCreateRequestWithFSO;
import org.apache.hadoop.ozone.om.request.key.*;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadCompleteRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadCompleteRequestWithFSO;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3InitiateMultipartUploadRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3InitiateMultipartUploadRequestWithFSO;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadCommitPartRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadCommitPartRequestWithFSO;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadAbortRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadAbortRequestWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;

import static org.apache.hadoop.ozone.om.OzoneManagerUtils.getBucketLayout;


/**
 * Factory class to instantiate bucket layout aware request classes.
 */
public final class BucketLayoutAwareOMKeyRequestFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(BucketLayoutAwareOMKeyRequestFactory.class);

  private static final HashMap<String, Class<? extends OMKeyRequest>>
      OM_KEY_REQUEST_CLASSES = new HashMap<>();

  static {
    // CreateDirectory
    addRequestClass(Type.CreateDirectory,
        OMDirectoryCreateRequest.class,
        BucketLayout.OBJECT_STORE
    );
    addRequestClass(Type.CreateDirectory,
        OMDirectoryCreateRequestWithFSO.class,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    // CreateFile
    addRequestClass(Type.CreateFile,
        OMFileCreateRequest.class,
        BucketLayout.OBJECT_STORE
    );
    addRequestClass(Type.CreateFile,
        OMFileCreateRequestWithFSO.class,
        BucketLayout.FILE_SYSTEM_OPTIMIZED
    );

    // CreateKey
    addRequestClass(Type.CreateKey,
        OMKeyCreateRequest.class,
        BucketLayout.OBJECT_STORE
    );
    addRequestClass(Type.CreateKey,
        OMKeyCreateRequestWithFSO.class,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    // AllocateBlock
    addRequestClass(Type.AllocateBlock,
        OMAllocateBlockRequest.class,
        BucketLayout.OBJECT_STORE
    );
    addRequestClass(Type.AllocateBlock,
        OMAllocateBlockRequestWithFSO.class,
        BucketLayout.FILE_SYSTEM_OPTIMIZED
    );

    // CommitKey
    addRequestClass(Type.CommitKey,
        OMKeyCommitRequest.class,
        BucketLayout.OBJECT_STORE
    );
    addRequestClass(Type.CommitKey,
        OMKeyCommitRequestWithFSO.class,
        BucketLayout.FILE_SYSTEM_OPTIMIZED
    );

    // DeleteKey
    addRequestClass(Type.DeleteKey,
        OMKeyDeleteRequest.class,
        BucketLayout.OBJECT_STORE);
    addRequestClass(Type.DeleteKey,
        OMKeyDeleteRequestWithFSO.class,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    // DeleteKeys
    addRequestClass(Type.DeleteKeys,
        OMKeysDeleteRequest.class,
        BucketLayout.OBJECT_STORE);

    // RenameKey
    addRequestClass(Type.RenameKey,
        OMKeyRenameRequest.class,
        BucketLayout.OBJECT_STORE);
    addRequestClass(Type.RenameKey,
        OMKeyRenameRequestWithFSO.class,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    // RenameKeys
    addRequestClass(Type.RenameKeys,
        OMKeysRenameRequest.class,
        BucketLayout.OBJECT_STORE);

    // PurgeKeys
    addRequestClass(Type.PurgeKeys,
        OMKeyPurgeRequest.class,
        BucketLayout.OBJECT_STORE);

    // PurgePaths
    addRequestClass(Type.PurgePaths,
        OMPathsPurgeRequestWithFSO.class,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    // InitiateMultiPartUpload
    addRequestClass(Type.InitiateMultiPartUpload,
        S3InitiateMultipartUploadRequest.class,
        BucketLayout.OBJECT_STORE);
    addRequestClass(Type.InitiateMultiPartUpload,
        S3InitiateMultipartUploadRequestWithFSO.class,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    // CommitMultiPartUpload
    addRequestClass(Type.CommitMultiPartUpload,
        S3MultipartUploadCommitPartRequest.class,
        BucketLayout.OBJECT_STORE);
    addRequestClass(Type.CommitMultiPartUpload,
        S3MultipartUploadCommitPartRequestWithFSO.class,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    // AbortMultiPartUpload
    addRequestClass(Type.AbortMultiPartUpload,
        S3MultipartUploadAbortRequest.class,
        BucketLayout.OBJECT_STORE);
    addRequestClass(Type.AbortMultiPartUpload,
        S3MultipartUploadAbortRequestWithFSO.class,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    // CompleteMultiPartUpload
    addRequestClass(Type.CompleteMultiPartUpload,
        S3MultipartUploadCompleteRequest.class,
        BucketLayout.OBJECT_STORE);
    addRequestClass(Type.CompleteMultiPartUpload,
        S3MultipartUploadCompleteRequestWithFSO.class,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
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
   * @param ozoneManager ozone manager instance
   * @return OMKeyRequest object
   * @throws IOException if the request type is not supported.
   */
  public static OMKeyRequest createRequest(String volumeName, String bucketName,
                                           OMRequest omRequest,
                                           OzoneManager ozoneManager)

      throws IOException {
    // Get the bucket layout of the bucket being accessed by this request.
    // While doing this we make sure we are resolving the real bucket in case of
    // link buckets.
    BucketLayout bucketLayout =
        getBucketLayout(volumeName, bucketName, ozoneManager, new HashSet<>());
    // Instantiate the request class based on the bucket layout.
    return getRequestInstance(omRequest, bucketLayout);

  }

  /**
   * Helper method to add a request class to the omKeyReqClasses map.
   *
   * @param requestType            type of the request
   * @param requestClass           Request class to be added.
   * @param associatedBucketLayout BucketLayout the request class is associated
   *                               with.
   */
  private static void addRequestClass(Type requestType,
                                      Class<? extends OMKeyRequest>
                                          requestClass,
                                      BucketLayout associatedBucketLayout) {
    // If the request class is associated to FSO bucket layout,
    // we add a suffix to its key in the map.
    OM_KEY_REQUEST_CLASSES.put(getKey(requestType, associatedBucketLayout),
        requestClass);
  }

  /**
   * Helper method to instantiate the specific request class based on the
   * bucket layout associated with the request.
   *
   * @param omRequest    OMRequest which is to be instantiated.
   * @param bucketLayout BucketLayout associated with the request.
   * @return OMKeyRequest instance.
   * @throws OMException if the request type is not supported with the
   *                     associated bucket layout.
   */
  private static OMKeyRequest getRequestInstance(OMRequest omRequest,
                                                 BucketLayout bucketLayout)
      throws OMException {
    Type requestType = omRequest.getCmdType();

    // If the request class is associated to FSO bucket layout,
    // we add a suffix to its key in the map.
    String classKey = getKey(requestType, bucketLayout);

    // Check if the key is present in the map.
    if (OM_KEY_REQUEST_CLASSES.containsKey(classKey)) {
      try {
        // Get the constructor of the request class.
        Constructor<? extends OMKeyRequest> declaredConstructor =
            OM_KEY_REQUEST_CLASSES.get(classKey)
                .getDeclaredConstructor(OMRequest.class, BucketLayout.class);

        // Invoke the constructor.
        return declaredConstructor.newInstance(omRequest, bucketLayout);
      } catch (NoSuchMethodException | InvocationTargetException |
          InstantiationException | IllegalAccessException e) {
        String errMsg = "Exception while creating OMKeyRequest of type " +
            requestType + " for bucket layout " + bucketLayout;
        LOG.error(errMsg, e);
        throw new OMException(errMsg,
            OMException.ResultCodes.INTERNAL_ERROR);
      }
    }

    // We did not find this key in the map, it means this request type is not
    // supported.
    throw new OMException(
        "Request type " + requestType + " not supported with bucket layout " +
            bucketLayout, OMException.ResultCodes.INTERNAL_ERROR);
  }

  /**
   * Generates a key name for a request type and bucket layout.
   *
   * @param requestType  type of the request
   * @param bucketLayout Flavor of the request types based on the bucket
   *                     layout.
   * @return key name for the request type.
   */
  private static String getKey(Type requestType, BucketLayout bucketLayout) {
    return requestType.toString() +
        (bucketLayout.isFileSystemOptimized() ? bucketLayout : "");
  }
}
