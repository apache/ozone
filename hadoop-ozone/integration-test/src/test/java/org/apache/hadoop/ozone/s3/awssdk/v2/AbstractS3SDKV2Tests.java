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

package org.apache.hadoop.ozone.s3.awssdk.v2;

import static org.apache.hadoop.ozone.OzoneConsts.MB;
import static org.apache.hadoop.ozone.s3.awssdk.S3SDKTestUtils.calculateDigest;
import static org.apache.hadoop.ozone.s3.awssdk.S3SDKTestUtils.createFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.xml.bind.DatatypeConverter;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.s3.S3ClientFactory;
import org.apache.hadoop.ozone.s3.S3GatewayService;
import org.apache.hadoop.ozone.s3.awssdk.S3SDKTestUtils;
import org.apache.ozone.test.OzoneTestBase;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.CompleteMultipartUploadPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.CreateMultipartUploadPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedCompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedCreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedUploadPartRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.UploadPartPresignRequest;
import software.amazon.awssdk.utils.IoUtils;

/**
 * This is an abstract class to test the AWS Java S3 SDK operations.
 * This class should be extended for OM standalone and OM HA (Ratis) cluster setup.
 *
 * The test scenarios are adapted from
 * - https://github.com/awsdocs/aws-doc-sdk-examples/tree/main/javav2/example_code/s3/src/main/java/com/example/s3
 * - https://github.com/ceph/s3-tests
 *
 * TODO: Add tests to different types of S3 client (Async client, CRT-based client)
 *  See:
 *   - https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/examples-s3.html
 *   - https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/asynchronous.html
 *   - https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/crt-based-s3-client.html
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
public abstract class AbstractS3SDKV2Tests extends OzoneTestBase {

  private static MiniOzoneCluster cluster = null;
  private static S3Client s3Client = null;

  /**
   * Create a MiniOzoneCluster with S3G enabled for testing.
   * @param conf Configurations to start the cluster
   * @throws Exception exception thrown when waiting for the cluster to be ready.
   */
  static void startCluster(OzoneConfiguration conf) throws Exception {
    S3GatewayService s3g = new S3GatewayService();
    cluster = MiniOzoneCluster.newBuilder(conf)
        .addService(s3g)
        .setNumDatanodes(5)
        .build();
    cluster.waitForClusterToBeReady();
    s3Client = new S3ClientFactory(s3g.getConf()).createS3ClientV2();
  }

  /**
   * Shutdown the MiniOzoneCluster.
   */
  static void shutdownCluster() throws IOException {
    if (s3Client != null) {
      s3Client.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testPutObject() {
    final String bucketName = getBucketName();
    final String keyName = getKeyName();
    final String content = "bar";
    s3Client.createBucket(b -> b.bucket(bucketName));

    PutObjectResponse putObjectResponse = s3Client.putObject(b -> b
            .bucket(bucketName)
            .key(keyName),
        RequestBody.fromString(content));

    assertEquals("\"37b51d194a7513e45b56f6524f2d51f2\"", putObjectResponse.eTag());

    ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(
        b -> b.bucket(bucketName).key(keyName)
    );
    GetObjectResponse getObjectResponse = objectBytes.response();

    assertEquals(content, objectBytes.asUtf8String());
    assertEquals("\"37b51d194a7513e45b56f6524f2d51f2\"", getObjectResponse.eTag());
  }

  @Test
  public void testCopyObject() {
    final String sourceBucketName = getBucketName("source");
    final String destBucketName = getBucketName("dest");
    final String sourceKey = getKeyName("source");
    final String destKey = getKeyName("dest");
    final String content = "bar";
    s3Client.createBucket(b -> b.bucket(sourceBucketName));
    s3Client.createBucket(b -> b.bucket(destBucketName));

    PutObjectResponse putObjectResponse = s3Client.putObject(b -> b
            .bucket(sourceBucketName)
            .key(sourceKey),
        RequestBody.fromString(content));

    assertEquals("\"37b51d194a7513e45b56f6524f2d51f2\"", putObjectResponse.eTag());

    CopyObjectRequest copyReq = CopyObjectRequest.builder()
        .sourceBucket(sourceBucketName)
        .sourceKey(sourceKey)
        .destinationBucket(destBucketName)
        .destinationKey(destKey)
        .build();

    CopyObjectResponse copyObjectResponse = s3Client.copyObject(copyReq);
    assertEquals("\"37b51d194a7513e45b56f6524f2d51f2\"", copyObjectResponse.copyObjectResult().eTag());
  }

  @Test
  public void testLowLevelMultipartUpload(@TempDir Path tempDir) throws Exception {
    final String bucketName = getBucketName();
    final String keyName = getKeyName();
    final Map<String, String> userMetadata = new HashMap<>();
    userMetadata.put("key1", "value1");
    userMetadata.put("key2", "value2");

    List<Tag> tags = Arrays.asList(
        Tag.builder().key("tag1").value("value1").build(),
        Tag.builder().key("tag2").value("value2").build()
    );

    s3Client.createBucket(b -> b.bucket(bucketName));

    File multipartUploadFile = Files.createFile(tempDir.resolve("multipartupload.txt")).toFile();

    createFile(multipartUploadFile, (int) (25 * MB));

    multipartUpload(bucketName, keyName, multipartUploadFile, (int) (5 * MB), userMetadata, tags);

    ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(
        b -> b.bucket(bucketName).key(keyName)
    );

    GetObjectResponse getObjectResponse = objectBytes.response();

    assertEquals(tags.size(), getObjectResponse.tagCount());

    HeadObjectResponse headObjectResponse = s3Client.headObject(b -> b.bucket(bucketName).key(keyName));
    assertTrue(headObjectResponse.hasMetadata());
    assertEquals(userMetadata, headObjectResponse.metadata());
  }

  @Test
  public void testPresignedUrlGet() throws Exception {
    final String bucketName = getBucketName();
    final String keyName = getKeyName();
    final String content = "bar";
    s3Client.createBucket(b -> b.bucket(bucketName));

    s3Client.putObject(b -> b
            .bucket(bucketName)
            .key(keyName),
        RequestBody.fromString(content));

    try (S3Presigner presigner = createS3Presigner()) {
      GetObjectRequest objectRequest = GetObjectRequest.builder()
          .bucket(bucketName)
          .key(keyName)
          .build();

      GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
          .signatureDuration(Duration.ofMinutes(10))  // The URL will expire in 10 minutes.
          .getObjectRequest(objectRequest)
          .build();

      PresignedGetObjectRequest presignedRequest = presigner.presignGetObject(presignRequest);

      // Download the object using HttpUrlConnection (since v1.1)
      // Capture the response body to a byte array.
      URL presignedUrl = presignedRequest.url();
      HttpURLConnection connection = (HttpURLConnection) presignedUrl.openConnection();
      connection.setRequestMethod("GET");
      // Download the result of executing the request.
      try (InputStream s3is = connection.getInputStream();
           ByteArrayOutputStream bos = new ByteArrayOutputStream(
               content.getBytes(StandardCharsets.UTF_8).length)) {
        IoUtils.copy(s3is, bos);
        assertEquals(content, bos.toString("UTF-8"));
      }

      // Use the AWS SDK for Java SdkHttpClient class to do the download
      SdkHttpRequest request = SdkHttpRequest.builder()
          .method(SdkHttpMethod.GET)
          .uri(presignedUrl.toURI())
          .build();

      HttpExecuteRequest executeRequest = HttpExecuteRequest.builder()
          .request(request)
          .build();

      try (SdkHttpClient sdkHttpClient = ApacheHttpClient.create();
           ByteArrayOutputStream bos = new ByteArrayOutputStream(
               content.getBytes(StandardCharsets.UTF_8).length)) {
        HttpExecuteResponse response = sdkHttpClient.prepareRequest(executeRequest).call();
        assertTrue(response.responseBody().isPresent(), () -> "The presigned url download request " +
            "should have a response body");
        response.responseBody().ifPresent(
            abortableInputStream -> {
              try {
                IoUtils.copy(abortableInputStream, bos);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
        assertEquals(content, bos.toString("UTF-8"));
      }
    }
  }

  @Test
  public void testPresignedUrlPut() throws Exception {
    final String bucketName = getBucketName();
    final String keyName = getKeyName();
    s3Client.createBucket(b -> b.bucket(bucketName));

    try (S3Presigner presigner = createS3Presigner()) {

      PutObjectRequest objectRequest = PutObjectRequest.builder()
          .bucket(bucketName)
          .key(keyName)
          .build();

      PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
          .signatureDuration(Duration.ofMinutes(10))
          .putObjectRequest(objectRequest)
          .build();

      PresignedPutObjectRequest presignedRequest = presigner.presignPutObject(presignRequest);

      // use http url connection
      HttpURLConnection connection = null;
      String expectedContent;
      String actualContent;
      try {
        expectedContent = "This is a test content for presigned PUT URL.";
        connection = (HttpURLConnection) presignedRequest.url().openConnection();
        connection.setRequestMethod("PUT");
        connection.setDoOutput(true);
        try (OutputStream os = connection.getOutputStream()) {
          os.write(expectedContent.getBytes(StandardCharsets.UTF_8));
          os.flush();
        }

        int responseCode = connection.getResponseCode();
        assertEquals(200, responseCode, "PutObject presigned URL should return 200 OK");

        //verify the object was uploaded
        ResponseInputStream<GetObjectResponse> object1 = s3Client.getObject(b1 -> b1.bucket(bucketName).key(keyName));
        actualContent = IoUtils.toUtf8String(object1);
        assertEquals(expectedContent, actualContent);

        // Use the AWS SDK for Java SdkHttpClient class to test the PUT request
        expectedContent = "This content is for testing the SdkHttpClient PUT request.";
        SdkHttpRequest request = SdkHttpRequest.builder()
            .method(SdkHttpMethod.PUT)
            .uri(presignedRequest.url().toURI())
            .build();

        byte[] bytes = expectedContent.getBytes(StandardCharsets.UTF_8);
        HttpExecuteRequest executeRequest = HttpExecuteRequest.builder()
            .request(request)
            .contentStreamProvider(() -> new ByteArrayInputStream(bytes))
            .build();

        try (SdkHttpClient sdkHttpClient = ApacheHttpClient.create()) {
          HttpExecuteResponse response = sdkHttpClient.prepareRequest(executeRequest).call();
          assertEquals(200, response.httpResponse().statusCode(),
              "PutObject presigned URL should return 200 OK via SdkHttpClient");
        }

        //verify the object was uploaded
        ResponseInputStream<GetObjectResponse> object = s3Client.getObject(b -> b.bucket(bucketName).key(keyName));
        actualContent = IoUtils.toUtf8String(object);
        assertEquals(expectedContent, actualContent);
      } finally {
        if (connection != null) {
          connection.disconnect();
        }
      }
    }
  }

  @Test
  public void testPresignedUrlMultipartUpload(@TempDir Path tempDir) throws Exception {
    final String bucketName = getBucketName();
    final String keyName = getKeyName();
    final Map<String, String> userMetadata = new HashMap<>();
    userMetadata.put("key1", "value1");
    userMetadata.put("key2", "value2");

    List<Tag> tags = Arrays.asList(
        Tag.builder().key("tag1").value("value1").build(),
        Tag.builder().key("tag2").value("value2").build()
    );

    s3Client.createBucket(b -> b.bucket(bucketName));

    File multipartUploadFile = Files.createFile(tempDir.resolve("multipartupload.txt")).toFile();
    createFile(multipartUploadFile, (int) (10 * MB));

    try (S3Presigner presigner = createS3Presigner()) {

      // generate create MPU presigned URL
      CreateMultipartUploadRequest createRequest = CreateMultipartUploadRequest.builder()
          .bucket(bucketName)
          .key(keyName)
          .metadata(userMetadata)
          .tagging(Tagging.builder().tagSet(tags).build())
          .build();

      CreateMultipartUploadPresignRequest createMPUPresignRequest = CreateMultipartUploadPresignRequest.builder()
          .signatureDuration(Duration.ofMinutes(10))
          .createMultipartUploadRequest(createRequest)
          .build();

      PresignedCreateMultipartUploadRequest presignCreateMultipartUpload =
          presigner.presignCreateMultipartUpload(createMPUPresignRequest);

      mpuWithHttpURLConnection(presignCreateMultipartUpload, multipartUploadFile, bucketName, keyName, presigner,
          userMetadata, tags);
      mpuWithSdkHttpClient(presignCreateMultipartUpload, multipartUploadFile, bucketName, keyName, presigner,
          userMetadata, tags);
    }
  }

  private void mpuWithHttpURLConnection(PresignedCreateMultipartUploadRequest presignCreateMultipartUpload,
      File multipartUploadFile, String bucketName, String keyName,
      S3Presigner presigner, Map<String, String> userMetadata,
      List<Tag> tags) throws IOException {
    // create MPU using presigned URL
    String uploadId;
    HttpURLConnection createMultiPartUploadConnection = null;
    try {
      createMultiPartUploadConnection = openHttpUrlConnection(presignCreateMultipartUpload);
      int createMultiPartUploadConnectionResponseCode = createMultiPartUploadConnection.getResponseCode();
      assertEquals(200, createMultiPartUploadConnectionResponseCode,
          "CreateMultipartUploadPresignRequest should return 200 OK");

      try (InputStream is = createMultiPartUploadConnection.getInputStream()) {
        String responseXml = IOUtils.toString(is, StandardCharsets.UTF_8);
        uploadId = S3SDKTestUtils.extractUploadId(responseXml);
      }
    } finally {
      if (createMultiPartUploadConnection != null) {
        createMultiPartUploadConnection.disconnect();
      }
    }

    // Upload parts using presigned URL
    List<CompletedPart> completedParts =
        uploadPartWithHttpURLConnection(multipartUploadFile, bucketName, keyName, presigner, uploadId);

    // complete MPU using presigned URL
    CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
        .bucket(bucketName)
        .key(keyName)
        .uploadId(uploadId)
        .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
        .build();
    CompleteMultipartUploadPresignRequest completeMultipartUploadPresignRequest =
        CompleteMultipartUploadPresignRequest.builder()
            .signatureDuration(Duration.ofMinutes(10))
            .completeMultipartUploadRequest(completeRequest)
            .build();

    PresignedCompleteMultipartUploadRequest presignedCompleteMultipartUploadRequest =
        presigner.presignCompleteMultipartUpload(completeMultipartUploadPresignRequest);

    completeMPUWithHttpUrlConnection(presignedCompleteMultipartUploadRequest, completedParts);

    // verify upload result
    HeadObjectResponse headObjectResponse = s3Client.headObject(b -> b.bucket(bucketName).key(keyName));
    assertTrue(headObjectResponse.hasMetadata());
    assertEquals(userMetadata, headObjectResponse.metadata());

    ResponseInputStream<GetObjectResponse> object = s3Client.getObject(b -> b.bucket(bucketName).key(keyName));
    assertEquals(tags.size(), object.response().tagCount());
    String actualContent = IoUtils.toUtf8String(object);
    String originalContent = new String(Files.readAllBytes(multipartUploadFile.toPath()), StandardCharsets.UTF_8);
    assertEquals(originalContent, actualContent, "Uploaded file content should match original file content");
  }

  private List<CompletedPart> uploadPartWithHttpURLConnection(File multipartUploadFile, String bucketName,
      String keyName, S3Presigner presigner, String uploadId)
      throws IOException {
    List<CompletedPart> completedParts = new ArrayList<>();
    int partNumber = 1;
    ByteBuffer bb = ByteBuffer.allocate((int) (5 * MB));

    try (RandomAccessFile file = new RandomAccessFile(multipartUploadFile, "r")) {
      long fileSize = file.length();
      long position = 0;

      while (position < fileSize) {
        file.seek(position);
        long read = file.getChannel().read(bb);

        bb.flip();

        // First create an UploadPartRequest
        UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
            .bucket(bucketName)
            .key(keyName)
            .uploadId(uploadId)
            .partNumber(partNumber)
            .contentLength((long) bb.remaining())
            .build();

        // Generate presigned URL for each part
        UploadPartPresignRequest presignRequest = UploadPartPresignRequest.builder()
            .signatureDuration(Duration.ofMinutes(10))
            .uploadPartRequest(uploadPartRequest)
            .build();

        PresignedUploadPartRequest presignedRequest = presigner.presignUploadPart(presignRequest);

        // use presigned URL to upload the part
        HttpURLConnection connection = null;
        try {
          connection = (HttpURLConnection) presignedRequest.url().openConnection();
          connection.setDoOutput(true);
          connection.setRequestMethod("PUT");

          try (OutputStream os = connection.getOutputStream()) {
            os.write(bb.array(), 0, bb.remaining());
            os.flush();
          }

          int responseCode = connection.getResponseCode();
          assertEquals(200, responseCode,
              String.format("Upload part %d should return 200 OK", partNumber));

          String etag = connection.getHeaderField("ETag");
          CompletedPart part = CompletedPart.builder()
              .partNumber(partNumber)
              .eTag(etag)
              .build();
          completedParts.add(part);
        } finally {
          if (connection != null) {
            connection.disconnect();
          }
        }

        bb.clear();
        position += read;
        partNumber++;
      }
    }
    return completedParts;
  }

  private HttpURLConnection openHttpUrlConnection(PresignedCreateMultipartUploadRequest request) throws IOException {
    HttpURLConnection connection = (HttpURLConnection) request.url().openConnection();
    connection.setDoOutput(true);
    connection.setRequestMethod("POST");
    for (Map.Entry<String, List<String>> entry : request.signedHeaders().entrySet()) {
      String key = entry.getKey();
      for (String value : entry.getValue()) {
        connection.setRequestProperty(key, value);
      }
    }
    return connection;
  }

  private void completeMPUWithHttpUrlConnection(PresignedCompleteMultipartUploadRequest request,
      List<CompletedPart> completedParts) throws IOException {
    HttpURLConnection completeMultipartUploadConnection = null;
    try {
      completeMultipartUploadConnection = (HttpURLConnection) request.url().openConnection();
      completeMultipartUploadConnection.setDoOutput(true);
      completeMultipartUploadConnection.setRequestMethod("POST");

      // copy headers
      for (Map.Entry<String, List<String>> entry : request.signedHeaders().entrySet()) {
        String key = entry.getKey();
        for (String value : entry.getValue()) {
          completeMultipartUploadConnection.setRequestProperty(key, value);
        }
      }

      String xmlPayload = buildCompleteMultipartUploadXml(completedParts);
      byte[] payloadBytes = xmlPayload.getBytes(StandardCharsets.UTF_8);
      try (OutputStream os = completeMultipartUploadConnection.getOutputStream()) {
        IOUtils.write(payloadBytes, os);
        os.flush();
      }

      int completeMultipartUploadConnectionResponseCode = completeMultipartUploadConnection.getResponseCode();
      assertEquals(200, completeMultipartUploadConnectionResponseCode,
          "CompleteMultipartUploadPresignRequest should return 200 OK");
    } finally {
      if (completeMultipartUploadConnection != null) {
        completeMultipartUploadConnection.disconnect();
      }
    }
  }

  private void mpuWithSdkHttpClient(PresignedCreateMultipartUploadRequest presignCreateMultipartUpload,
      File multipartUploadFile, String bucketName, String keyName,
      S3Presigner presigner, Map<String, String> userMetadata, List<Tag> tags)
      throws Exception {
    // create MPU using presigned URL
    String uploadId;
    try (SdkHttpClient sdkHttpClient = ApacheHttpClient.create()) {
      uploadId = createMPUWithSdkHttpClient(presignCreateMultipartUpload, sdkHttpClient);

      // Upload parts using presigned URL
      List<CompletedPart> completedParts =
          uploadPartWithSdkHttpClient(multipartUploadFile, bucketName, keyName, presigner, uploadId, sdkHttpClient);

      // complete MPU using presigned URL
      completeMPUWithSdkHttpClient(bucketName, keyName, presigner, uploadId, completedParts, sdkHttpClient);

      // verify upload result
      HeadObjectResponse headObjectResponse = s3Client.headObject(b -> b.bucket(bucketName).key(keyName));
      assertTrue(headObjectResponse.hasMetadata());
      assertEquals(userMetadata, headObjectResponse.metadata());

      ResponseInputStream<GetObjectResponse> object = s3Client.getObject(b -> b.bucket(bucketName).key(keyName));
      assertEquals(tags.size(), object.response().tagCount());
      String actualContent = IoUtils.toUtf8String(object);
      String originalContent = new String(Files.readAllBytes(multipartUploadFile.toPath()), StandardCharsets.UTF_8);
      assertEquals(originalContent, actualContent, "Uploaded file content should match original file content");
    }
  }

  private void completeMPUWithSdkHttpClient(String bucketName, String keyName, S3Presigner presigner, String uploadId,
      List<CompletedPart> completedParts, SdkHttpClient sdkHttpClient) throws Exception {
    CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
        .bucket(bucketName)
        .key(keyName)
        .uploadId(uploadId)
        .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
        .build();

    CompleteMultipartUploadPresignRequest completeMultipartUploadPresignRequest =
        CompleteMultipartUploadPresignRequest.builder()
            .signatureDuration(Duration.ofMinutes(10))
            .completeMultipartUploadRequest(completeRequest)
            .build();

    PresignedCompleteMultipartUploadRequest presignedCompleteMultipartUploadRequest =
        presigner.presignCompleteMultipartUpload(completeMultipartUploadPresignRequest);

    String xmlPayload = buildCompleteMultipartUploadXml(completedParts);
    byte[] payloadBytes = xmlPayload.getBytes(StandardCharsets.UTF_8);

    SdkHttpRequest completeMultipartUploadRequest = SdkHttpRequest.builder()
        .method(SdkHttpMethod.POST)
        .uri(presignedCompleteMultipartUploadRequest.url().toURI())
        .appendHeader("content-type", "application/xml")
        .build();

    HttpExecuteRequest completeMultipartUploadExecuteRequest = HttpExecuteRequest.builder()
        .request(completeMultipartUploadRequest)
        .contentStreamProvider(() -> new ByteArrayInputStream(payloadBytes))
        .build();

    HttpExecuteResponse completeMultipartUploadResponse =
        sdkHttpClient.prepareRequest(completeMultipartUploadExecuteRequest).call();
    assertEquals(200, completeMultipartUploadResponse.httpResponse().statusCode(),
        "CompleteMultipartUploadPresignRequest should return 200 OK");
  }

  private List<CompletedPart> uploadPartWithSdkHttpClient(File multipartUploadFile, String bucketName, String keyName,
      S3Presigner presigner, String uploadId,
      SdkHttpClient httpClient) throws Exception {
    List<CompletedPart> completedParts = new ArrayList<>();
    int partNumber = 1;
    ByteBuffer bb = ByteBuffer.allocate((int) (5 * MB));

    try (RandomAccessFile file = new RandomAccessFile(multipartUploadFile, "r")) {
      long fileSize = file.length();
      long position = 0;

      while (position < fileSize) {
        file.seek(position);
        long read = file.getChannel().read(bb);

        bb.flip();

        // Generate presigned URL for each part
        UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
            .bucket(bucketName)
            .key(keyName)
            .uploadId(uploadId)
            .partNumber(partNumber)
            .contentLength((long) bb.remaining())
            .build();

        UploadPartPresignRequest presignRequest = UploadPartPresignRequest.builder()
            .signatureDuration(Duration.ofMinutes(10))
            .uploadPartRequest(uploadPartRequest)
            .build();

        PresignedUploadPartRequest presignedRequest = presigner.presignUploadPart(presignRequest);

        // upload each part using presigned URL
        SdkHttpRequest uploadPartSdkRequest = SdkHttpRequest.builder()
            .method(SdkHttpMethod.PUT)
            .uri(presignedRequest.url().toURI())
            .build();

        byte[] bytes = new byte[bb.remaining()];
        bb.get(bytes);

        HttpExecuteRequest uploadPartExecuteRequest = HttpExecuteRequest.builder()
            .request(uploadPartSdkRequest)
            .contentStreamProvider(() -> new ByteArrayInputStream(bytes))
            .build();

        HttpExecuteResponse uploadPartResponse = httpClient.prepareRequest(uploadPartExecuteRequest).call();

        String etag = uploadPartResponse.httpResponse()
            .firstMatchingHeader("ETag")
            .orElseThrow(() -> new RuntimeException("ETag missing in response"));

        CompletedPart part = CompletedPart.builder()
            .partNumber(partNumber)
            .eTag(etag)
            .build();
        completedParts.add(part);

        bb.clear();
        position += read;
        partNumber++;
      }
    }
    return completedParts;
  }

  private String createMPUWithSdkHttpClient(PresignedCreateMultipartUploadRequest request,
      SdkHttpClient httpClient) throws Exception {
    String uploadId;
    SdkHttpRequest createMultipartUploadRequest = SdkHttpRequest.builder()
        .method(SdkHttpMethod.POST)
        .uri(request.url().toURI())
        .headers(request.signedHeaders())
        .build();

    HttpExecuteRequest createMultipartUploadExecuteRequest = HttpExecuteRequest.builder()
        .request(createMultipartUploadRequest)
        .build();

    HttpExecuteResponse createMultipartUploadResponse =
        httpClient.prepareRequest(createMultipartUploadExecuteRequest).call();

    try (InputStream is = createMultipartUploadResponse.responseBody().get()) {
      String responseXml = IOUtils.toString(is, StandardCharsets.UTF_8);
      uploadId = S3SDKTestUtils.extractUploadId(responseXml);
    }
    return uploadId;
  }

  private String buildCompleteMultipartUploadXml(List<CompletedPart> parts) {
    StringBuilder xml = new StringBuilder();
    xml.append("<CompleteMultipartUpload>\n");
    for (CompletedPart part : parts) {
      xml.append("  <Part>\n");
      xml.append("    <PartNumber>").append(part.partNumber()).append("</PartNumber>\n");
      xml.append("    <ETag>").append(part.eTag()).append("</ETag>\n");
      xml.append("  </Part>\n");
    }
    xml.append("</CompleteMultipartUpload>");
    return xml.toString();
  }

  private S3Presigner createS3Presigner() {
    return S3Presigner.builder()
        // TODO: Find a way to retrieve the path style configuration from S3Client instead
        .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
        .endpointOverride(s3Client.serviceClientConfiguration().endpointOverride().get())
        .region(s3Client.serviceClientConfiguration().region())
        .credentialsProvider(s3Client.serviceClientConfiguration().credentialsProvider()).build();
  }

  private String getBucketName() {
    return getBucketName(null);
  }

  private String getBucketName(String suffix) {
    return (getTestName() + "bucket" + suffix).toLowerCase(Locale.ROOT);
  }

  private String getKeyName() {
    return getKeyName(null);
  }

  private String getKeyName(String suffix) {
    return (getTestName() +  "key" + suffix).toLowerCase(Locale.ROOT);
  }

  private String multipartUpload(String bucketName, String key, File file, int partSize,
                                 Map<String, String> userMetadata, List<Tag> tags) throws Exception {
    String uploadId = initiateMultipartUpload(bucketName, key, userMetadata, tags);

    List<CompletedPart> completedParts = uploadParts(bucketName, key, uploadId, file, partSize);

    completeMultipartUpload(bucketName, key, uploadId, completedParts);

    return uploadId;
  }

  private String initiateMultipartUpload(String bucketName, String key,
                                         Map<String, String> metadata, List<Tag> tags) {
    CreateMultipartUploadResponse createMultipartUploadResponse = s3Client.createMultipartUpload(b -> b
        .bucket(bucketName)
        .metadata(metadata)
        .tagging(Tagging.builder().tagSet(tags).build())
        .key(key));

    assertEquals(bucketName, createMultipartUploadResponse.bucket());
    assertEquals(key, createMultipartUploadResponse.key());
    // TODO: Once bucket lifecycle configuration is supported, should check for "abortDate" and "abortRuleId"

    return createMultipartUploadResponse.uploadId();
  }

  private List<CompletedPart> uploadParts(String bucketName, String key, String uploadId, File inputFile, int partSize)
      throws Exception {
    // Upload the parts of the file
    int partNumber = 1;
    ByteBuffer bb = ByteBuffer.allocate(partSize);
    List<CompletedPart> completedParts = new ArrayList<>();
    try (RandomAccessFile file = new RandomAccessFile(inputFile, "r");
         InputStream fileInputStream = Files.newInputStream(inputFile.toPath())) {
      long fileSize = file.length();
      long position = 0;
      while (position < fileSize) {
        file.seek(position);
        long read = file.getChannel().read(bb);

        bb.flip(); // Swap position and limit before reading from the buffer
        UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
            .bucket(bucketName)
            .key(key)
            .uploadId(uploadId)
            .partNumber(partNumber)
            .build();

        UploadPartResponse partResponse = s3Client.uploadPart(
            uploadPartRequest,
            RequestBody.fromByteBuffer(bb));

        assertEquals(DatatypeConverter.printHexBinary(
            calculateDigest(fileInputStream, 0, partSize)).toLowerCase(), partResponse.eTag());

        CompletedPart part = CompletedPart.builder()
            .partNumber(partNumber)
            .eTag(partResponse.eTag())
            .build();
        completedParts.add(part);

        bb.clear();
        position += read;
        partNumber++;
      }
    }

    return completedParts;
  }

  private void completeMultipartUpload(String bucketName, String key, String uploadId,
                                       List<CompletedPart> completedParts) {
    CompleteMultipartUploadResponse compResponse = s3Client.completeMultipartUpload(b -> b
        .bucket(bucketName)
        .key(key)
        .uploadId(uploadId)
        .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build()));
    assertEquals(bucketName, compResponse.bucket());
    assertEquals(key, compResponse.key());
  }
}
