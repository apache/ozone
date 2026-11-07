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

package org.apache.hadoop.ozone.s3.awssdk;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig.ConfigStrings.HDDS_SCM_HTTP_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig.ConfigStrings.HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_KERBEROS_KEYTAB_FILE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.s3.S3ClientFactory.ACCESS_KEY;
import static org.apache.hadoop.ozone.s3.S3ClientFactory.SECRET_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.BAD_DIGEST;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.SIGNATURE_DOES_NOT_MATCH;
import static org.apache.hadoop.ozone.s3.util.S3Consts.DECODED_CONTENT_LENGTH_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STREAMING_AWS4_HMAC_SHA256_PAYLOAD;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STREAMING_AWS4_HMAC_SHA256_PAYLOAD_TRAILER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_TRAILER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.s3.S3ClientFactory;
import org.apache.hadoop.ozone.s3.S3GatewayService;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.ClusterForTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.Part;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

/** Official SDK signed uploads through a Kerberos-secured gateway, OM and datanodes. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractS3SignedChunksInSecureCluster extends ClusterForTests<MiniOzoneCluster> {

  private static final String BUCKET = "signed-chunks";
  private static final String CRC32C_TRAILER = "x-amz-checksum-crc32c";
  private static final String WRONG_CHECKSUM = "AAAAAA==";
  private static final byte[] DATA = new byte[128 * 1024 + 1024];

  static {
    new Random(0).nextBytes(DATA);
  }

  private MiniKdc kdc;
  private OzoneConfiguration conf;
  private S3GatewayService gateway;
  private S3ClientFactory factory;
  private S3Client client;

  @BeforeAll
  void setup(@TempDir Path tempDir) throws Exception {
    kdc = new MiniKdc(MiniKdc.createConf(), tempDir.toFile());
    kdc.start();
    conf = createBaseConfiguration();
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    conf.set(OZONE_ADMINISTRATORS, "scm");
    String host = InetAddress.getLocalHost().getCanonicalHostName().toLowerCase(Locale.ROOT);
    String principal = "scm/" + host + "@" + kdc.getRealm();
    String httpPrincipal = "HTTP/" + host + "@" + kdc.getRealm();
    File keytab = tempDir.resolve("services.keytab").toFile();
    kdc.createPrincipal(keytab, principal, httpPrincipal);
    conf.set(HDDS_SCM_KERBEROS_PRINCIPAL_KEY, principal);
    conf.set(OZONE_OM_KERBEROS_PRINCIPAL_KEY, principal);
    conf.set(HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY, principal);
    conf.set(OZONE_S3G_KERBEROS_PRINCIPAL_KEY, principal);
    conf.set(HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY, httpPrincipal);
    conf.set(OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY, httpPrincipal);
    conf.set(HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY, keytab.getAbsolutePath());
    conf.set(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY, keytab.getAbsolutePath());
    conf.set(HDDS_DATANODE_KERBEROS_KEYTAB_FILE_KEY, keytab.getAbsolutePath());
    conf.set(OZONE_S3G_KERBEROS_KEYTAB_FILE_KEY, keytab.getAbsolutePath());
    conf.set(HDDS_SCM_HTTP_KERBEROS_KEYTAB_FILE_KEY, keytab.getAbsolutePath());
    conf.set(OZONE_OM_HTTP_KERBEROS_KEYTAB_FILE, keytab.getAbsolutePath());
    startCluster();
  }

  @Override
  protected OzoneConfiguration createOzoneConfig() {
    return conf;
  }

  @Override
  protected MiniOzoneCluster createCluster() throws Exception {
    // The HA builder bootstraps the SCM CA, including for a single SCM.
    return MiniOzoneCluster.newHABuilder(createOzoneConfig())
        .setSCMServiceId("signed-chunks-scm")
        .setNumDatanodes(3)
        .build();
  }

  @Override
  protected void onClusterReady() throws Exception {
    try (OzoneClient ozone = getCluster().newClient()) {
      // setS3Secret only replaces the secret of an access ID that already exists.
      ozone.getObjectStore().getS3Secret(ACCESS_KEY);
      ozone.getObjectStore().setS3Secret(ACCESS_KEY, SECRET_KEY);
    }
    gateway = new S3GatewayService();
    gateway.start(getCluster().getConf());
    factory = new S3ClientFactory(gateway.getConf());
    client = factory.createS3ClientV2();
    client.createBucket(b -> b.bucket(BUCKET));
  }

  @AfterAll
  void teardown() throws Exception {
    try {
      IOUtils.closeQuietly(client);
      if (gateway != null) {
        gateway.stop();
      }
    } finally {
      shutdownCluster();
      if (kdc != null) {
        kdc.stop();
      }
      UserGroupInformation.reset();
    }
  }

  /**
   * One change per verification point: a data chunk, the final chunk with and without a trailer,
   * the signed trailer, and a correctly signed but wrong trailer checksum.
   */
  static Stream<Arguments> rejectedUploads() {
    return Stream.of(
        Arguments.of(false, Mutation.PAYLOAD, SIGNATURE_DOES_NOT_MATCH),
        Arguments.of(false, Mutation.FINAL_SIGNATURE, SIGNATURE_DOES_NOT_MATCH),
        Arguments.of(true, Mutation.FINAL_SIGNATURE, SIGNATURE_DOES_NOT_MATCH),
        Arguments.of(true, Mutation.TRAILER, SIGNATURE_DOES_NOT_MATCH),
        Arguments.of(true, Mutation.BAD_CHECKSUM, BAD_DIGEST));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testPutObject(boolean trailer) throws Exception {
    String key = UUID.randomUUID().toString();
    sendSigned(trailer, Mutation.NONE,
        s3 -> s3.putObject(putRequest(key, trailer, Mutation.NONE), RequestBody.fromBytes(DATA)));
    assertThat(readObject(key)).isEqualTo(DATA);
  }

  @ParameterizedTest
  @MethodSource("rejectedUploads")
  void testRejectedPutObjectKeepsObject(boolean trailer, Mutation mutation, S3ErrorTable expected) throws Exception {
    String key = UUID.randomUUID().toString();
    byte[] original = "original object".getBytes(US_ASCII);
    client.putObject(b -> b.bucket(BUCKET).key(key), RequestBody.fromBytes(original));
    assertRejected(expected, () -> sendSigned(trailer, mutation,
        s3 -> s3.putObject(putRequest(key, trailer, mutation), RequestBody.fromBytes(DATA))));
    assertThat(readObject(key)).isEqualTo(original);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testUploadPart(boolean trailer) throws Exception {
    String key = UUID.randomUUID().toString();
    String uploadId = client.createMultipartUpload(b -> b.bucket(BUCKET).key(key)).uploadId();
    String etag = sendSigned(trailer, Mutation.NONE,
        s3 -> s3.uploadPart(partRequest(key, uploadId, trailer, Mutation.NONE), RequestBody.fromBytes(DATA))).eTag();
    client.completeMultipartUpload(b -> b.bucket(BUCKET).key(key).uploadId(uploadId)
        .multipartUpload(m -> m.parts(CompletedPart.builder().partNumber(1).eTag(etag).build())));
    assertThat(readObject(key)).isEqualTo(DATA);
  }

  @ParameterizedTest
  @MethodSource("rejectedUploads")
  void testRejectedUploadPartKeepsPart(boolean trailer, Mutation mutation, S3ErrorTable expected) throws Exception {
    String key = UUID.randomUUID().toString();
    String uploadId = client.createMultipartUpload(b -> b.bucket(BUCKET).key(key)).uploadId();
    client.uploadPart(b -> b.bucket(BUCKET).key(key).uploadId(uploadId).partNumber(1),
        RequestBody.fromBytes("original part".getBytes(US_ASCII)));
    List<Part> parts = listParts(key, uploadId);
    assertRejected(expected, () -> sendSigned(trailer, mutation,
        s3 -> s3.uploadPart(partRequest(key, uploadId, trailer, mutation), RequestBody.fromBytes(DATA))));
    assertThat(listParts(key, uploadId)).isEqualTo(parts);
  }

  /** Sends one request through a transport that applies the mutation after the SDK signed the body. */
  private <T> T sendSigned(boolean trailer, Mutation mutation, Function<S3Client, T> request) throws Exception {
    try (TamperingHttpClient transport = new TamperingHttpClient(trailer, mutation);
         S3Client signed = factory.createS3ClientV2(true, builder -> builder.httpClient(transport)
             .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED))) {
      return request.apply(signed);
    }
  }

  private static PutObjectRequest putRequest(String key, boolean trailer, Mutation mutation) {
    PutObjectRequest.Builder request = PutObjectRequest.builder().bucket(BUCKET).key(key);
    if (mutation == Mutation.BAD_CHECKSUM) {
      request.checksumCRC32C(WRONG_CHECKSUM).overrideConfiguration(o -> o.putHeader(X_AMZ_TRAILER, CRC32C_TRAILER));
    } else if (trailer) {
      request.checksumAlgorithm(ChecksumAlgorithm.CRC32_C);
    }
    return request.build();
  }

  private static UploadPartRequest partRequest(String key, String uploadId, boolean trailer, Mutation mutation) {
    UploadPartRequest.Builder request = UploadPartRequest.builder().bucket(BUCKET).key(key)
        .uploadId(uploadId).partNumber(1);
    if (mutation == Mutation.BAD_CHECKSUM) {
      request.checksumCRC32C(WRONG_CHECKSUM).overrideConfiguration(o -> o.putHeader(X_AMZ_TRAILER, CRC32C_TRAILER));
    } else if (trailer) {
      request.checksumAlgorithm(ChecksumAlgorithm.CRC32_C);
    }
    return request.build();
  }

  private byte[] readObject(String key) {
    return client.getObjectAsBytes(b -> b.bucket(BUCKET).key(key)).asByteArray();
  }

  private List<Part> listParts(String key, String uploadId) {
    return client.listParts(b -> b.bucket(BUCKET).key(key).uploadId(uploadId)).parts();
  }

  private static void assertRejected(S3ErrorTable expected, Executable upload) {
    S3Exception error = assertThrows(S3Exception.class, upload);
    assertThat(error.statusCode()).isEqualTo(expected.getHttpCode());
    assertThat(error.awsErrorDetails().errorCode()).isEqualTo(expected.getCode());
  }

  /** The part of the signed body changed after signing, or a wrong trailer checksum that the SDK signs. */
  enum Mutation {
    NONE, PAYLOAD, FINAL_SIGNATURE, TRAILER, BAD_CHECKSUM
  }

  /** Mutates the encoded bytes after the SDK has signed both headers and body. */
  private static final class TamperingHttpClient implements SdkHttpClient {
    private static final Pattern CHUNK = Pattern.compile("(?:^|\r\n)[0-9a-f]+;chunk-signature=([0-9a-f]{64})\r\n");
    private final SdkHttpClient delegate = ApacheHttpClient.create();
    private final boolean trailer;
    private final Mutation mutation;

    private TamperingHttpClient(boolean trailer, Mutation mutation) {
      this.trailer = trailer;
      this.mutation = mutation;
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      assertThat(request.httpRequest().firstMatchingHeader(X_AMZ_CONTENT_SHA256))
          .contains(trailer ? STREAMING_AWS4_HMAC_SHA256_PAYLOAD_TRAILER : STREAMING_AWS4_HMAC_SHA256_PAYLOAD);
      assertThat(request.httpRequest().firstMatchingHeader(DECODED_CONTENT_LENGTH_HEADER))
          .contains(Integer.toString(DATA.length));
      assertThat(request.httpRequest().firstMatchingHeader(CRC32C_TRAILER)).isEmpty();
      if (trailer) {
        assertThat(request.httpRequest().firstMatchingHeader(X_AMZ_TRAILER)).contains(CRC32C_TRAILER);
      } else {
        assertThat(request.httpRequest().firstMatchingHeader(X_AMZ_TRAILER)).isEmpty();
      }
      byte[] encoded;
      try (InputStream in = request.contentStreamProvider().orElseThrow().newStream()) {
        encoded = in.readAllBytes();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      String body = new String(encoded, ISO_8859_1);
      Matcher chunks = CHUNK.matcher(body);
      List<Integer> signatures = new ArrayList<>();
      List<Integer> payloads = new ArrayList<>();
      while (chunks.find()) {
        signatures.add(chunks.start(1));
        payloads.add(chunks.end());
      }
      assertThat(signatures).hasSize(3);
      assertThat(body).contains("\r\n0;chunk-signature=").endsWith("\r\n\r\n");
      if (trailer) {
        assertThat(body).contains("\r\n" + CRC32C_TRAILER + ":", "\r\nx-amz-trailer-signature:");
      }
      int offset = -1;
      switch (mutation) {
      case PAYLOAD:
        offset = payloads.get(1);
        break;
      case FINAL_SIGNATURE:
        offset = signatures.get(2);
        break;
      case TRAILER:
        offset = body.indexOf(CRC32C_TRAILER + ":") + CRC32C_TRAILER.length() + 1;
        break;
      case BAD_CHECKSUM:
        assertThat(body).contains("\r\n" + CRC32C_TRAILER + ":" + WRONG_CHECKSUM + "\r\n");
        break;
      default:
        break;
      }
      if (offset >= 0) {
        encoded[offset] = encoded[offset] == '0' ? (byte) '1' : (byte) '0';
      }
      return delegate.prepareRequest(HttpExecuteRequest.builder().request(request.httpRequest())
          .contentStreamProvider(() -> new ByteArrayInputStream(encoded)).build());
    }

    @Override
    public void close() {
      delegate.close();
    }
  }
}
