package org.apache.hadoop.ozone.s3.rest;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.s3.S3GatewayService;
import org.apache.ozone.test.OzoneTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;

import static org.apache.hadoop.ozone.OzoneConsts.LOCALHOST;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests for S3 REST edge cases that cannot be triggered by AWS SDK clients.
 * For example: negative/zero max-keys, or other invalid parameters that SDK would block client-side.
 */
public class TestS3RestNonSdkCases extends OzoneTestBase {

  private static MiniOzoneCluster cluster = null;
  private static S3GatewayService s3g = null;
  private static final String accessKey = "testuser";
  private static final String secretKey = "testpass";

  @BeforeAll
  public static void startCluster() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 1);
    s3g = new S3GatewayService();

    cluster = MiniOzoneCluster.newBuilder(conf)
        .addService(s3g)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    cluster.newClient().getObjectStore().createS3Bucket(getTestBucketName());
  }

  @AfterAll
  public static void shutdownCluster() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testListObjectsWithNegativeMaxKeys() throws Exception {
    final String bucketName = getTestBucketName();
    String s3Endpoint = getS3EndpointURL();
    String queryString = "max-keys=-1";
    String url = s3Endpoint + "/" + bucketName + "/?" + queryString;

    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
    conn.setRequestMethod("GET");
    S3V4Signer.signRequest(conn, accessKey, secretKey, "us-east-1", "s3", bucketName, queryString);

    int code = conn.getResponseCode();
    assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, code,
        "Should return 400 Bad Request (InvalidArgument) for max-keys=-1");
  }

  private static String getS3EndpointURL() {
    String addr = s3g.getConf().get(OZONE_S3G_HTTP_ADDRESS_KEY);
    String hostPort = addr.replace("0.0.0.0", LOCALHOST);
    return "http://" + hostPort;
  }

  private static String getTestBucketName() {
    return ("testrestnegmaxkeys" + System.currentTimeMillis()).toLowerCase();
  }
}
