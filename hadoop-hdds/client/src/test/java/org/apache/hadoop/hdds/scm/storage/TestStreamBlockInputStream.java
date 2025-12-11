package org.apache.hadoop.hdds.scm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.function.Function;

public class TestStreamBlockInputStream {

  @Test
  public void testCustomStreamReadConfigIsApplied() throws Exception {
    // Arrange: create a config with non-default values
    OzoneClientConfig clientConfig = new OzoneClientConfig();
    clientConfig.setStreamReadPreReadSize(64L << 20);
    clientConfig.setStreamReadResponseDataSize(2 << 20);
    clientConfig.setStreamReadTimeout(Duration.ofSeconds(5));

    // Sanity check
    assertEquals(Duration.ofSeconds(5), clientConfig.getStreamReadTimeout());
    // Create a dummy BlockID for the test
    BlockID blockID = new BlockID(1L, 1L);
    long length = 1024L;
    // Create a mock Pipeline instance.
    Pipeline pipeline = mock(Pipeline.class);

    Token<OzoneBlockTokenIdentifier> token = null;
    // Mock XceiverClientFactory since StreamBlockInputStream requires it in the constructor
    XceiverClientFactory xceiverClientFactory = mock(XceiverClientFactory.class);
    Function<BlockID, BlockLocationInfo> refreshFunction = b -> null;
    // Create a StreamBlockInputStream instance
    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        blockID, length, pipeline, token,
        xceiverClientFactory, refreshFunction, clientConfig)) {

      // Assert: fields should match config values
      assertEquals(64L << 20, sbis.getPreReadSize());
      assertEquals(2 << 20, sbis.getResponseDataSize());
      assertEquals(Duration.ofSeconds(5), sbis.getReadTimeout());
    }
  }
}
