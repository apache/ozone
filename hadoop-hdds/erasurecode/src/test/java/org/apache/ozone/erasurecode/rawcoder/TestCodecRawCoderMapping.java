package org.apache.ozone.erasurecode.rawcoder;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.ozone.erasurecode.rawcoder.util.CodecUtil;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test the codec to raw coder mapping.
 */
public class TestCodecRawCoderMapping {

  private static final String RS_CODEC_NAME = "rs";
  private static final String XOR_CODEC_NAME = "xor";

  @Test
  public void testRSDefaultRawCoder() {
    ECReplicationConfig coderOptions =
        new ECReplicationConfig(RS_CODEC_NAME + "-6-3-1024K");
    // should return default raw coder of rs codec
    RawErasureEncoder encoder =
        CodecUtil.createRawEncoderWithFallback(coderOptions);
    RawErasureDecoder decoder =
        CodecUtil.createRawDecoderWithFallback(coderOptions);
    if (ErasureCodeNative.isNativeCodeLoaded()) {
      Assert.assertTrue(encoder instanceof NativeRSRawEncoder);
      Assert.assertTrue(decoder instanceof NativeRSRawDecoder);
    } else {
      Assert.assertTrue(encoder instanceof RSRawEncoder);
      Assert.assertTrue(decoder instanceof RSRawDecoder);
    }
  }

  @Test
  public void testXORRawCoder() {
    ECReplicationConfig coderOptions =
        new ECReplicationConfig(XOR_CODEC_NAME + "-6-3-1024K");
    // should return default raw coder of rs codec
    RawErasureEncoder encoder =
        CodecUtil.createRawEncoderWithFallback(coderOptions);
    RawErasureDecoder decoder =
        CodecUtil.createRawDecoderWithFallback(coderOptions);
    if (ErasureCodeNative.isNativeCodeLoaded()) {
      Assert.assertTrue(encoder instanceof NativeXORRawEncoder);
      Assert.assertTrue(decoder instanceof NativeXORRawDecoder);
    } else {
      Assert.assertTrue(encoder instanceof XORRawEncoder);
      Assert.assertTrue(decoder instanceof XORRawDecoder);
    }
  }
}