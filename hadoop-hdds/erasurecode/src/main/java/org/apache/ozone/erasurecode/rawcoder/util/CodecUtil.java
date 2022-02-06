package org.apache.ozone.erasurecode.rawcoder.util;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.ozone.erasurecode.CodecRegistry;
import org.apache.ozone.erasurecode.rawcoder.RawErasureCoderFactory;
import org.apache.ozone.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.ozone.erasurecode.rawcoder.RawErasureEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A codec &amp; coder utility to help create coders conveniently.
 * <p>
 * {@link CodecUtil} includes erasure coder configurations key and default
 * values such as coder class name and erasure codec option values included
 * by {@link ECReplicationConfig}.{@link RawErasureEncoder} and
 * {@link RawErasureDecoder} are created by createRawEncoder and
 * createRawDecoder.
 */
@InterfaceAudience.Private
public final class CodecUtil {

  public static final Logger LOG = LoggerFactory.getLogger(CodecUtil.class);

  private CodecUtil() {
  }

  private static RawErasureCoderFactory createRawCoderFactory(String coderName,
      String codecName) {
    RawErasureCoderFactory fact;
    fact = CodecRegistry.getInstance().
        getCoderByName(codecName, coderName);

    return fact;
  }

  public static RawErasureEncoder createRawEncoderWithFallback(
      final ECReplicationConfig ecReplicationConfig) {
    // Note: Coders can be configurable, but for now, we just use whats
    //  available.
    String codecName = ecReplicationConfig.getCodec().name().toLowerCase();
    String[] rawCoderNames =
        CodecRegistry.getInstance().getCoderNames(codecName);
    for (String rawCoderName : rawCoderNames) {
      try {
        if (rawCoderName != null) {
          RawErasureCoderFactory fact =
              createRawCoderFactory(rawCoderName, codecName);
          return fact.createEncoder(ecReplicationConfig);
        }
      } catch (LinkageError | Exception e) {
        // Fallback to next coder if possible
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Failed to create raw erasure encoder " + rawCoderName
                  + ", fallback to next codec if possible",
              e);
        }
      }
    }
    throw new IllegalArgumentException(
        "Fail to create raw erasure " + "encoder with given codec: "
            + codecName);
  }

  public static RawErasureDecoder createRawDecoderWithFallback(
      final ECReplicationConfig ecReplicationConfig) {
    // Note: Coders can be configurable, but for now, we just use whats
    //  available.
    String codecName = ecReplicationConfig.getCodec().name().toLowerCase();
    String[] coders = CodecRegistry.getInstance().getCoderNames(codecName);
    for (String rawCoderName : coders) {
      try {
        if (rawCoderName != null) {
          RawErasureCoderFactory fact =
              createRawCoderFactory(rawCoderName, codecName);
          return fact.createDecoder(ecReplicationConfig);
        }
      } catch (LinkageError | Exception e) {
        // Fallback to next coder if possible
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Failed to create raw erasure decoder " + rawCoderName
                  + ", fallback to next codec if possible",
              e);
        }
      }
    }
    throw new IllegalArgumentException(
        "Fail to create raw erasure " + "decoder with given codec: "
            + codecName);
  }
}