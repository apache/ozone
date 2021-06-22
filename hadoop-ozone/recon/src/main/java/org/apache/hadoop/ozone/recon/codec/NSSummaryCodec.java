package org.apache.hadoop.ozone.recon.codec;

import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.hdds.utils.db.Codec;

import java.io.IOException;

public class NSSummaryCodec implements Codec<NSSummary>{

  @Override
  public byte[] toPersistedFormat(NSSummary object) throws IOException {
    return new byte[0];
  }

  @Override
  public NSSummary fromPersistedFormat(byte[] rawData) throws IOException {
    return null;
  }

  @Override
  public NSSummary copyObject(NSSummary object) {
    return null;
  }
}
