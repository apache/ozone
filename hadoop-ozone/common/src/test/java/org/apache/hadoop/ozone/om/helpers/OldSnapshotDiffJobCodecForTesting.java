package org.apache.hadoop.ozone.om.helpers;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hdds.utils.db.Codec;
import java.io.IOException;


public class OldSnapshotDiffJobCodecForTesting
    implements Codec<SnapshotDiffJob> {

  private static final ObjectMapper MAPPER = new ObjectMapper()
      .setSerializationInclusion(JsonInclude.Include.NON_NULL)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @Override
  public Class<SnapshotDiffJob> getTypeClass() {
    return SnapshotDiffJob.class;
  }

  @Override
  public byte[] toPersistedFormatImpl(SnapshotDiffJob object) throws IOException {
    return MAPPER.writeValueAsBytes(object);
  }

  @Override
  public SnapshotDiffJob fromPersistedFormatImpl(byte[] rawData) throws IOException {
    return MAPPER.readValue(rawData, SnapshotDiffJob.class);
  }

  @Override
  public SnapshotDiffJob copyObject(SnapshotDiffJob object) {
    // Note: Not really a "copy". from OmDBDiffReportEntryCodec
    return object;
  }
}
