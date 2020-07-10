package org.apache.hadoop.hdds.scm.metadata;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.junit.Test;

import java.util.UUID;

public class TestPipelineIDCodec {

  @Test
  public void testPersistingZeroAsUUID() throws Exception {
    long leastSigBits = 0x0000_0000_0000_0000l;
    long mostSigBits = 0x0000_0000_0000_0000l;
    byte[] expected = new byte[] {
        b(0x00), b(0x00), b(0x00), b(0x00), b(0x00), b(0x00), b(0x00), b(0x00),
        b(0x00), b(0x00), b(0x00), b(0x00), b(0x00), b(0x00), b(0x00), b(0x00)
    };

    checkPersisting(leastSigBits, mostSigBits, expected);
  }

  @Test
  public void testPersistingFFAsUUID() throws Exception {
    long leastSigBits = 0xFFFF_FFFF_FFFF_FFFFL;
    long mostSigBits = 0xFFFF_FFFF_FFFF_FFFFL;
    byte[] expected = new byte[] {
        b(0xFF), b(0xFF), b(0xFF), b(0xFF), b(0xFF), b(0xFF), b(0xFF), b(0xFF),
        b(0xFF), b(0xFF), b(0xFF), b(0xFF), b(0xFF), b(0xFF), b(0xFF), b(0xFF)
    };

    checkPersisting(leastSigBits, mostSigBits, expected);
  }

  @Test
  public void testPersistingARandomUUID() throws Exception {
    for (int i=0; i<100; i++) {
      UUID uuid = UUID.randomUUID();

      long mask = 0x0000_0000_0000_00FFL;

      byte[] expected = new byte[] {
          b(((int) (uuid.getMostSignificantBits() >> 56 & mask))),
          b(((int) (uuid.getMostSignificantBits() >> 48 & mask))),
          b(((int) (uuid.getMostSignificantBits() >> 40 & mask))),
          b(((int) (uuid.getMostSignificantBits() >> 32 & mask))),
          b(((int) (uuid.getMostSignificantBits() >> 24 & mask))),
          b(((int) (uuid.getMostSignificantBits() >> 16 & mask))),
          b(((int) (uuid.getMostSignificantBits() >> 8 & mask))),
          b(((int) (uuid.getMostSignificantBits() & mask))),

          b(((int) (uuid.getLeastSignificantBits() >> 56 & mask))),
          b(((int) (uuid.getLeastSignificantBits() >> 48 & mask))),
          b(((int) (uuid.getLeastSignificantBits() >> 40 & mask))),
          b(((int) (uuid.getLeastSignificantBits() >> 32 & mask))),
          b(((int) (uuid.getLeastSignificantBits() >> 24 & mask))),
          b(((int) (uuid.getLeastSignificantBits() >> 16 & mask))),
          b(((int) (uuid.getLeastSignificantBits() >> 8 & mask))),
          b(((int) (uuid.getLeastSignificantBits() & mask))),
      };

      checkPersisting(
          uuid.getMostSignificantBits(),
          uuid.getLeastSignificantBits(),
          expected
      );
    }
  }

  @Test
  public void testConvertAndReadBackZeroAsUUID() throws Exception {
    long mostSigBits = 0x0000_0000_0000_0000L;
    long leastSigBits = 0x0000_0000_0000_0000L;
    UUID uuid = new UUID(mostSigBits, leastSigBits);
    PipelineID pid = PipelineID.valueOf(uuid);

    byte[] encoded = new PipelineIDCodec().toPersistedFormat(pid);
    PipelineID decoded = new PipelineIDCodec().fromPersistedFormat(encoded);

    assertEquals(pid, decoded);
  }

  @Test
  public void testConvertAndReadBackFFAsUUID() throws Exception {
    long mostSigBits = 0xFFFF_FFFF_FFFF_FFFFL;
    long leastSigBits = 0xFFFF_FFFF_FFFF_FFFFL;
    UUID uuid = new UUID(mostSigBits, leastSigBits);
    PipelineID pid = PipelineID.valueOf(uuid);

    byte[] encoded = new PipelineIDCodec().toPersistedFormat(pid);
    PipelineID decoded = new PipelineIDCodec().fromPersistedFormat(encoded);

    assertEquals(pid, decoded);
  }

  @Test
  public void testConvertAndReadBackRandomUUID() throws Exception {
    UUID uuid = UUID.randomUUID();
    PipelineID pid = PipelineID.valueOf(uuid);

    byte[] encoded = new PipelineIDCodec().toPersistedFormat(pid);
    PipelineID decoded = new PipelineIDCodec().fromPersistedFormat(encoded);

    assertEquals(pid, decoded);
  }

  private void checkPersisting(
      long mostSigBits, long leastSigBits, byte[] expected
  ) throws Exception {
    UUID uuid = new UUID(mostSigBits, leastSigBits);
    PipelineID pid = PipelineID.valueOf(uuid);

    byte[] encoded = new PipelineIDCodec().toPersistedFormat(pid);

    assertArrayEquals(expected, encoded);
  }

  private byte b(int i) {
    return (byte) (i & 0x0000_00FF);
  }
}
