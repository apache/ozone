package org.apache.hadoop.ozone.om;

import mockit.Expectations;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test delete table prefix is correctly built.
 */
public class TestDeleteTablePrefix {
  @Test
  public void testKeyForDeleteTable() {
    OmKeyInfo omKeyInfo = new OmKeyInfo.Builder()
        .setObjectID(42).build();
    Assert.assertEquals("0000000000000001-2A-0",
            new DeleteTablePrefix(1L, true).buildKey(omKeyInfo));

    long current = Time.now();
    String expectedTimestamp = String.format("%016X-%16X-2A", current, 3L);

    new Expectations(Time.class) {
      {
        Time.now(); result = current;
      }
    };
    Assert.assertEquals(expectedTimestamp,
            new DeleteTablePrefix(3L, false).buildKey(omKeyInfo));

    Assert.assertEquals("0000000000000003-2A-0",
            new DeleteTablePrefix(3L, true).buildKey(omKeyInfo));

    Assert.assertEquals("0000000000000144-2A-0",
            new DeleteTablePrefix(324L, true).buildKey(omKeyInfo));
  }
}
