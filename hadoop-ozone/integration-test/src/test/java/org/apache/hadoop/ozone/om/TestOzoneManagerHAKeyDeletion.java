package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;

public class TestOzoneManagerHAKeyDeletion extends TestOzoneManagerHA {

  @Test
  public void testKeyDeletion() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String data = "random data";
    String keyName1 = "dir/file1";
    String keyName2 = "dir/file2";
    String keyName3 = "dir/file3";
    String keyName4 = "dir/file4";
    List<String> keyList1 = new ArrayList<>();
    keyList1.add(keyName2);
    keyList1.add(keyName3);

    testCreateFile(ozoneBucket, keyName1, data, true, false);
    testCreateFile(ozoneBucket, keyName2, data, true, false);
    testCreateFile(ozoneBucket, keyName3, data, true, false);
    testCreateFile(ozoneBucket, keyName4, data, true, false);

    ozoneBucket.deleteKey(keyName1);
    ozoneBucket.deleteKey(keyName2);
    ozoneBucket.deleteKey(keyName3);
    ozoneBucket.deleteKey(keyName4);

    // Now check delete table has entries been removed.

    OzoneManager ozoneManager = getCluster().getOMLeader();

    KeyDeletingService keyDeletingService =
        (KeyDeletingService) ozoneManager.getKeyManager().getDeletingService();

    // Check on leader OM Count.
    GenericTestUtils.waitFor(() ->
        keyDeletingService.getRunCount().get() >= 2, 10000, 120000);
    GenericTestUtils.waitFor(() ->
            keyDeletingService.getDeletedKeyCount().get() == 4, 10000, 120000);

    // Check delete table is empty or not on all OMs.
    getCluster().getOzoneManagersList().forEach((om) -> {
      try {
        GenericTestUtils.waitFor(() ->
                !om.getMetadataManager().getDeletedTable().iterator().hasNext(),
            10000, 120000);
      } catch (Exception ex) {
        fail("TestOzoneManagerHAKeyDeletion failed");
      }
    });
  }
}
