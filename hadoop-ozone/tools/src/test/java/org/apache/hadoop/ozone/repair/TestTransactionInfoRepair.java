package org.apache.hadoop.ozone.repair;

import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.UnsupportedEncodingException;
import java.util.function.Supplier;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.TRANSACTION_INFO_TABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests TransactionInfoRepair.
 */
public class TestTransactionInfoRepair {


  private static final String DB_PATH = "testDBPath";
  private final long testTerm = 1;
  private final long testIndex = 1;

  private void mockTransactionInfo(MockedStatic<RocksDBUtils> mockUtil) {
    mockUtil.when(() ->
        RocksDBUtils.getValue(any(ManagedRocksDB.class), any(ColumnFamilyHandle.class), anyString(),
            any(Codec.class))).thenReturn(mock(TransactionInfo.class));

    TransactionInfo transactionInfo2 = mock(TransactionInfo.class);
    doReturn(TermIndex.valueOf(testTerm, testIndex)).when(transactionInfo2).getTermIndex();
    mockUtil.when(() ->
        RocksDBUtils.getValue(any(ManagedRocksDB.class), any(ColumnFamilyHandle.class), anyString(),
            any(Codec.class))).thenReturn(transactionInfo2);

  }

  @Test
  public void testUpdateTransactionInfoTableSuccessful() throws Exception {
    ManagedRocksDB mdb = mockRockDB();
    try (GenericTestUtils.SystemOutCapturer outCapturer = new GenericTestUtils.SystemOutCapturer()) {
      testCommand(mdb, mock(ColumnFamilyHandle.class), () -> {
        try {
          return outCapturer.getOutput();
        } catch (UnsupportedEncodingException e) {
          throw new RuntimeException(e);
        }
      }, new String[]{String.format("The original highest transaction Info was (t:%s, i:%s)", testTerm, testIndex),
              String.format("The highest transaction info has been updated to: (t:%s, i:%s)", testTerm, testIndex)});
    }



//    try (MockedStatic<ManagedRocksDB> mocked = mockStatic(ManagedRocksDB.class);
//          MockedStatic<RocksDBUtils> mockUtil = mockStatic(RocksDBUtils.class)) {
//      ManagedRocksDB db = mock(ManagedRocksDB.class);
////      RocksDB rdb = ;
////      when(rdb.getName()).thenReturn("rrrrrrrrr");
//      doReturn(mock(RocksDB.class)).when(db).get();
////      ManagedRocksDB db = spy(ManagedRocksDB.class);
//
//      mocked.when(() -> ManagedRocksDB.open(anyString(), anyList(), anyList())).thenReturn(db);
////      mocked.when(() -> ManagedRocksDB.open(anyString(), anyList(), anyList()))
// .thenThrow(new RocksDBException("hello"));
//
//      ColumnFamilyHandle columnFamilyHandle = mock(ColumnFamilyHandle.class);
////      doReturn("cccccccc".getBytes(StandardCharsets.UTF_8)).when(columnFamilyHandle).getName();
//      TransactionInfo transactionInfo = mock(TransactionInfo.class);
//      List<ColumnFamilyHandle> ls1 = new ArrayList<>();
////      ls1.add(new )
//      mockUtil.when(() -> RocksDBUtils.getColumnFamilyDescriptors(anyString()))
//          .thenReturn(ls1);
//
//      mockUtil.when(() -> RocksDBUtils.getColumnFamilyHandle(anyString(), anyList()))
//          .thenReturn(columnFamilyHandle);
//      mockUtil.when(() ->
//          RocksDBUtils.getValue(any(ManagedRocksDB.class), any(ColumnFamilyHandle.class), anyString(),
//              any(Codec.class))).thenReturn(transactionInfo);
//      TransactionInfo transactionInfo2 = mock(TransactionInfo.class);
//      long testTerm = 1;
//      long testIndex = 1;
//      doReturn(TermIndex.valueOf(testTerm, testIndex)).when(transactionInfo2).getTermIndex();
//      mockUtil.when(() ->
//          RocksDBUtils.getValue(any(ManagedRocksDB.class), any(ColumnFamilyHandle.class), anyString(),
//              any(Codec.class))).thenReturn(transactionInfo2);
//
//      TransactionInfoRepair cmd = spy(TransactionInfoRepair.class);
////      ManagedRocksDB db3 = mock(ManagedRocksDB.class);
////      doThrow(new RuntimeException("tttttttt")).when(cmd).getManagedRocksDB(anyList(), anyList());
//      cmd.setHighestTransactionTermIndex(testTerm + "#" + testIndex);
//      RDBRepair rrr = mock(RDBRepair.class);
//      when(rrr.getDbPath()).thenReturn("some path.....");// must have, otherwise test fail
//      cmd.parent = rrr;
//
//      try (GenericTestUtils.SystemOutCapturer outCapturer = new GenericTestUtils.SystemOutCapturer()) {
//        cmd.call();
////        IOException ioe = assertThrows(IOException.class, () -> cmd.execute(client));
//        assertThat(outCapturer.getOutput()).contains(
//            String.format("The original highest transaction Info was (t:%s, i:%s)", testTerm, testIndex));
//
//        assertThat(outCapturer.getOutput()).contains(
//            String.format("The highest transaction info has been updated to: (t:%s, i:%s)", testTerm, testIndex));
//
////        assertThat(ioe.getMessage()).contains("remove current leader");
//      }
//    }
  }

  @Test
  public void testCommandWhenTableNotInDBForGivenPath() throws Exception {
    ManagedRocksDB mdb = mockRockDB();
    try (GenericTestUtils.SystemErrCapturer errCapturer = new GenericTestUtils.SystemErrCapturer()) {
      testCommand(mdb, null, () -> {
        try {
          return errCapturer.getOutput();
        } catch (UnsupportedEncodingException e) {
          throw new RuntimeException(e);
        }
      }, new String[]{TRANSACTION_INFO_TABLE + " is not in a column family in DB for the given path"});
    }

//    try (MockedStatic<ManagedRocksDB> mocked = mockStatic(ManagedRocksDB.class);
//         MockedStatic<RocksDBUtils> mockUtil = mockStatic(RocksDBUtils.class)) {
//
//
//
//      mockUtil.when(() -> RocksDBUtils.getColumnFamilyHandle(anyString(), anyList()))
//          .thenReturn(null);
//      TransactionInfoRepair cmd = spy(TransactionInfoRepair.class);
//      RDBRepair rrr = mock(RDBRepair.class);
////      when(rrr.getDbPath()).thenReturn("some path.....");
//      cmd.parent = rrr;
//
//      try (GenericTestUtils.SystemErrCapturer errCapturer = new GenericTestUtils.SystemErrCapturer()) {
//        cmd.call();
//        assertThat(errCapturer.getOutput()).contains(TRANSACTION_INFO_TABLE +
//        " is not in a column family in DB for the given path");
//      }
//
//    }
  }

  @Test
  public void testCommandWhenFailToUpdateRocksDBForGivenPath() throws Exception {
    ManagedRocksDB mdb = mockRockDB();
    RocksDB rdb = mdb.get();

    doThrow(RocksDBException.class).when(rdb)
        .put(any(ColumnFamilyHandle.class), any(byte[].class), any(byte[].class));

    try (GenericTestUtils.SystemErrCapturer errCapturer = new GenericTestUtils.SystemErrCapturer()) {
      testCommand(mdb, mock(ColumnFamilyHandle.class), () -> {
        try {
          return errCapturer.getOutput();
        } catch (UnsupportedEncodingException e) {
          throw new RuntimeException(e);
        }
      }, new String[]{"Failed to update the RocksDB for the given path: " + DB_PATH});
    }

//    try (MockedStatic<ManagedRocksDB> mocked = mockStatic(ManagedRocksDB.class);
//         MockedStatic<RocksDBUtils> mockUtil = mockStatic(RocksDBUtils.class)) {
//      ManagedRocksDB db = mock(ManagedRocksDB.class);
//      RocksDB rocksDB = mock(RocksDB.class);
//      doReturn(rocksDB).when(db).get();
//
//      doThrow(RocksDBException.class).when(rocksDB)
//          .put(any(ColumnFamilyHandle.class), any(byte[].class), any(byte[].class));
//      mocked.when(() -> ManagedRocksDB.open(anyString(), anyList(), anyList())).thenReturn(db);
//      mockUtil.when(() -> RocksDBUtils.getColumnFamilyHandle(anyString(), anyList()))
//          .thenReturn(mock(ColumnFamilyHandle.class));
//      mockUtil.when(() ->
//          RocksDBUtils.getValue(any(ManagedRocksDB.class), any(ColumnFamilyHandle.class), anyString(),
//              any(Codec.class))).thenReturn(mock(TransactionInfo.class));
//
//      TransactionInfoRepair cmd = spy(TransactionInfoRepair.class);
//      RDBRepair rdbRepair = mock(RDBRepair.class);
//      String dbPath = "testDBPath";
//      when(rdbRepair.getDbPath()).thenReturn(dbPath);
//      doReturn(rdbRepair).when(cmd).getParent();
//      long testTerm = 1;
//      long testIndex = 1;
//      cmd.setHighestTransactionTermIndex(testTerm + "#" + testIndex);
//
//      try (GenericTestUtils.SystemErrCapturer errCapturer = new GenericTestUtils.SystemErrCapturer()) {
//        cmd.call();
//        assertThat(errCapturer.getOutput()).contains("Failed to update the RocksDB for the given path: " + dbPath);
//      }
//
//    }
  }


  private void testCommand(ManagedRocksDB mdb, ColumnFamilyHandle columnFamilyHandle, Supplier<String> capturer,
                           String[] messages) throws Exception {
    try (MockedStatic<ManagedRocksDB> mocked = mockStatic(ManagedRocksDB.class);
         MockedStatic<RocksDBUtils> mockUtil = mockStatic(RocksDBUtils.class)) {

      mocked.when(() -> ManagedRocksDB.open(anyString(), anyList(), anyList())).thenReturn(mdb);
      mockUtil.when(() -> RocksDBUtils.getColumnFamilyHandle(anyString(), anyList()))
          .thenReturn(columnFamilyHandle);

      mockUtil.when(() ->
          RocksDBUtils.getValue(any(ManagedRocksDB.class), any(ColumnFamilyHandle.class), anyString(),
              any(Codec.class))).thenReturn(mock(TransactionInfo.class));

      mockTransactionInfo(mockUtil);

      TransactionInfoRepair cmd = spy(TransactionInfoRepair.class);
      RDBRepair rdbRepair = mock(RDBRepair.class);
      when(rdbRepair.getDbPath()).thenReturn(DB_PATH);
      doReturn(rdbRepair).when(cmd).getParent();
      cmd.setHighestTransactionTermIndex(testTerm + "#" + testIndex);

//      try (GenericTestUtils.SystemErrCapturer errCapturer = new GenericTestUtils.SystemErrCapturer()) {
      cmd.call();
      for (String message : messages) {
        assertThat(capturer.get()).contains(message);
      }
//      }
    }
  }

  private ManagedRocksDB mockRockDB() {
    ManagedRocksDB db = mock(ManagedRocksDB.class);
    RocksDB rocksDB = mock(RocksDB.class);
    doReturn(rocksDB).when(db).get();
    return db;
  }


}
