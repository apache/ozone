package org.apache.hadoop.ozone.om.snapshot.filter;

import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.service.AbstractKeyDeletingService;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;

import java.io.IOException;

public class ReclaimableDirFilter extends ReclaimableFilter<OmKeyInfo> {

  private final OzoneManager ozoneManager;

  /**
   * Filter to return deleted directories which are reclaimable based on their presence in previous snapshot in
   * the snapshot chain.
   *
   * @param omSnapshotManager
   * @param snapshotChainManager
   * @param currentSnapshotInfo  : If null the deleted keys in AOS needs to be processed, hence the latest snapshot
   *                             in the snapshot chain corresponding to bucket key needs to be processed.
   * @param metadataManager      : MetadataManager corresponding to snapshot or AOS.
   * @param lock                 : Lock for Active OM.
   */
  public ReclaimableDirFilter(OzoneManager ozoneManager,
                              OmSnapshotManager omSnapshotManager, SnapshotChainManager snapshotChainManager,
                              SnapshotInfo currentSnapshotInfo, OMMetadataManager metadataManager,
                              IOzoneManagerLock lock) {
    super(ozoneManager, omSnapshotManager, snapshotChainManager, currentSnapshotInfo, metadataManager, lock, 1);
    this.ozoneManager = ozoneManager;
  }

  @Override
  protected String getVolumeName(Table.KeyValue<String, OmKeyInfo> keyValue) throws IOException {
    return keyValue.getValue().getVolumeName();
  }

  @Override
  protected String getBucketName(Table.KeyValue<String, OmKeyInfo> keyValue) throws IOException {
    return keyValue.getValue().getBucketName();
  }

  @Override
  protected Boolean isReclaimable(Table.KeyValue<String, OmKeyInfo> deletedDirInfo) throws IOException {
    ReferenceCounted<OmSnapshot> previousSnapshot = getPreviousOmSnapshot(0);
    Table<String, OmDirectoryInfo> prevDirTable = previousSnapshot == null ? null :
        previousSnapshot.get().getMetadataManager().getDirectoryTable();
    return isDirReclaimable(deletedDirInfo, prevDirTable,
        getMetadataManager().getSnapshotRenamedTable());
  }

  private boolean isDirReclaimable(Table.KeyValue<String, OmKeyInfo> deletedDir,
                                   Table<String, OmDirectoryInfo> previousDirTable,
                                   Table<String, String> renamedTable) throws IOException {
    if (previousDirTable == null) {
      return true;
    }

    String deletedDirDbKey = deletedDir.getKey();
    OmKeyInfo deletedDirInfo = deletedDir.getValue();
    String dbRenameKey = ozoneManager.getMetadataManager().getRenameKey(
        deletedDirInfo.getVolumeName(), deletedDirInfo.getBucketName(),
        deletedDirInfo.getObjectID());

      /*
      snapshotRenamedTable: /volumeName/bucketName/objectID ->
          /volumeId/bucketId/parentId/dirName
       */
    String dbKeyBeforeRename = renamedTable.getIfExist(dbRenameKey);
    String prevDbKey = null;

    if (dbKeyBeforeRename != null) {
      prevDbKey = dbKeyBeforeRename;
    } else {
      // In OMKeyDeleteResponseWithFSO OzonePathKey is converted to
      // OzoneDeletePathKey. Changing it back to check the previous DirTable.
      prevDbKey = ozoneManager.getMetadataManager()
          .getOzoneDeletePathDirKey(deletedDirDbKey);
    }

    OmDirectoryInfo prevDirectoryInfo = previousDirTable.get(prevDbKey);
    if (prevDirectoryInfo == null) {
      return true;
    }
    return prevDirectoryInfo.getObjectID() != deletedDirInfo.getObjectID();
  }
}
