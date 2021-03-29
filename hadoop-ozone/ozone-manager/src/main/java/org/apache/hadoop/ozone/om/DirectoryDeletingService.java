package org.apache.hadoop.ozone.om;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.ozone.ClientVersions.CURRENT_VERSION;

/**
 * This is a background service to delete orphan directories and its
 * sub paths(sub-dirs and sub-files).
 *
 * <p>
 * This will scan the metadata of om periodically to get the orphan dirs from
 * DeletedDirectoryTable and find its sub paths. It will fetch all sub-files
 * from KeyTable and move those to DeletedTable so that OM's
 * KeyDeletingService will cleanup those files later. It will fetch all
 * sub-directories from the DirectoryTable and move those to
 * DeletedDirectoryTable so that these will be visited in next iterations.
 *
 * <p>
 * After moving all sub-files and sub-dirs the parent orphan directory will be
 * deleted by this service. It will continue traversing until all the leaf path
 * components of an orphan directory is visited.
 */
public class DirectoryDeletingService extends BackgroundService {

  private final KeyManager keyManager;
  private final OzoneManager ozoneManager;
  private AtomicLong deletedDirsCount;
  private AtomicLong deletedFilesCount;
  private final AtomicLong runCount;
  private final long numEntries = 1000;
  private static ClientId clientId = ClientId.randomId();

  public DirectoryDeletingService(long interval, TimeUnit unit,
      long serviceTimeout, OzoneManager ozoneManager) {
    super("DirectoryDeletingService", interval, unit, 1, serviceTimeout);
    this.keyManager = ozoneManager.getKeyManager();
    this.ozoneManager = ozoneManager;
    this.deletedDirsCount = new AtomicLong(0);
    this.deletedFilesCount = new AtomicLong(0);
    this.runCount = new AtomicLong(0);
  }

  private boolean shouldRun() {
    if (ozoneManager == null) {
      // OzoneManager can be null for testing
      return true;
    }
    return ozoneManager.isLeaderReady();
  }

  private boolean isRatisEnabled() {
    if (ozoneManager == null) {
      return false;
    }
    return ozoneManager.isRatisEnabled();
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new DirectoryDeletingService.DirDeletingTask());
    return queue;
  }

  private class DirDeletingTask implements BackgroundTask {

    @Override
    public int getPriority() {
      return 0;
    }

    @Override
    public BackgroundTaskResult call() throws Exception {
      if (shouldRun()) {
        runCount.incrementAndGet();
        long count = numEntries;
        try {
          long startTime = Time.monotonicNow();
          // step-1) Get one pending deleted directory
          OmKeyInfo pendingDeletedDirInfo = keyManager.getPendingDeletionDir();
          if (pendingDeletedDirInfo != null) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Pending deleted dir name: {}",
                  pendingDeletedDirInfo.getKeyName());
            }
            // step-1: get all sub directories under the deletedDir
            List<OmKeyInfo> dirs =
                keyManager.getPendingDeletionSubDirs(pendingDeletedDirInfo,
                    --count);
            count = count - dirs.size();
            List<OmKeyInfo> deletedSubDirList = new ArrayList<>();
            for (OmKeyInfo dirInfo : dirs) {
              deletedSubDirList.add(dirInfo);
              if (LOG.isDebugEnabled()) {
                LOG.debug("deleted sub dir name: {}",
                    dirInfo.getKeyName());
              }
            }

            // step-2: get all sub files under the deletedDir
            List<OmKeyInfo> purgeDeletedFiles =
                keyManager.getPendingDeletionSubFiles(pendingDeletedDirInfo,
                    --count);

            if (LOG.isDebugEnabled()) {
              for (OmKeyInfo fileInfo : purgeDeletedFiles) {
                LOG.debug("deleted sub file name: {}", fileInfo.getKeyName());
              }
            }

            // step-3: Since there is a boundary condition of 'numEntries' in
            // each batch, check whether the sub paths count reached batch size
            // limit. If count reached limit then there can be some more child
            // paths to be visited and will keep the parent deleted directory
            // for one more pass.
            List<String> purgeDeletedDirs = new ArrayList<>();
            if (count > 0) {
              // TODO: Now, there is only one entry in this list. Maintained
              //  list data structure becuase this can be extended to add
              //  more directories within the batchSize limit.
              purgeDeletedDirs.add(pendingDeletedDirInfo.getPath());
            }

            if (isRatisEnabled()) {
              submitPurgePaths(purgeDeletedDirs, purgeDeletedFiles,
                  deletedSubDirList);
            }
            // TODO: need to handle delete with non-ratis

            deletedDirsCount.addAndGet(purgeDeletedDirs.size());
            deletedFilesCount.addAndGet(purgeDeletedFiles.size());
            if (LOG.isDebugEnabled()) {
              LOG.debug("Number of dirs deleted: {}, Number of files moved:" +
                      " {} to DeletedTable, elapsed time: {}ms",
                  deletedDirsCount, deletedFilesCount,
                  Time.monotonicNow() - startTime);
            }
          }
        } catch(Exception e){

        }
      }

      // place holder by returning empty results of this call back.
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }
  }

  /**
   * Returns the number of dirs deleted by the background service.
   *
   * @return Long count.
   */
  @VisibleForTesting
  public long getDeletedDirsCount() {
    return deletedDirsCount.get();
  }

  /**
   * Returns the number of files moved to DeletedTable by the background
   * service.
   *
   * @return Long count.
   */
  @VisibleForTesting
  public long getMovedFilesCount() {
    return deletedFilesCount.get();
  }

  /**
   * Returns the number of times this Background service has run.
   *
   * @return Long, run count.
   */
  @VisibleForTesting
  public long getRunCount() {
    return runCount.get();
  }

  private int submitPurgePaths(List<String> purgeDeletedDirs,
      List<OmKeyInfo> purgeDeletedFiles, List<OmKeyInfo> markDirsAsDeleted) {
    // Put all keys to be purged in a list
    int deletedCount = 0;
    OzoneManagerProtocolProtos.PurgePathsRequest.Builder purgePathsRequest =
        OzoneManagerProtocolProtos.PurgePathsRequest.newBuilder();
    for (String purgeDir : purgeDeletedDirs) {
      purgePathsRequest.addDeletedDirs(purgeDir);
    }
    for (OmKeyInfo purgeFile : purgeDeletedFiles) {
      purgePathsRequest.addDeletedSubFiles(
          purgeFile.getProtobuf(CURRENT_VERSION));
    }

    // Add these directories to deletedDirTable, so that its sub-paths will be
    // traversed in next iteration to ensure cleanup all sub-children.
    for (OmKeyInfo dir : markDirsAsDeleted) {
      purgePathsRequest.addMarkDeletedSubDirs(dir.getProtobuf(CURRENT_VERSION));
    }

    OzoneManagerProtocolProtos.OMRequest omRequest =
        OzoneManagerProtocolProtos.OMRequest.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.PurgePaths)
            .setPurgePathsRequest(purgePathsRequest)
            .setClientId(clientId.toString())
            .build();

    // Submit Purge paths request to OM
    try {
      RaftClientRequest raftClientRequest =
          createRaftClientRequestForDelete(omRequest);
      ozoneManager.getOmRatisServer().submitRequest(omRequest,
          raftClientRequest);
    } catch (ServiceException e) {
      LOG.error("PurgePaths request failed. Will retry at next run.");
      return 0;
    }
    return deletedCount;
  }


  private RaftClientRequest createRaftClientRequestForDelete(
      OzoneManagerProtocolProtos.OMRequest omRequest) {
    return RaftClientRequest.newBuilder()
        .setClientId(clientId)
        .setServerId(ozoneManager.getOmRatisServer().getRaftPeerId())
        .setGroupId(ozoneManager.getOmRatisServer().getRaftGroupId())
        .setCallId(runCount.get())
        .setMessage(
            Message.valueOf(
                OMRatisHelper.convertRequestToByteString(omRequest)))
        .setType(RaftClientRequest.writeRequestType())
        .build();
  }

}
