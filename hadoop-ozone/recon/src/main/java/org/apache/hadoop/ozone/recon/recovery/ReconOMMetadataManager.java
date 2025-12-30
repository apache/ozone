/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.recovery;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.api.types.ReconBasicOmKeyInfo;

/**
 * Interface for the OM Metadata Manager + DB store maintained by
 * Recon.
 */
public interface ReconOMMetadataManager extends OMMetadataManager {

  /**
   * Refresh the DB instance to point to a new location. Get rid of the old
   * DB instance.
   *
   * @param dbLocation      New location of the OM Snapshot DB.
   * @param addCacheMetrics
   */
  void updateOmDB(File dbLocation, boolean addCacheMetrics) throws IOException;

  /**
   * Get the most recent sequence number from the Ozone Manager Metadata
   * Database.
   */
  long getLastSequenceNumberFromDB();

  /**
   * Check if OM tables are initialized.
   * @return true if OM Tables are initialized, otherwise false.
   */
  boolean isOmTablesInitialized();

  /**
   * Return a list of volumes based on the specified start volume and maximum
   * number of volumes returned.
   *
   * This method can be optimized by using username as a filter.
   *
   * @param startKey the start volume name determines where to start listing
   * from, this key is excluded from the result. Returns all the volumes if
   * it's null.
   * @param maxKeys the maximum number of volumes to return.
   * @return volumes with starting from <code>startKey</code> limited by
   *         <code>maxKeys</code>
   */
  List<OmVolumeArgs> listVolumes(String startKey,
                                 int maxKeys) throws IOException;

  /**
   * Return all volumes in the file system.
   * @return all the volumes from the OM DB.
   */
  List<OmVolumeArgs> listVolumes() throws IOException;

  /**
   * Check if volume exists in the OM table.
   * @param volName volume name without any protocol prefix.
   * @return true if volume exists, otherwise false.
   * @throws IOException IOE
   */
  boolean volumeExists(String volName) throws IOException;

  /**
   * Returns a list of buckets represented by {@link OmBucketInfo} in the given
   * volume.
   *
   * @param volumeName the name of the volume. If volumeName is empty, list
   * all buckets in the cluster.
   * @param startBucket the start bucket name. Only the buckets whose name is
   * after this value will be included in the result. This key is excluded from
   * the result.
   * @param maxNumOfBuckets the maximum number of buckets to return. It ensures
   * the size of the result will not exceed this limit.
   * @return a list of buckets.
   * @throws IOException
   */
  List<OmBucketInfo> listBucketsUnderVolume(String volumeName,
      String startBucket, int maxNumOfBuckets) throws IOException;

  /**
   * List all buckets under a volume.
   * @param volumeName volume name without protocol prefix.
   * @return buckets under volume or all buckets if volume is null.
   */
  List<OmBucketInfo> listBucketsUnderVolume(
      String volumeName) throws IOException;

  /**
   * Return the OzoneConfiguration instance used by Recon.
   * @return OzoneConfiguration
   */
  OzoneConfiguration getOzoneConfiguration();

  /**
   * A lighter weight version of the getKeyTable method that only returns the ReconBasicOmKeyInfo wrapper object. This
   * avoids creating a full OMKeyInfo object for each key if it is not needed.
   * @param bucketLayout The Bucket layout to use for the key table.
   * @return A table of keys and their metadata.
   * @throws IOException
   */
  Table<String, ReconBasicOmKeyInfo> getKeyTableBasic(BucketLayout bucketLayout) throws IOException;

  /**
   * Create a ReconOMMetadataManager instance given an OM DB checkpoint.
   * @param conf - OzoneConfiguration
   * @param checkpoint - DBCheckpoint
   * @return ReconOMMetadataManager instance
   * @throws IOException
   */
  ReconOMMetadataManager createCheckpointReconMetadataManager(
      OzoneConfiguration conf, DBCheckpoint checkpoint) throws IOException;

}
