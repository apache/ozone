/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.api.types;

import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.api.BucketHandler;
import org.apache.hadoop.ozone.recon.api.EntityHandler;
import org.apache.hadoop.ozone.recon.api.RootEntityHandler;
import org.apache.hadoop.ozone.recon.api.VolumeEntityHandler;
import org.apache.hadoop.ozone.recon.api.DirectoryEntityHandler;
import org.apache.hadoop.ozone.recon.api.KeyEntityHandler;
import org.apache.hadoop.ozone.recon.api.BucketEntityHandler;
import org.apache.hadoop.ozone.recon.api.UnknownEntityHandler;

import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Enum class for namespace type.
 */
public enum EntityType {
  ROOT {
    public EntityHandler create(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager omMetadataManager,
      OzoneStorageContainerManager reconSCM,
      BucketHandler bucketHandler){
      return new RootEntityHandler(reconNamespaceSummaryManager, omMetadataManager,
                               reconSCM);
    }
  },
  VOLUME {
    public EntityHandler create(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager omMetadataManager,
      OzoneStorageContainerManager reconSCM,
      BucketHandler bucketHandler){
      return new VolumeEntityHandler(reconNamespaceSummaryManager, omMetadataManager,
                               reconSCM);
    }
  },
  BUCKET {
    public EntityHandler create(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager omMetadataManager,
      OzoneStorageContainerManager reconSCM,
      BucketHandler bucketHandler){
      return new BucketEntityHandler(reconNamespaceSummaryManager, omMetadataManager,
                                 reconSCM, bucketHandler);
    }
  },
  DIRECTORY {
    public EntityHandler create(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager omMetadataManager,
      OzoneStorageContainerManager reconSCM,
      BucketHandler bucketHandler){
      return new DirectoryEntityHandler(reconNamespaceSummaryManager, omMetadataManager,
                                 reconSCM, bucketHandler);
    }
  },
  KEY {
    public EntityHandler create(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager omMetadataManager,
      OzoneStorageContainerManager reconSCM,
      BucketHandler bucketHandler){
      return new KeyEntityHandler(reconNamespaceSummaryManager, omMetadataManager,
                                 reconSCM, bucketHandler);
    }
  },
  UNKNOWN { // if path is invalid
    public EntityHandler create(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager omMetadataManager,
      OzoneStorageContainerManager reconSCM,
      BucketHandler bucketHandler){
      return new UnknownEntityHandler(reconNamespaceSummaryManager, omMetadataManager,
                               reconSCM);
    }
  };

  abstract public EntityHandler create(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager omMetadataManager,
      OzoneStorageContainerManager reconSCM,
      BucketHandler bucketHandler);
}
