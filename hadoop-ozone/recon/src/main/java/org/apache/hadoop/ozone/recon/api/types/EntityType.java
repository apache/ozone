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
import org.apache.hadoop.ozone.recon.api.handlers.BucketHandler;
import org.apache.hadoop.ozone.recon.api.handlers.EntityHandler;
import org.apache.hadoop.ozone.recon.api.handlers.RootEntityHandler;
import org.apache.hadoop.ozone.recon.api.handlers.VolumeEntityHandler;
import org.apache.hadoop.ozone.recon.api.handlers.DirectoryEntityHandler;
import org.apache.hadoop.ozone.recon.api.handlers.KeyEntityHandler;
import org.apache.hadoop.ozone.recon.api.handlers.BucketEntityHandler;
import org.apache.hadoop.ozone.recon.api.handlers.UnknownEntityHandler;

import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

/**
 * Enum class for namespace type.
 */
public enum EntityType {
  ROOT {
    public EntityHandler create(
        ReconNamespaceSummaryManager reconNamespaceSummaryManager,
        ReconOMMetadataManager omMetadataManager,
        OzoneStorageContainerManager reconSCM,
        BucketHandler bucketHandler, String path) {
      return new RootEntityHandler(reconNamespaceSummaryManager,
              omMetadataManager, reconSCM, path);
    }
  },
  VOLUME {
    public EntityHandler create(
        ReconNamespaceSummaryManager reconNamespaceSummaryManager,
        ReconOMMetadataManager omMetadataManager,
        OzoneStorageContainerManager reconSCM,
        BucketHandler bucketHandler, String path) {
      return new VolumeEntityHandler(reconNamespaceSummaryManager,
              omMetadataManager, reconSCM, path);
    }
  },
  BUCKET {
    public EntityHandler create(
        ReconNamespaceSummaryManager reconNamespaceSummaryManager,
        ReconOMMetadataManager omMetadataManager,
        OzoneStorageContainerManager reconSCM,
        BucketHandler bucketHandler, String path) {
      return new BucketEntityHandler(reconNamespaceSummaryManager,
              omMetadataManager, reconSCM, bucketHandler, path);
    }
  },
  DIRECTORY {
    public EntityHandler create(
        ReconNamespaceSummaryManager reconNamespaceSummaryManager,
        ReconOMMetadataManager omMetadataManager,
        OzoneStorageContainerManager reconSCM,
        BucketHandler bucketHandler, String path) {
      return new DirectoryEntityHandler(reconNamespaceSummaryManager,
              omMetadataManager, reconSCM, bucketHandler, path);
    }
  },
  KEY {
    public EntityHandler create(
        ReconNamespaceSummaryManager reconNamespaceSummaryManager,
        ReconOMMetadataManager omMetadataManager,
        OzoneStorageContainerManager reconSCM,
        BucketHandler bucketHandler, String path) {
      return new KeyEntityHandler(reconNamespaceSummaryManager,
              omMetadataManager, reconSCM, bucketHandler, path);
    }
  },
  UNKNOWN { // if path is invalid
    public EntityHandler create(
        ReconNamespaceSummaryManager reconNamespaceSummaryManager,
        ReconOMMetadataManager omMetadataManager,
        OzoneStorageContainerManager reconSCM,
        BucketHandler bucketHandler, String path) {
      return new UnknownEntityHandler(reconNamespaceSummaryManager,
              omMetadataManager, reconSCM);
    }
  };

  public abstract EntityHandler create(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager omMetadataManager,
      OzoneStorageContainerManager reconSCM,
      BucketHandler bucketHandler, String path);
}
