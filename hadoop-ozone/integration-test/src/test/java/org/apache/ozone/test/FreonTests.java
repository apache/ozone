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

package org.apache.ozone.test;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.ozone.ClientConfigForTesting;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;

/**
 * Group tests that write more data than usual (e.g. Freon tests).  These are
 * separated from {@link NonHATests}, because they tend to take a bit longer.
 * This keeps {@link NonHATests} leaner.
 * <p/>
 * Specific tests are implemented in separate classes, and they are subclasses
 * here as {@link Nested} inner classes.  This allows running all tests in the
 * same cluster.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class FreonTests extends ClusterForTests<MiniOzoneCluster> {

  @Override
  protected OzoneConfiguration createOzoneConfig() {
    OzoneConfiguration conf = super.createOzoneConfig();
    ClientConfigForTesting.newBuilder(StorageUnit.MB)
        .setChunkSize(4)
        .setBlockSize(256)
        .applyTo(conf);
    return conf;
  }

  @Nested
  class DNRPCLoadGenerator extends org.apache.hadoop.ozone.freon.TestDNRPCLoadGenerator {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class HadoopDirTreeGenerator extends org.apache.hadoop.ozone.freon.TestHadoopDirTreeGenerator {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class HadoopNestedDirGenerator extends org.apache.hadoop.ozone.freon.TestHadoopNestedDirGenerator {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class HsyncGenerator extends org.apache.hadoop.ozone.freon.TestHsyncGenerator {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class OmBucketReadWriteFileOps extends org.apache.hadoop.ozone.freon.TestOmBucketReadWriteFileOps {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class OmBucketReadWriteKeyOps extends org.apache.hadoop.ozone.freon.TestOmBucketReadWriteKeyOps {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class RandomKeyGenerator extends org.apache.hadoop.ozone.freon.TestRandomKeyGenerator {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }
}
