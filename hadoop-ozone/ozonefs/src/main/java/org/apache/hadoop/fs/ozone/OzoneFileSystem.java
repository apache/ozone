/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.ozone;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

import com.google.common.base.Strings;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderTokenIssuer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LeaseRecoverable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.SafeMode;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.LeaseKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.security.token.DelegationTokenIssuer;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_NOT_FOUND;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.NO_SUCH_BLOCK;
import static org.apache.hadoop.ozone.OzoneConsts.FORCE_LEASE_RECOVERY_ENV;

/**
 * The Ozone Filesystem implementation.
 * <p>
 * This subclass is marked as private as code should not be creating it
 * directly; use {@link FileSystem#get(org.apache.hadoop.conf.Configuration)}
 * and variants to create one. If cast to {@link OzoneFileSystem}, extra
 * methods and features may be accessed. Consider those private and unstable.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class OzoneFileSystem extends BasicOzoneFileSystem
    implements KeyProviderTokenIssuer, LeaseRecoverable, SafeMode {

  private OzoneFSStorageStatistics storageStatistics;
  private boolean forceRecovery;

  public OzoneFileSystem() {
    this.storageStatistics = new OzoneFSStorageStatistics();
    String force = System.getProperty(FORCE_LEASE_RECOVERY_ENV);
    forceRecovery = Strings.isNullOrEmpty(force) ? false : Boolean.parseBoolean(force);
  }

  @Override
  public KeyProvider getKeyProvider() throws IOException {
    return getAdapter().getKeyProvider();
  }

  @Override
  public URI getKeyProviderUri() throws IOException {
    return getAdapter().getKeyProviderUri();
  }

  @Override
  public DelegationTokenIssuer[] getAdditionalTokenIssuers()
      throws IOException {
    KeyProvider keyProvider;
    try {
      keyProvider = getKeyProvider();
    } catch (IOException ioe) {
      LOG.debug("Error retrieving KeyProvider.", ioe);
      return null;
    }
    if (keyProvider instanceof DelegationTokenIssuer) {
      return new DelegationTokenIssuer[]{(DelegationTokenIssuer)keyProvider};
    }
    return null;
  }

  StorageStatistics getOzoneFSOpsCountStatistics() {
    return storageStatistics;
  }

  @Override
  protected void incrementCounter(Statistic statistic, long count) {
    if (storageStatistics != null) {
      storageStatistics.incrementCounter(statistic, count);
    }
  }

  @Override
  protected OzoneClientAdapter createAdapter(ConfigurationSource conf,
      String bucketStr, String volumeStr, String omHost, int omPort)
      throws IOException {
    return new OzoneClientAdapterImpl(omHost, omPort, conf, volumeStr,
        bucketStr,
        storageStatistics);
  }

  @Override
  protected InputStream createFSInputStream(InputStream inputStream) {
    return new CapableOzoneFSInputStream(inputStream, statistics);
  }

  @Override
  protected OzoneFSOutputStream createFSOutputStream(
          OzoneFSOutputStream outputStream) {
    return new CapableOzoneFSOutputStream(outputStream, isHsyncEnabled());
  }

  @Override
  public boolean hasPathCapability(final Path path, final String capability)
      throws IOException {
    // qualify the path to make sure that it refers to the current FS.
    final Path p = makeQualified(path);
    boolean cap =
        OzonePathCapabilities.hasPathCapability(p, capability);
    if (cap) {
      return cap;
    }
    return super.hasPathCapability(p, capability);
  }

  @Override
  public boolean recoverLease(Path f) throws IOException {
    statistics.incrementWriteOps(1);
    LOG.trace("recoverLease() path:{}", f);

    Path qualifiedPath = makeQualified(f);
    String key = pathToKey(qualifiedPath);
    LeaseKeyInfo leaseKeyInfo;
    try {
      leaseKeyInfo = getAdapter().recoverFilePrepare(key, forceRecovery);
    } catch (OMException e) {
      if (e.getResult() == OMException.ResultCodes.KEY_ALREADY_CLOSED) {
        // key is already closed, let's just return success
        return true;
      }
      throw e;
    }

    OmKeyLocationInfoGroup keyLatestVersionLocations = leaseKeyInfo.getKeyInfo().getLatestVersionLocations();
    List<OmKeyLocationInfo> keyLocationInfoList = keyLatestVersionLocations.getLocationList();
    OmKeyLocationInfoGroup openKeyLatestVersionLocations = leaseKeyInfo.getOpenKeyInfo().getLatestVersionLocations();
    List<OmKeyLocationInfo> openKeyLocationInfoList = openKeyLatestVersionLocations.getLocationList();

    int openKeyLocationSize = openKeyLocationInfoList.size();
    int keyLocationSize = keyLocationInfoList.size();
    OmKeyLocationInfo openKeyFinalBlock = null;
    OmKeyLocationInfo openKeyPenultimateBlock = null;
    OmKeyLocationInfo keyFinalBlock;

    if (keyLocationSize > 0) {
      // Block info from fileTable
      keyFinalBlock = keyLocationInfoList.get(keyLocationSize - 1);
      // Block info from openFileTable
      if (openKeyLocationSize > 1) {
        openKeyFinalBlock = openKeyLocationInfoList.get(openKeyLocationSize - 1);
        openKeyPenultimateBlock = openKeyLocationInfoList.get(openKeyLocationSize - 2);
      } else if (openKeyLocationSize > 0) {
        openKeyFinalBlock = openKeyLocationInfoList.get(0);
      }
      // Finalize the final block and get block length
      try {
        // CASE 1: When openFileTable has more block than fileTable
        // Try to finalize last block of openFileTable
        // Add that block into fileTable locationInfo
        if (openKeyLocationSize > keyLocationSize) {
          openKeyFinalBlock.setLength(getAdapter().finalizeBlock(openKeyFinalBlock));
          keyLocationInfoList.add(openKeyFinalBlock);
        }
        // CASE 2: When openFileTable penultimate block length is not equal to fileTable block length of last block
        // Finalize and get the actual block length and update in fileTable last block
        if ((openKeyPenultimateBlock != null && keyFinalBlock != null) &&
            openKeyPenultimateBlock.getLength() != keyFinalBlock.getLength() &&
            openKeyPenultimateBlock.getBlockID().getLocalID() == keyFinalBlock.getBlockID().getLocalID()) {
          keyFinalBlock.setLength(getAdapter().finalizeBlock(keyFinalBlock));
        }
        // CASE 3: When openFileTable has same number of blocks as fileTable
        // Finalize and get actual length of fileTable final block
        if (keyLocationInfoList.size() == openKeyLocationInfoList.size() && keyFinalBlock != null) {
          keyFinalBlock.setLength(getAdapter().finalizeBlock(keyFinalBlock));
        }
      } catch (Throwable e) {
        if (e instanceof StorageContainerException && (((StorageContainerException) e).getResult().equals(NO_SUCH_BLOCK)
            || ((StorageContainerException) e).getResult().equals(CONTAINER_NOT_FOUND))
            && openKeyPenultimateBlock != null && keyFinalBlock != null &&
            openKeyPenultimateBlock.getBlockID().getLocalID() == keyFinalBlock.getBlockID().getLocalID()) {
          try {
            keyFinalBlock.setLength(getAdapter().finalizeBlock(keyFinalBlock));
          } catch (Throwable exp) {
            if (!forceRecovery) {
              throw exp;
            }
            LOG.warn("Failed to finalize block. Continue to recover the file since {} is enabled.",
                FORCE_LEASE_RECOVERY_ENV, exp);
          }
        } else if (!forceRecovery) {
          throw e;
        } else {
          LOG.warn("Failed to finalize block. Continue to recover the file since {} is enabled.",
              FORCE_LEASE_RECOVERY_ENV, e);
        }
      }
    }

    // recover and commit file
    long keyLength = keyLocationInfoList.stream().mapToLong(OmKeyLocationInfo::getLength).sum();
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(leaseKeyInfo.getKeyInfo().getVolumeName())
        .setBucketName(leaseKeyInfo.getKeyInfo().getBucketName()).setKeyName(leaseKeyInfo.getKeyInfo().getKeyName())
        .setReplicationConfig(leaseKeyInfo.getKeyInfo().getReplicationConfig()).setDataSize(keyLength)
        .setLocationInfoList(keyLocationInfoList)
        .build();
    getAdapter().recoverFile(keyArgs);
    return true;
  }

  @Override
  public boolean isFileClosed(Path f) throws IOException {
    LOG.trace("isFileClosed() path:{}", f);
    Path qualifiedPath = makeQualified(f);
    String key = pathToKey(qualifiedPath);
    return getAdapter().isFileClosed(key);
  }

  @Override
  public boolean setSafeMode(SafeModeAction action, boolean isChecked)
      throws IOException {
    return setSafeModeUtil(action, isChecked);
  }
}
