/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.keyvalue;

import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerLocationUtil;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters.KeyPrefixFilter;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;


/**
 * Block Iterator for KeyValue Container. This block iterator returns blocks
 * which match with the {@link MetadataKeyFilters.KeyPrefixFilter}. If no
 * filter is specified, then default filter used is
 * {@link MetadataKeyFilters#getNormalKeyFilter()}
 */
@InterfaceAudience.Public
public class KeyValueBlockIterator implements BlockIterator<BlockData>,
    Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(
      KeyValueBlockIterator.class);

  private TableIterator<String, ? extends Table.KeyValue<String, BlockData>> blockIterator;
  private final ReferenceCountedDB db;
  private static KeyPrefixFilter defaultBlockFilter = MetadataKeyFilters
      .getNormalKeyFilter();
  private KeyPrefixFilter blockFilter;
  private BlockData nextBlock;
  private long containerId;
  // If true, indicates that the internal iterator position was moved using
  // seek, and our queued up default block is no longer valid.
  private boolean wasReset;

  /**
   * KeyValueBlockIterator to iterate blocks in a container.
   * @param id - container id
   * @param path -  container base path
   * @throws IOException
   */

  public KeyValueBlockIterator(long id, File path)
      throws IOException {
    this(id, path, defaultBlockFilter);
  }

  /**
   * KeyValueBlockIterator to iterate blocks in a container.
   * @param id - container id
   * @param path - container base path
   * @param filter - Block filter, filter to be applied for blocks
   * @throws IOException
   */
  public KeyValueBlockIterator(long id, File path, KeyPrefixFilter filter)
      throws IOException {
    containerId = id;
    File metdataPath = new File(path, OzoneConsts.METADATA);
    File containerFile = ContainerUtils.getContainerFile(metdataPath
        .getParentFile());
    ContainerData containerData = ContainerDataYaml.readContainerFile(
        containerFile);
    KeyValueContainerData keyValueContainerData = (KeyValueContainerData)
        containerData;
    keyValueContainerData.setDbFile(KeyValueContainerLocationUtil
        .getContainerDBFile(metdataPath, containerId));
    db = BlockUtils.getDB(keyValueContainerData, new
        OzoneConfiguration());
    blockIterator = db.getStore().getBlockDataTable().iterator();
    blockFilter = filter;
    wasReset = true;
  }

  /**
   * This method returns blocks matching with the filter.
   * @return next block or null if no more blocks
   * @throws IOException
   */
  @Override
  public BlockData nextBlock() throws IOException, NoSuchElementException {
    if (wasReset) {
      nextBlock = findNextBlock();
      wasReset = false;
    }

    if (nextBlock == null) {
      throw new NoSuchElementException("Block Iterator reached end for " +
              "ContainerID " + containerId);
    }

    BlockData result = nextBlock;
    nextBlock = findNextBlock();

    return result;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (wasReset) {
      nextBlock = findNextBlock();
      wasReset = false;
    }

    return (nextBlock != null);
  }

  @Override
  public void seekToFirst() {
    // Cannot call findNextBlock() here, as the IOException would break the
    // interface.
    blockIterator.seekToFirst();
    wasReset = true;
  }

  public void close() {
    db.close();
  }

  /**
   * @return The next block in the iterator that passes the filter.
   * @throws IOException
   */
  private BlockData findNextBlock() throws IOException {
    BlockData foundBlock = null;

    while (blockIterator.hasNext() && foundBlock == null) {
      KeyValue blockKV = blockIterator.next();
      if (blockFilter.filterKey(null, blockKV.getKey(), null)) {
        foundBlock = BlockUtils.getBlockData(blockKV.getValue());
        if (LOG.isTraceEnabled()) {
          LOG.trace("Block matching with filter found: blockID is : {} for " +
                  "containerID {}", foundBlock.getLocalID(), containerId);
        }
      }
    }

    return foundBlock;
  }
}
