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

package org.apache.hadoop.ozone.om;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OmSnapshotLocalData.VersionMeta;
import org.apache.ozone.rocksdb.util.SstFileInfo;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.introspector.BeanAccess;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Represent;
import org.yaml.snakeyaml.representer.Representer;

/**
 * Class for creating and reading snapshot local properties / data YAML files.
 * Checksum of the YAML fields are computed and stored in the YAML file transparently to callers.
 * Inspired by org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml
 */
public final class OmSnapshotLocalDataYaml {

  public static final Tag SNAPSHOT_YAML_TAG = new Tag("OmSnapshotLocalData");
  public static final Tag SNAPSHOT_VERSION_META_TAG = new Tag("VersionMeta");
  public static final Tag SST_FILE_INFO_TAG = new Tag("SstFileInfo");
  public static final String YAML_FILE_EXTENSION = ".yaml";

  private OmSnapshotLocalDataYaml() {
  }

  /**
   * Representer class to define which fields need to be stored in yaml file.
   */
  private static class OmSnapshotLocalDataRepresenter extends Representer {

    OmSnapshotLocalDataRepresenter(DumperOptions options) {
      super(options);
      this.addClassTag(OmSnapshotLocalData.class, SNAPSHOT_YAML_TAG);
      this.addClassTag(VersionMeta.class, SNAPSHOT_VERSION_META_TAG);
      this.addClassTag(SstFileInfo.class, SST_FILE_INFO_TAG);
      representers.put(SstFileInfo.class, new RepresentSstFileInfo());
      representers.put(VersionMeta.class, new RepresentVersionMeta());
      representers.put(TransactionInfo.class, data -> new ScalarNode(Tag.STR, data.toString(), null, null,
          DumperOptions.ScalarStyle.PLAIN));
      representers.put(UUID.class, data ->
          new ScalarNode(Tag.STR, data.toString(), null, null, DumperOptions.ScalarStyle.PLAIN));
    }

    private class RepresentSstFileInfo implements Represent {
      @Override
      public Node representData(Object data) {
        SstFileInfo info = (SstFileInfo) data;
        Map<String, Object> map = new java.util.LinkedHashMap<>();
        map.put(OzoneConsts.OM_SST_FILE_INFO_FILE_NAME, info.getFileName());
        map.put(OzoneConsts.OM_SST_FILE_INFO_START_KEY, info.getStartKey());
        map.put(OzoneConsts.OM_SST_FILE_INFO_END_KEY, info.getEndKey());
        map.put(OzoneConsts.OM_SST_FILE_INFO_COL_FAMILY, info.getColumnFamily());

        // Explicitly create a mapping node with the desired tag
        return representMapping(SST_FILE_INFO_TAG, map, DumperOptions.FlowStyle.BLOCK);
      }
    }

    // New inner class for VersionMeta
    private class RepresentVersionMeta implements Represent {
      @Override
      public Node representData(Object data) {
        VersionMeta meta = (VersionMeta) data;
        Map<String, Object> map = new java.util.LinkedHashMap<>();
        map.put(OzoneConsts.OM_SLD_VERSION_META_PREV_SNAP_VERSION, meta.getPreviousSnapshotVersion());
        map.put(OzoneConsts.OM_SLD_VERSION_META_SST_FILES, meta.getSstFiles());

        return representMapping(SNAPSHOT_VERSION_META_TAG, map, DumperOptions.FlowStyle.BLOCK);
      }
    }


    /**
     * Omit properties with null value.
     */
    @Override
    protected NodeTuple representJavaBeanProperty(
        Object bean, Property property, Object value, Tag tag) {
      return value == null
          ? null
          : super.representJavaBeanProperty(bean, property, value, tag);
    }
  }

  /**
   * Constructor class for OmSnapshotLocalData.
   * This is used when parsing YAML files into OmSnapshotLocalDataYaml objects.
   */
  private static class SnapshotLocalDataConstructor extends SafeConstructor {
    SnapshotLocalDataConstructor() {
      super(new LoaderOptions());
      //Adding our own specific constructors for tags.
      this.yamlConstructors.put(SNAPSHOT_YAML_TAG, new ConstructSnapshotLocalData());
      this.yamlConstructors.put(SNAPSHOT_VERSION_META_TAG, new ConstructVersionMeta());
      this.yamlConstructors.put(SST_FILE_INFO_TAG, new ConstructSstFileInfo());
      TypeDescription omDesc = new TypeDescription(OmSnapshotLocalData.class);
      omDesc.putMapPropertyType(OzoneConsts.OM_SLD_VERSION_SST_FILE_INFO, Integer.class, VersionMeta.class);
      this.addTypeDescription(omDesc);
      TypeDescription versionMetaDesc = new TypeDescription(VersionMeta.class);
      versionMetaDesc.putListPropertyType(OzoneConsts.OM_SLD_VERSION_META_SST_FILES, SstFileInfo.class);
      this.addTypeDescription(versionMetaDesc);
    }

    private final class ConstructSstFileInfo extends AbstractConstruct {
      @Override
      public Object construct(Node node) {
        MappingNode mnode = (MappingNode) node;
        Map<Object, Object> nodes = constructMapping(mnode);
        return new SstFileInfo((String) nodes.get(OzoneConsts.OM_SST_FILE_INFO_FILE_NAME),
            (String) nodes.get(OzoneConsts.OM_SST_FILE_INFO_START_KEY),
            (String) nodes.get(OzoneConsts.OM_SST_FILE_INFO_END_KEY),
            (String) nodes.get(OzoneConsts.OM_SST_FILE_INFO_COL_FAMILY));
      }
    }

    private final class ConstructVersionMeta extends AbstractConstruct {

      @Override
      public Object construct(Node node) {
        MappingNode mnode = (MappingNode) node;
        Map<Object, Object> nodes = constructMapping(mnode);
        return new VersionMeta((Integer) nodes.get(OzoneConsts.OM_SLD_VERSION_META_PREV_SNAP_VERSION),
            (List<SstFileInfo>) nodes.get(OzoneConsts.OM_SLD_VERSION_META_SST_FILES));
      }
    }

    private final class ConstructSnapshotLocalData extends AbstractConstruct {
      @SuppressWarnings("unchecked")
      @Override
      public Object construct(Node node) {
        MappingNode mnode = (MappingNode) node;
        Map<Object, Object> nodes = constructMapping(mnode);
        final String snapIdStr = (String) nodes.get(OzoneConsts.OM_SLD_SNAP_ID);
        UUID snapId = UUID.fromString(snapIdStr);
        final String prevSnapIdStr = (String) nodes.get(OzoneConsts.OM_SLD_PREV_SNAP_ID);
        UUID prevSnapId = prevSnapIdStr != null ? UUID.fromString(prevSnapIdStr) : null;
        final String purgeTxInfoStr = (String) nodes.get(OzoneConsts.OM_SLD_TXN_INFO);
        final long dbTxnSeqNumber = ((Number)nodes.get(OzoneConsts.OM_SLD_DB_TXN_SEQ_NUMBER)).longValue();
        TransactionInfo transactionInfo = purgeTxInfoStr != null ? TransactionInfo.valueOf(purgeTxInfoStr) : null;
        OmSnapshotLocalData snapshotLocalData = new OmSnapshotLocalData(snapId, Collections.emptyList(), prevSnapId,
            transactionInfo, dbTxnSeqNumber);

        // Set version from YAML
        Integer version = (Integer) nodes.get(OzoneConsts.OM_SLD_VERSION);
        snapshotLocalData.setVersion(version);

        // Set other fields from parsed YAML
        snapshotLocalData.setSstFiltered((Boolean) nodes.getOrDefault(OzoneConsts.OM_SLD_IS_SST_FILTERED, false));

        // Handle potential Integer/Long type mismatch from YAML parsing
        Object lastDefragTimeObj = nodes.getOrDefault(OzoneConsts.OM_SLD_LAST_DEFRAG_TIME, -1L);
        long lastDefragTime;
        if (lastDefragTimeObj instanceof Number) {
          lastDefragTime = ((Number) lastDefragTimeObj).longValue();
        } else {
          throw new IllegalArgumentException("Invalid type for lastDefragTime: " +
              lastDefragTimeObj.getClass().getName() + ". Expected Number type.");
        }
        snapshotLocalData.setLastDefragTime(lastDefragTime);

        snapshotLocalData.setNeedsDefrag((Boolean) nodes.getOrDefault(OzoneConsts.OM_SLD_NEEDS_DEFRAG, false));
        Map<Integer, VersionMeta> versionMetaMap =
            (Map<Integer, VersionMeta>) nodes.get(OzoneConsts.OM_SLD_VERSION_SST_FILE_INFO);
        if (versionMetaMap != null) {
          snapshotLocalData.setVersionSstFileInfos(versionMetaMap);
        }

        String checksum = (String) nodes.get(OzoneConsts.OM_SLD_CHECKSUM);
        if (checksum != null) {
          snapshotLocalData.setChecksum(checksum);
        }

        return snapshotLocalData;
      }
    }
  }

  /**
   * Factory class for constructing and pooling instances of the Yaml object.
   * This class extends BasePooledObjectFactory to support object pooling,
   * minimizing the expense of repeatedly creating and destroying Yaml instances.
   *
   * The Yaml instances created by this factory are customized to use a specific
   * set of property and serialization/deserialization configurations.
   * - BeanAccess is configured to access fields directly, allowing manipulation
   *   of private fields in objects.
   * - The PropertyUtils allows read-only properties to be accessed.
   * - Custom Representer and Constructor classes tailored to the OmSnapshotLocalData
   *   data structure are employed to customize how objects are represented in YAML.
   *
   * This class provides thread-safe pooling and management of Yaml instances,
   * ensuring efficient resource usage in high-concurrency environments.
   */
  public static class YamlFactory extends BasePooledObjectFactory<Yaml> {

    @Override
    public Yaml create() {
      PropertyUtils propertyUtils = new PropertyUtils();
      propertyUtils.setBeanAccess(BeanAccess.FIELD);
      propertyUtils.setAllowReadOnlyProperties(true);
      DumperOptions options = new DumperOptions();
      Representer representer = new OmSnapshotLocalDataRepresenter(options);
      representer.setPropertyUtils(propertyUtils);
      SafeConstructor snapshotDataConstructor = new SnapshotLocalDataConstructor();
      return new Yaml(snapshotDataConstructor, representer);
    }

    @Override
    public PooledObject<Yaml> wrap(Yaml yaml) {
      return new DefaultPooledObject<>(yaml);
    }
  }
}
