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

package org.apache.hadoop.hdds.conf;

import static java.util.Collections.unmodifiableSortedSet;
import static java.util.stream.Collectors.toCollection;
import static org.apache.hadoop.hdds.ratis.RatisHelper.HDDS_DATANODE_RATIS_PREFIX_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CONTAINER_COPY_WORKDIR;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.slf4j.Logger;

/**
 * Configuration for ozone.
 */
@InterfaceAudience.Private
public class OzoneConfiguration extends Configuration implements MutableConfigurationSource {

  public static final SortedSet<String> TAGS = unmodifiableSortedSet(
      Arrays.stream(ConfigTag.values())
          .map(Enum::name)
          .collect(toCollection(TreeSet::new)));

  static {
    addDeprecatedKeys();

    activate();
  }

  private Properties delegatingProps;

  public static OzoneConfiguration of(ConfigurationSource source) {
    if (source instanceof LegacyHadoopConfigurationSource) {
      return new OzoneConfiguration(((LegacyHadoopConfigurationSource) source)
          .getOriginalHadoopConfiguration());
    }
    return (OzoneConfiguration) source;
  }

  public static OzoneConfiguration of(OzoneConfiguration source) {
    return source;
  }

  public static OzoneConfiguration of(Configuration conf) {
    Objects.requireNonNull(conf, "conf == null");

    return conf instanceof OzoneConfiguration
        ? (OzoneConfiguration) conf
        : new OzoneConfiguration(conf);
  }

  /**
   * @return a new config object of type {@code T} configured with defaults
   * and any overrides from XML
   */
  public static <T> T newInstanceOf(Class<T> configurationClass) {
    OzoneConfiguration conf = new OzoneConfiguration();
    return conf.getObject(configurationClass);
  }

  public OzoneConfiguration() {
  }

  public OzoneConfiguration(Configuration conf) {
    super(conf);
  }

  public List<Property> readPropertyFromXml(URL url) throws JAXBException {
    JAXBContext context = JAXBContext.newInstance(XMLConfiguration.class);
    Unmarshaller um = context.createUnmarshaller();

    XMLConfiguration config = (XMLConfiguration) um.unmarshal(url);
    return config.getProperties();
  }

  /**
   * Class to marshall/un-marshall configuration from xml files.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "configuration")
  public static class XMLConfiguration {

    @XmlElement(name = "property", type = Property.class)
    private List<Property> properties = new ArrayList<>();

    public XMLConfiguration() {
    }

    public XMLConfiguration(List<Property> properties) {
      this.properties = new ArrayList<>(properties);
    }

    public List<Property> getProperties() {
      return Collections.unmodifiableList(properties);
    }
  }

  /**
   * Class to marshall/un-marshall configuration properties from xml files.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "property")
  public static class Property implements Comparable<Property> {

    private String name;
    private String value;
    private String tag;
    private String description;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }

    public String getTag() {
      return tag;
    }

    public void setTag(String tag) {
      this.tag = tag;
    }

    public String getDescription() {
      return description;
    }

    public void setDescription(String description) {
      this.description = description;
    }

    @Override
    public int compareTo(Property o) {
      if (this == o) {
        return 0;
      }
      return this.getName().compareTo(o.getName());
    }

    @Override
    public String toString() {
      return this.getName() + " " + this.getValue() + " " + this.getTag();
    }

    @Override
    public int hashCode() {
      return this.getName().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      return (obj instanceof Property) && (((Property) obj).getName())
          .equals(this.getName());
    }
  }

  public static List<String> getConfigurationResourceFiles() {
    List<String> resourceFiles = new ArrayList<>();

    // even though core-default and core-site are added by the parent Configuration class,
    // we add it here for them to be a part of the resourceFiles list.
    // addDefaultResource is idempotent so any duplicate items in this list will be handled accordingly
    resourceFiles.add("hdfs-default.xml");
    resourceFiles.add("hdfs-site.xml");
    resourceFiles.add("core-default.xml");
    resourceFiles.add("core-site.xml");

    // Modules with @Config annotations.  If new one is introduced, add it to this list.
    String[] modules = new String[] {
        "hdds-common",
        "hdds-client",
        "hdds-container-service",
        "hdds-server-framework",
        "hdds-server-scm",
        "ozone-common",
        "ozone-csi",
        "ozone-manager",
        "ozone-recon",
    };
    for (String module : modules) {
      resourceFiles.add(module + "-default.xml");
    }

    // Non-generated configs
    resourceFiles.add("ozone-default.xml");
    resourceFiles.add("ozone-site.xml");

    return resourceFiles;
  }

  /** Add default resources. */
  public static void activate() {
    for (String resourceFile : getConfigurationResourceFiles()) {
      addDefaultResource(resourceFile);
    }
  }

  /**
   * The super class method getAllPropertiesByTag
   * does not override values of properties
   * if there is no tag present in the configs of
   * newly added resources.
   *
   * @param tag
   * @return Properties that belong to the tag
   */
  @Override
  public Properties getAllPropertiesByTag(String tag) {
    // Call getProps first to load the newly added resources
    // before calling super.getAllPropertiesByTag
    Properties updatedProps = getProps();
    Properties propertiesByTag = super.getAllPropertiesByTag(tag);
    Properties props = new Properties();
    Enumeration properties = propertiesByTag.propertyNames();
    while (properties.hasMoreElements()) {
      Object propertyName = properties.nextElement();
      // get the current value of the property
      Object value = updatedProps.getProperty(propertyName.toString());
      if (value != null) {
        props.put(propertyName, value);
      }
    }
    return props;
  }

  public Map<String, String> getOzoneProperties() {
    String ozoneRegex = ".*(ozone|hdds|ratis|container|scm|recon)\\..*";
    return getValByRegex(ozoneRegex);
  }

  @Override
  public Collection<String> getConfigKeys() {
    return getProps().keySet()
        .stream()
        .map(Object::toString)
        .collect(Collectors.toList());
  }

  @Override
  public Map<String, String> getPropsMatchPrefixAndTrimPrefix(
      String keyPrefix) {
    Properties props = getProps();
    Map<String, String> configMap = new HashMap<>();
    for (String name : props.stringPropertyNames()) {
      if (name.startsWith(keyPrefix)) {
        String value = get(name);
        String keyName = name.substring(keyPrefix.length());
        configMap.put(keyName, value);
      }
    }
    return configMap;
  }

  @Override
  public boolean isPropertyTag(String tagStr) {
    return TAGS.contains(tagStr) || super.isPropertyTag(tagStr);
  }

  private static void addDeprecatedKeys() {
    Configuration.addDeprecations(new DeprecationDelta[]{
        new DeprecationDelta("ozone.datanode.pipeline.limit",
            ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT),
        new DeprecationDelta(HDDS_DATANODE_RATIS_PREFIX_KEY + "."
           + RaftServerConfigKeys.PREFIX + "." + "rpcslowness.timeout",
           HDDS_DATANODE_RATIS_PREFIX_KEY + "."
           + RaftServerConfigKeys.PREFIX + "." + "rpc.slowness.timeout"),
        new DeprecationDelta("dfs.datanode.keytab.file",
            HddsConfigKeys.HDDS_DATANODE_KERBEROS_KEYTAB_FILE_KEY),
        new DeprecationDelta("ozone.scm.chunk.layout",
            ScmConfigKeys.OZONE_SCM_CONTAINER_LAYOUT_KEY),
        new DeprecationDelta("hdds.datanode.replication.work.dir",
            OZONE_CONTAINER_COPY_WORKDIR),
        new DeprecationDelta("dfs.container.chunk.write.sync",
            OzoneConfigKeys.HDDS_CONTAINER_CHUNK_WRITE_SYNC_KEY),
        new DeprecationDelta("dfs.container.ipc",
            OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT),
        new DeprecationDelta("dfs.container.ipc.random.port",
            OzoneConfigKeys.HDDS_CONTAINER_IPC_RANDOM_PORT),
        new DeprecationDelta("dfs.container.ratis.admin.port",
            OzoneConfigKeys.HDDS_CONTAINER_RATIS_ADMIN_PORT),
        new DeprecationDelta("dfs.container.ratis.datanode.storage.dir",
            OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATANODE_STORAGE_DIR),
        new DeprecationDelta("dfs.container.ratis.datastream.enabled",
            OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED),
        new DeprecationDelta("dfs.container.ratis.datastream.port",
            OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_PORT),
        new DeprecationDelta("dfs.container.ratis.datastream.random.port",
            OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_RANDOM_PORT),
        new DeprecationDelta("dfs.container.ratis.enabled",
            ScmConfigKeys.HDDS_CONTAINER_RATIS_ENABLED_KEY),
        new DeprecationDelta("dfs.container.ratis.ipc",
            OzoneConfigKeys.HDDS_CONTAINER_RATIS_IPC_PORT),
        new DeprecationDelta("dfs.container.ratis.ipc.random.port",
            OzoneConfigKeys.HDDS_CONTAINER_RATIS_IPC_RANDOM_PORT),
        new DeprecationDelta("dfs.container.ratis.leader.pending.bytes.limit",
            ScmConfigKeys.HDDS_CONTAINER_RATIS_LEADER_PENDING_BYTES_LIMIT),
        new DeprecationDelta("dfs.container.ratis.log.appender.queue.byte-limit",
            ScmConfigKeys.HDDS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT),
        new DeprecationDelta("dfs.container.ratis.log.appender.queue.num-elements",
            ScmConfigKeys.HDDS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS),
        new DeprecationDelta("dfs.container.ratis.log.purge.gap",
            ScmConfigKeys.HDDS_CONTAINER_RATIS_LOG_PURGE_GAP),
        new DeprecationDelta("dfs.container.ratis.log.queue.byte-limit",
            ScmConfigKeys.HDDS_CONTAINER_RATIS_LOG_QUEUE_BYTE_LIMIT),
        new DeprecationDelta("dfs.container.ratis.log.queue.num-elements",
            ScmConfigKeys.HDDS_CONTAINER_RATIS_LOG_QUEUE_NUM_ELEMENTS),
        new DeprecationDelta("dfs.container.ratis.num.container.op.executors",
            ScmConfigKeys.HDDS_CONTAINER_RATIS_NUM_CONTAINER_OP_EXECUTORS_KEY),
        new DeprecationDelta("dfs.container.ratis.num.write.chunk.threads.per.volume",
            ScmConfigKeys.HDDS_CONTAINER_RATIS_NUM_WRITE_CHUNK_THREADS_PER_VOLUME),
        new DeprecationDelta("dfs.container.ratis.rpc.type",
            ScmConfigKeys.HDDS_CONTAINER_RATIS_RPC_TYPE_KEY),
        new DeprecationDelta("dfs.container.ratis.segment.preallocated.size",
            ScmConfigKeys.HDDS_CONTAINER_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY),
        new DeprecationDelta("dfs.container.ratis.segment.size",
            ScmConfigKeys.HDDS_CONTAINER_RATIS_SEGMENT_SIZE_KEY),
        new DeprecationDelta("dfs.container.ratis.server.port",
            OzoneConfigKeys.HDDS_CONTAINER_RATIS_SERVER_PORT),
        new DeprecationDelta("dfs.container.ratis.statemachinedata.sync.retries",
            ScmConfigKeys.HDDS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_RETRIES),
        new DeprecationDelta("dfs.container.ratis.statemachinedata.sync.timeout",
            ScmConfigKeys.HDDS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_TIMEOUT),
        new DeprecationDelta("dfs.container.ratis.statemachine.max.pending.apply-transactions",
            ScmConfigKeys.HDDS_CONTAINER_RATIS_STATEMACHINE_MAX_PENDING_APPLY_TXNS),
        new DeprecationDelta("dfs.ratis.leader.election.minimum.timeout.duration",
            ScmConfigKeys.HDDS_RATIS_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY),
        new DeprecationDelta("dfs.ratis.server.retry-cache.timeout.duration",
            ScmConfigKeys.HDDS_RATIS_SERVER_RETRY_CACHE_TIMEOUT_DURATION_KEY),
        new DeprecationDelta("dfs.ratis.snapshot.threshold",
            ScmConfigKeys.HDDS_RATIS_SNAPSHOT_THRESHOLD_KEY),
        new DeprecationDelta("dfs.datanode.dns.interface",
            HddsConfigKeys.HDDS_DATANODE_DNS_INTERFACE_KEY),
        new DeprecationDelta("dfs.datanode.dns.nameserver",
            HddsConfigKeys.HDDS_DATANODE_DNS_NAMESERVER_KEY),
        new DeprecationDelta("dfs.datanode.hostname",
            HddsConfigKeys.HDDS_DATANODE_HOST_NAME_KEY),
        new DeprecationDelta("dfs.datanode.data.dir",
            ScmConfigKeys.HDDS_DATANODE_DIR_KEY),
        new DeprecationDelta("dfs.datanode.use.datanode.hostname",
            HddsConfigKeys.HDDS_DATANODE_USE_DN_HOSTNAME),
        new DeprecationDelta("dfs.xframe.enabled",
            HddsConfigKeys.HDDS_XFRAME_OPTION_ENABLED),
        new DeprecationDelta("dfs.xframe.value",
            HddsConfigKeys.HDDS_XFRAME_OPTION_VALUE),
        new DeprecationDelta("dfs.metrics.session-id",
            HddsConfigKeys.HDDS_METRICS_SESSION_ID_KEY),
        new DeprecationDelta("dfs.client.https.keystore.resource",
            OzoneConfigKeys.OZONE_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY),
        new DeprecationDelta("dfs.https.server.keystore.resource",
            OzoneConfigKeys.OZONE_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY),
        new DeprecationDelta("dfs.http.policy",
            OzoneConfigKeys.OZONE_HTTP_POLICY_KEY),
        new DeprecationDelta("dfs.datanode.kerberos.principal",
            HddsConfigKeys.HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY),
        new DeprecationDelta("dfs.datanode.kerberos.keytab.file",
            HddsConfigKeys.HDDS_DATANODE_KERBEROS_KEYTAB_FILE_KEY),
        new DeprecationDelta("dfs.metrics.percentiles.intervals",
            HddsConfigKeys.HDDS_METRICS_PERCENTILES_INTERVALS_KEY),
        new DeprecationDelta("hdds.recon.heartbeat.interval",
            HddsConfigKeys.HDDS_RECON_HEARTBEAT_INTERVAL),
    });
  }

  /**
   * Gets backwards-compatible configuration property values.
   * @param name Primary configuration attribute key name.
   * @param fallbackName The key name of the configuration property that needs
   *                     to be backward compatible.
   * @param defaultValue The default value to be returned.
   */
  public int getInt(String name, String fallbackName, int defaultValue,
      Consumer<String> log) {
    String value = this.getTrimmed(name);
    if (value == null) {
      value = this.getTrimmed(fallbackName);
      if (log != null) {
        log.accept(name + " is not set.  Fallback to " + fallbackName +
            ", which is set to " + value);
      }
    }
    if (value == null) {
      return defaultValue;
    }
    return Integer.parseInt(value);
  }

  @Override
  public synchronized void reloadConfiguration() {
    super.reloadConfiguration();
    delegatingProps = null;
  }

  @Override
  protected final synchronized Properties getProps() {
    if (delegatingProps == null) {
      String complianceMode = getPropertyUnsafe(OzoneConfigKeys.OZONE_SECURITY_CRYPTO_COMPLIANCE_MODE,
          OzoneConfigKeys.OZONE_SECURITY_CRYPTO_COMPLIANCE_MODE_UNRESTRICTED);
      Properties cryptoProperties = getCryptoProperties();
      delegatingProps = new DelegatingProperties(super.getProps(), complianceMode, cryptoProperties);
    }
    return delegatingProps;
  }

  /**
   * Get a property value without the compliance check. It's needed to get the compliance
   * mode from the configuration.
   *
   * @param key property name
   * @param defaultValue default value
   * @return property value, without compliance check
   */
  private String getPropertyUnsafe(String key, String defaultValue) {
    return super.getProps().getProperty(key, defaultValue);
  }

  private Properties getCryptoProperties() {
    try {
      return super.getAllPropertiesByTag(ConfigTag.CRYPTO_COMPLIANCE.toString());
    } catch (NoSuchMethodError e) {
      // We need to handle NoSuchMethodError, because in Hadoop 2 we don't have the
      // getAllPropertiesByTag method. We won't be supporting the compliance mode with
      // that version, so we are safe to catch the exception and return a new Properties object.
      return new Properties();
    }
  }

  /**
   * Get a duration value from the configuration, and default to the given value if it's invalid.
   * @param logger the logger to use
   * @param key the key to get the value from
   * @param defaultValue the default value to use if the key is not set
   * @param unit the unit of the duration
   * @return the duration value
   */
  public long getOrFixDuration(Logger logger, String key, String defaultValue, TimeUnit unit) {
    maybeFixInvalidDuration(logger, key, defaultValue, unit);
    return getTimeDuration(key, defaultValue, unit);
  }

  private boolean maybeFixInvalidDuration(Logger logger, String key, String defaultValue, TimeUnit unit) {
    boolean fixed = maybeFixInvalidDuration(key, defaultValue, unit);
    if (fixed) {
      logger.warn("{} must be greater than zero, defaulting to {}", key, defaultValue);
    }
    return fixed;
  }

  private boolean maybeFixInvalidDuration(String key, String defaultValue, TimeUnit unit) {
    long duration = getTimeDuration(key, defaultValue, unit);
    if (duration <= 0) {
      set(key, defaultValue);
      return true;
    }
    return false;
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    DelegatingProperties properties = (DelegatingProperties) getProps();
    return properties.iterator();
  }
}
