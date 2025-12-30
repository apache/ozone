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

package org.apache.hadoop.hdds.server;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collection;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.recon.ReconConfigKeys;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic utilities for all HDDS/Ozone servers.
 */
public final class ServerUtils {

  private static final Logger LOG = LoggerFactory.getLogger(
      ServerUtils.class);

  private ServerUtils() {
  }

  /**
   * Checks that a given value is with a range.
   *
   * For example, sanitizeUserArgs(17, 3, 5, 10)
   * ensures that 17 is greater/equal than 3 * 5 and less/equal to 3 * 10.
   *
   * @param key           - config key of the value
   * @param valueTocheck  - value to check
   * @param baseKey       - config key of the baseValue
   * @param baseValue     - the base value that is being used.
   * @param minFactor     - range min - a 2 here makes us ensure that value
   *                        valueTocheck is at least twice the baseValue.
   * @param maxFactor     - range max
   * @return long
   */
  public static long sanitizeUserArgs(String key, long valueTocheck,
      String baseKey, long baseValue, long minFactor, long maxFactor) {
    long minLimit = baseValue * minFactor;
    long maxLimit = baseValue * maxFactor;
    if (valueTocheck < minLimit) {
      LOG.warn(
          "{} value = {} is smaller than min = {} based on"
          + " the key value of {}, reset to the min value {}.",
          key, valueTocheck, minLimit, baseKey, minLimit);
      valueTocheck = minLimit;
    } else if (valueTocheck > maxLimit) {
      LOG.warn(
          "{} value = {} is larger than max = {} based on"
          + " the key value of {}, reset to the max value {}.",
          key, valueTocheck, maxLimit, baseKey, maxLimit);
      valueTocheck = maxLimit;
    }

    return valueTocheck;
  }

  /**
   * After starting an RPC server, updates configuration with the actual
   * listening address of that server. The listening address may be different
   * from the configured address if, for example, the configured address uses
   * port 0 to request use of an ephemeral port.
   *
   * @param conf configuration to update
   * @param rpcAddressKey configuration key for RPC server address
   * @param addr configured address
   * @param rpcServer started RPC server.
   */
  public static InetSocketAddress updateRPCListenAddress(
      OzoneConfiguration conf, String rpcAddressKey,
      InetSocketAddress addr, RPC.Server rpcServer) {
    return updateListenAddress(conf, rpcAddressKey, addr,
        rpcServer.getListenerAddress());
  }

  /**
   * After starting an server, updates configuration with the actual
   * listening address of that server. The listening address may be different
   * from the configured address if, for example, the configured address uses
   * port 0 to request use of an ephemeral port.
   *
   * @param conf       configuration to update
   * @param addressKey configuration key for RPC server address
   * @param addr       configured address
   * @param listenAddr the real listening address.
   */
  public static InetSocketAddress updateListenAddress(OzoneConfiguration conf,
      String addressKey, InetSocketAddress addr, InetSocketAddress listenAddr) {
    InetSocketAddress updatedAddr = new InetSocketAddress(addr.getHostString(),
        listenAddr.getPort());
    conf.set(addressKey,
        addr.getHostString() + ":" + listenAddr.getPort());
    return updatedAddr;
  }

  /**
   * Get the location where SCM should store its metadata directories.
   * Fall back to OZONE_METADATA_DIRS if not defined.
   *
   * @param conf
   * @return File
   */
  public static File getScmDbDir(ConfigurationSource conf) {
    File metadataDir = getDirectoryFromConfig(conf,
        ScmConfigKeys.OZONE_SCM_DB_DIRS, "SCM");
    if (metadataDir != null) {
      return metadataDir;
    }

    LOG.warn("{} is not configured. We recommend adding this setting. " +
        "Falling back to {} instead.",
        ScmConfigKeys.OZONE_SCM_DB_DIRS, HddsConfigKeys.OZONE_METADATA_DIRS);
    return getOzoneMetaDirPath(conf);
  }

  /**
   * Utility method to retrieve the value of a key representing a DB directory
   * and create a File object for the directory. The method also sets the
   * directory permissions based on the configuration.
   *
   * @param conf configuration bag
   * @param key Key to test
   * @param componentName Which component's key is this
   * @return File created from the value of the key in conf.
   */
  public static File getDirectoryFromConfig(ConfigurationSource conf,
                                            String key,
                                            String componentName) {
    final Collection<String> metadirs = conf.getTrimmedStringCollection(key);
    if (metadirs.size() > 1) {
      throw new IllegalArgumentException(
          "Bad config setting " + key +
              ". " + componentName +
              " does not support multiple metadata dirs currently");
    }

    if (metadirs.size() == 1) {
      final File dbDirPath = new File(metadirs.iterator().next());
      if (!dbDirPath.mkdirs() && !dbDirPath.exists()) {
        throw new IllegalArgumentException("Unable to create directory " +
            dbDirPath + " specified in configuration setting " +
            key);
      }
      try {
        Path path = dbDirPath.toPath();
        // Fetch the permissions for the respective component from the config
        String permissionValue = getPermissions(key, conf);
        String symbolicPermission = getSymbolicPermission(permissionValue);

        // Set the permissions for the directory
        Files.setPosixFilePermissions(path,
            PosixFilePermissions.fromString(symbolicPermission));
      } catch (Exception e) {
        throw new RuntimeException("Failed to set directory permissions for " +
            dbDirPath + ": " + e.getMessage(), e);
      }
      return dbDirPath;
    }

    return null;
  }

  /**
   * Fetches the symbolic representation of the permission value.
   *
   * @param permissionValue the permission value (octal or symbolic)
   * @return the symbolic representation of the permission value
   */
  private static String getSymbolicPermission(String permissionValue) {
    if (isSymbolic(permissionValue)) {
      // For symbolic representation, use it directly
      return permissionValue;
    } else {
      // For octal representation, convert it to FsPermission object and then
      // to symbolic representation
      short octalPermission = Short.parseShort(permissionValue, 8);
      FsPermission fsPermission = new FsPermission(octalPermission);
      return fsPermission.toString();
    }
  }

  /**
   * Checks if the permission value is in symbolic representation.
   *
   * @param permissionValue the permission value to check
   * @return true if the permission value is in symbolic representation,
   * false otherwise
   */
  private static boolean isSymbolic(String permissionValue) {
    return permissionValue.matches(".*[rwx].*");
  }

  /**
   * Retrieves the permissions' configuration value for a given config key.
   *
   * @param key  The configuration key.
   * @param conf The ConfigurationSource object containing the config
   * @return The permissions' configuration value for the specified key.
   * @throws IllegalArgumentException If the configuration value is not defined
   */
  public static String getPermissions(String key, ConfigurationSource conf) {
    String configName = "";

    // Assign the appropriate config name based on the KEY
    if (key.equals(ReconConfigKeys.OZONE_RECON_DB_DIR)) {
      configName = ReconConfigKeys.OZONE_RECON_DB_DIRS_PERMISSIONS;
    } else if (key.equals(ScmConfigKeys.OZONE_SCM_DB_DIRS)) {
      configName = ScmConfigKeys.OZONE_SCM_DB_DIRS_PERMISSIONS;
    } else if (key.equals(OzoneConfigKeys.OZONE_OM_DB_DIRS)) {
      configName = OzoneConfigKeys.OZONE_OM_DB_DIRS_PERMISSIONS;
    } else {
      // If the permissions are not defined for the config, we make it fall
      // back to the default permissions for metadata files and directories
      configName = OzoneConfigKeys.OZONE_METADATA_DIRS_PERMISSIONS;
    }

    String configValue = conf.get(configName);
    if (configValue != null) {
      return configValue;
    }

    throw new IllegalArgumentException(
        "Invalid configuration value for key: " + key);
  }

  /**
   * Checks and creates Ozone Metadir Path if it does not exist.
   *
   * @param conf - Configuration
   * @return File MetaDir
   * @throws IllegalArgumentException if the configuration setting is not set
   */
  public static File getOzoneMetaDirPath(ConfigurationSource conf) {
    File dirPath = getDirectoryFromConfig(conf,
        HddsConfigKeys.OZONE_METADATA_DIRS, "Ozone");
    if (dirPath == null) {
      throw new IllegalArgumentException(
          HddsConfigKeys.OZONE_METADATA_DIRS + " must be defined.");
    }
    return dirPath;
  }

  public static void setOzoneMetaDirPath(OzoneConfiguration conf,
                                         String path) {
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, path);
  }

  /**
   * Returns with the service specific metadata directory.
   * <p>
   * If the directory is missing the method tries to create it.
   *
   * @param conf The ozone configuration object
   * @param key  The configuration key which specify the directory.
   * @return The path of the directory.
   */
  public static File getDBPath(ConfigurationSource conf, String key) {
    final File dbDirPath =
        getDirectoryFromConfig(conf, key, "OM");
    if (dbDirPath != null) {
      return dbDirPath;
    }

    LOG.warn("{} is not configured. We recommend adding this setting. "
            + "Falling back to {} instead.", key,
        HddsConfigKeys.OZONE_METADATA_DIRS);
    return ServerUtils.getOzoneMetaDirPath(conf);
  }

  public static String getRemoteUserName() {
    UserGroupInformation remoteUser = Server.getRemoteUser();
    return remoteUser != null ? remoteUser.getUserName() : null;
  }

  /**
   * Get the default Ratis directory for a component when the specific
   * configuration is not set. This creates a component-specific subdirectory
   * under ozone.metadata.dirs to avoid conflicts when multiple components
   * are colocated on the same host.
   *
   * <p>For backward compatibility during upgrades, this method checks for
   * existing Ratis data in old locations before using the new component-specific
   * location. See {@link #findExistingRatisDirectory} for details on old locations.
   *
   * @param conf Configuration source
   * @param nodeType Type of the node component
   * @return Path to the component-specific ratis directory
   */
  public static String getDefaultRatisDirectory(ConfigurationSource conf,
      NodeType nodeType) {
    LOG.warn("Storage directory for Ratis is not configured. It is a good " +
            "idea to map this to an SSD disk. Falling back to {}",
        HddsConfigKeys.OZONE_METADATA_DIRS);
    File metaDirPath = ServerUtils.getOzoneMetaDirPath(conf);
    
    // Check for existing Ratis data from old versions for backward compatibility
    String existingDir = findExistingRatisDirectory(metaDirPath, nodeType);
    if (existingDir != null) {
      return existingDir;
    }
    
    // Use new component-specific location for new installations
    String componentName = getComponentName(nodeType);
    return Paths.get(metaDirPath.getPath(), componentName + ".ratis").toString();
  }

  /**
   * Checks for existing Ratis directories from previous versions for backward
   * compatibility during upgrades.
   *
   * <p>Older versions of Ozone used different directory structures:
   * <ul>
   *   <li>Versions up to 2.0.0: Shared {@code <ozone.metadata.dirs>/ratis} for all components</li>
   *   <li>Some SCM versions: Used {@code <ozone.metadata.dirs>/scm-ha}</li>
   * </ul>
   *
   * @param metaDirPath The ozone metadata directory path
   * @param nodeType Type of the node component
   * @return Path to existing old Ratis directory if found, null otherwise
   */
  private static String findExistingRatisDirectory(File metaDirPath,
      NodeType nodeType) {
    // Check component-specific old location (SCM used scm-ha in some versions)
    if ("scm".equals(getComponentName(nodeType))) {
      File oldScmRatisDir = new File(metaDirPath, "scm-ha");
      if (isNonEmptyDirectory(oldScmRatisDir)) {
        LOG.info("Found existing SCM Ratis directory at old location: {}. " +
                "Using it for backward compatibility during upgrade.",
            oldScmRatisDir.getPath());
        return oldScmRatisDir.getPath();
      }
    }

    // Check old shared Ratis location (used by version 2.0.0 and earlier)
    // All components (OM, SCM) shared /data/metadata/ratis
    File oldSharedRatisDir = new File(metaDirPath, "ratis");
    if (isNonEmptyDirectory(oldSharedRatisDir)) {
      LOG.info("Found existing Ratis directory at old shared location: {}. " +
              "Using it for backward compatibility during upgrade.",
          oldSharedRatisDir.getPath());
      return oldSharedRatisDir.getPath();
    }

    return null;
  }

  /**
   * Converts NodeType enum to the component name string used for directory naming.
   *
   * @param nodeType Type of the node component
   * @return Component name string (e.g., "om", "scm", "dn", "recon")
   */
  private static String getComponentName(NodeType nodeType) {
    switch (nodeType) {
    case OM:
      return "om";
    case SCM:
      return "scm";
    case DATANODE:
      return "dn";
    case RECON:
      return "recon";
    default:
      throw new IllegalArgumentException("Unknown NodeType: " + nodeType);
    }
  }

  /**
   * Get the default Ratis snapshot directory for a component when the specific
   * configuration is not set. This creates a component-specific subdirectory
   * under ozone.metadata.dirs to avoid conflicts when multiple components
   * are colocated on the same host.
   *
   * New path format: {ozone.metadata.dirs}/{NodeType}.ratis.snapshot
   * eg: /data/metadata/om.ratis.snapshot
   *     /data/metadata/scm.ratis.snapshot
   *
   * @param conf Configuration source
   * @param nodeType Type of the node component
   * @return Path to the component-specific ratis snapshot directory
   */
  public static String getDefaultRatisSnapshotDirectory(ConfigurationSource conf,
      NodeType nodeType) {
    LOG.warn("Snapshot directory for Ratis is not configured. Falling back to {}",
        HddsConfigKeys.OZONE_METADATA_DIRS);
    File metaDirPath = ServerUtils.getOzoneMetaDirPath(conf);
    
    // Use component-specific location
    String componentName = getComponentName(nodeType);
    return Paths.get(metaDirPath.getPath(),
        componentName + ".ratis." + OzoneConsts.OZONE_RATIS_SNAPSHOT_DIR).toString();
  }

  /**
   * Checks if a directory exists and is non-empty.
   *
   * @param dir Directory to check
   * @return true if directory exists and contains at least one file
   */
  private static boolean isNonEmptyDirectory(File dir) {
    if (dir != null && dir.exists() && dir.isDirectory()) {
      File[] files = dir.listFiles();
      return files != null && files.length > 0;
    }
    return false;
  }
}
