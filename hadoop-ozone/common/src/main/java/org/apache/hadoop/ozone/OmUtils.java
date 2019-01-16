/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Optional;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import static org.apache.hadoop.hdds.HddsUtils.getHostNameFromConfigKeys;
import static org.apache.hadoop.hdds.HddsUtils.getPortNumberFromConfigKeys;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_BIND_HOST_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_BIND_PORT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_PORT_DEFAULT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateless helper functions for the server and client side of OM
 * communication.
 */
public final class OmUtils {
  public static final Logger LOG = LoggerFactory.getLogger(OmUtils.class);

  private OmUtils() {
  }

  /**
   * Retrieve the socket address that is used by OM.
   * @param conf
   * @return Target InetSocketAddress for the SCM service endpoint.
   */
  public static InetSocketAddress getOmAddress(Configuration conf) {
    return NetUtils.createSocketAddr(getOmRpcAddress(conf));
  }

  /**
   * Retrieve the socket address that is used by OM.
   * @param conf
   * @return Target InetSocketAddress for the SCM service endpoint.
   */
  public static String getOmRpcAddress(Configuration conf) {
    final Optional<String> host = getHostNameFromConfigKeys(conf,
        OZONE_OM_ADDRESS_KEY);

    return host.orElse(OZONE_OM_BIND_HOST_DEFAULT) + ":" +
        getOmRpcPort(conf);
  }

  /**
   * Retrieve the socket address that should be used by clients to connect
   * to OM.
   * @param conf
   * @return Target InetSocketAddress for the OM service endpoint.
   */
  public static InetSocketAddress getOmAddressForClients(
      Configuration conf) {
    final Optional<String> host = getHostNameFromConfigKeys(conf,
        OZONE_OM_ADDRESS_KEY);

    if (!host.isPresent()) {
      throw new IllegalArgumentException(
          OZONE_OM_ADDRESS_KEY + " must be defined. See" +
              " https://wiki.apache.org/hadoop/Ozone#Configuration for" +
              " details on configuring Ozone.");
    }

    return NetUtils.createSocketAddr(
        host.get() + ":" + getOmRpcPort(conf));
  }

  public static int getOmRpcPort(Configuration conf) {
    // If no port number is specified then we'll just try the defaultBindPort.
    final Optional<Integer> port = getPortNumberFromConfigKeys(conf,
        OZONE_OM_ADDRESS_KEY);
    return port.orElse(OZONE_OM_PORT_DEFAULT);
  }

  public static int getOmRestPort(Configuration conf) {
    // If no port number is specified then we'll just try the default
    // HTTP BindPort.
    final Optional<Integer> port =
        getPortNumberFromConfigKeys(conf, OZONE_OM_HTTP_ADDRESS_KEY);
    return port.orElse(OZONE_OM_HTTP_BIND_PORT_DEFAULT);
  }

  /**
   * Get the location where OM should store its metadata directories.
   * Fall back to OZONE_METADATA_DIRS if not defined.
   *
   * @param conf
   * @return
   */
  public static File getOmDbDir(Configuration conf) {
    final Collection<String> dbDirs = conf.getTrimmedStringCollection(
        OMConfigKeys.OZONE_OM_DB_DIRS);

    if (dbDirs.size() > 1) {
      throw new IllegalArgumentException(
          "Bad configuration setting " + OMConfigKeys.OZONE_OM_DB_DIRS +
              ". OM does not support multiple metadata dirs currently.");
    }

    if (dbDirs.size() == 1) {
      final File dbDirPath = new File(dbDirs.iterator().next());
      if (!dbDirPath.exists() && !dbDirPath.mkdirs()) {
        throw new IllegalArgumentException("Unable to create directory " +
            dbDirPath + " specified in configuration setting " +
            OMConfigKeys.OZONE_OM_DB_DIRS);
      }
      return dbDirPath;
    }

    LOG.warn("{} is not configured. We recommend adding this setting. " +
        "Falling back to {} instead.",
        OMConfigKeys.OZONE_OM_DB_DIRS, HddsConfigKeys.OZONE_METADATA_DIRS);
    return ServerUtils.getOzoneMetaDirPath(conf);
  }

  /**
   * Checks if the OM request is read only or not.
   * @param omRequest OMRequest proto
   * @return True if its readOnly, false otherwise.
   */
  public static boolean isReadOnly(
      OzoneManagerProtocolProtos.OMRequest omRequest) {
    OzoneManagerProtocolProtos.Type cmdType = omRequest.getCmdType();
    switch (cmdType) {
      case CheckVolumeAccess:
      case InfoVolume:
      case ListVolume:
      case InfoBucket:
      case ListBuckets:
      case LookupKey:
      case ListKeys:
      case InfoS3Bucket:
      case ListS3Buckets:
      case ServiceList:
        return true;
      case CreateVolume:
      case SetVolumeProperty:
      case DeleteVolume:
      case CreateBucket:
      case SetBucketProperty:
      case DeleteBucket:
      case CreateKey:
      case RenameKey:
      case DeleteKey:
      case CommitKey:
      case AllocateBlock:
      case CreateS3Bucket:
      case DeleteS3Bucket:
      case InitiateMultiPartUpload:
      case CommitMultiPartUpload:
      case CompleteMultiPartUpload:
      case AbortMultiPartUpload:
        return false;
      default:
        LOG.error("CmdType {} is not categorized as readOnly or not.", cmdType);
        return false;
    }
  }

  public static byte[] getMD5Digest(String input) throws IOException {
    try {
      MessageDigest md = MessageDigest.getInstance(OzoneConsts.MD5_HASH);
      return md.digest(input.getBytes(StandardCharsets.UTF_8));
    } catch (NoSuchAlgorithmException ex) {
      throw new IOException("Error creating an instance of MD5 digest.\n" +
          "This could possibly indicate a faulty JRE");
    }
  }

  public static byte[] getSHADigest() throws IOException {
    try {
      MessageDigest sha = MessageDigest.getInstance(OzoneConsts.FILE_HASH);
      return sha.digest(RandomStringUtils.random(32)
          .getBytes(StandardCharsets.UTF_8));
    } catch (NoSuchAlgorithmException ex) {
      throw new IOException("Error creating an instance of SHA-256 digest.\n" +
          "This could possibly indicate a faulty JRE");
    }
  }
}
