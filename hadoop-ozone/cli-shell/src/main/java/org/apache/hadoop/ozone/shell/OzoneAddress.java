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

package org.apache.hadoop.ozone.shell;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_HTTP_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_RPC_SCHEME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_INTERNAL_SERVICE_ID;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.http.client.utils.URIBuilder;

/**
 * Address of an ozone object for ozone shell.
 */
public class OzoneAddress {

  private static final int DEFAULT_OZONE_PORT = 50070;

  private static final String EMPTY_HOST = "___DEFAULT___";

  private URI ozoneURI;

  private String volumeName = "";

  private String bucketName = "";

  private String snapshotNameWithIndicator = "";

  private String keyName = "";

  private boolean isPrefix = false;

  public OzoneAddress() throws OzoneClientException {
    this("o3:///");
  }

  public OzoneAddress(String address)
      throws OzoneClientException {
    if (address == null || address.equals("")) {
      address = OZONE_RPC_SCHEME + ":///";
    }
    this.ozoneURI = parseURI(address);
    String path = this.ozoneURI.getPath();

    path = path.replaceAll("^/+", "");

    int sep1 = path.indexOf('/');
    int sep2 = path.indexOf('/', sep1 + 1);

    if (sep1 == -1) {
      volumeName = path;
    } else {
      //we have vol/bucket
      volumeName = path.substring(0, sep1);
      if (sep2 == -1) {
        bucketName = path.substring(sep1 + 1);
      } else {
        //we have vol/bucket/key/.../...
        bucketName = path.substring(sep1 + 1, sep2);
        keyName = path.substring(sep2 + 1);
      }
    }

  }

  @VisibleForTesting
  protected OzoneClient createRpcClient(ConfigurationSource conf)
      throws IOException {
    return OzoneClientFactory.getRpcClient(conf);
  }

  @VisibleForTesting
  protected OzoneClient createRpcClientFromHostPort(
      String host,
      int port,
      MutableConfigurationSource conf
  )
      throws IOException {
    return OzoneClientFactory.getRpcClient(ozoneURI.getHost(), port, conf);
  }

  @VisibleForTesting
  protected OzoneClient createRpcClientFromServiceId(
      String serviceId,
      MutableConfigurationSource conf
  )
      throws IOException {
    return OzoneClientFactory.getRpcClient(serviceId, conf);
  }

  public OzoneClient createClient(MutableConfigurationSource conf)
      throws IOException {
    OzoneClient client;
    String scheme = ozoneURI.getScheme();
    if (ozoneURI.getScheme() == null || scheme.isEmpty()) {
      scheme = OZONE_RPC_SCHEME;
    }
    if (scheme.equals(OZONE_HTTP_SCHEME)) {
      throw new UnsupportedOperationException(
          "REST schema is not supported any more. Please use AWS S3 protocol "
              + "if you need REST interface.");
    } else if (!scheme.equals(OZONE_RPC_SCHEME)) {
      throw new OzoneClientException(
          "Invalid URI, unknown protocol scheme: " + scheme + ". Use "
              + OZONE_RPC_SCHEME + ":// as the scheme");
    }

    if (ozoneURI.getHost() != null && !ozoneURI.getAuthority()
        .equals(EMPTY_HOST)) {
      if (OmUtils.isOmHAServiceId(conf, ozoneURI.getHost())) {
        // When host is an HA service ID
        if (ozoneURI.getPort() != -1) {
          throw new OzoneClientException(
              "Port " + ozoneURI.getPort() + " specified in URI but host '"
                  + ozoneURI.getHost() + "' is a logical (HA) OzoneManager "
                  + "and does not use port information.");
        }
        client = createRpcClientFromServiceId(ozoneURI.getHost(), conf);
      } else if (ozoneURI.getPort() == -1) {
        client = createRpcClientFromHostPort(ozoneURI.getHost(),
            OmUtils.getOmRpcPort(conf), conf);
      } else {
        client = createRpcClientFromHostPort(ozoneURI.getHost(),
            ozoneURI.getPort(), conf);
      }
    } else { // When host is not specified
      String localOmServiceId = conf.getTrimmed(OZONE_OM_INTERNAL_SERVICE_ID);
      if (localOmServiceId == null) {
        Collection<String> omServiceIds = conf.getTrimmedStringCollection(
            OZONE_OM_SERVICE_IDS_KEY);
        if (omServiceIds.size() > 1) {
          throw new OzoneClientException("Service ID or host name must not"
              + " be omitted when multiple ozone.om.service.ids is defined.");
        } else if (omServiceIds.size() == 1) {
          client = createRpcClientFromServiceId(omServiceIds.iterator().next(),
              conf);
        } else {
          client = createRpcClient(conf);
        }
      } else {
        client = createRpcClientFromServiceId(localOmServiceId, conf);
      }
    }
    return client;
  }

  /**
   * Create OzoneClient for S3Commands.
   *
   * @param conf
   * @param omServiceID
   * @return OzoneClient
   * @throws IOException
   */
  public OzoneClient createClientForS3Commands(
      OzoneConfiguration conf,
      String omServiceID
  )
      throws IOException {
    Collection<String> serviceIds = conf.
        getTrimmedStringCollection(OZONE_OM_SERVICE_IDS_KEY);
    if (omServiceID != null) {
      // OM HA cluster
      if (OmUtils.isOmHAServiceId(conf, omServiceID)) {
        return createRpcClientFromServiceId(omServiceID, conf);
      } else {
        throw new OzoneClientException("Service ID specified does not match" +
            " with " + OZONE_OM_SERVICE_IDS_KEY + " defined in the " +
            "configuration. Configured " + OZONE_OM_SERVICE_IDS_KEY + " are" +
            serviceIds);
      }
    } else if (serviceIds.size() > 1) {
      // If multiple om service ids are there and default value isn't set,
      // throw an error "om service ID must not be omitted"
      String localOmServiceId = conf.getTrimmed(OZONE_OM_INTERNAL_SERVICE_ID);
      if (!localOmServiceId.isEmpty()) {
        return createRpcClientFromServiceId(localOmServiceId, conf);
      }
      throw new OzoneClientException("Service ID must not"
          + " be omitted when cluster has multiple OM Services." +
          "  Configured " + OZONE_OM_SERVICE_IDS_KEY + " are "
          + serviceIds);
    }
    // for non-HA cluster and HA cluster with only 1 service ID
    // get service ID from configurations
    return createRpcClient(conf);
  }

  /**
   * verifies user provided URI.
   *
   * @param uri - UriString
   * @return URI
   */
  protected URI parseURI(String uri) throws OzoneClientException {
    if ((uri == null) || uri.isEmpty()) {
      throw new OzoneClientException(
          "Ozone URI is needed to execute this command.");
    }
    URIBuilder uriBuilder = new URIBuilder(stringToUri(uri));
    if (uriBuilder.getPort() == 0) {
      uriBuilder.setPort(DEFAULT_OZONE_PORT);
    }

    try {
      return uriBuilder.build();
    } catch (URISyntaxException e) {
      throw new OzoneClientException("Invalid URI: " + ozoneURI, e);
    }
  }

  /**
   * Construct a URI from a String with unescaped special characters
   * that have non-standard semantics. e.g. /, ?, #. A custom parsing
   * is needed to prevent misbehavior.
   *
   * @param pathString The input path in string form
   * @return URI
   */
  private static URI stringToUri(String pathString) {
    // parse uri components
    String scheme = null;
    String authority = null;
    int start = 0;

    // parse uri scheme, if any
    int colon = pathString.indexOf(':');
    int slash = pathString.indexOf('/');
    if (colon > 0 && (slash == colon + 1)) {
      // has a non zero-length scheme
      scheme = pathString.substring(0, colon);
      start = colon + 1;
    }

    // parse uri authority, if any
    if (pathString.startsWith("//", start) &&
        (pathString.length() - start > 2)) {
      start += 2;
      int nextSlash = pathString.indexOf('/', start);
      int authEnd = nextSlash > 0 ? nextSlash : pathString.length();
      authority = pathString.substring(start, authEnd);
      start = authEnd;
    }
    // uri path is the rest of the string. ? or # are not interpreted,
    // but any occurrence of them will be quoted by the URI ctor.
    String path = pathString.substring(start, pathString.length());

    // add leading slash to the path, if it does not exist
    int firstSlash = path.indexOf('/');
    if (firstSlash != 0) {
      path = "/" + path;
    }

    if (authority == null || authority.equals("")) {
      authority = EMPTY_HOST;
    }
    // Construct the URI
    try {
      return new URI(scheme, authority, path, null, null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getOmHost() {
    return ozoneURI.getHost();
  }

  public String getOmServiceId(ConfigurationSource conf) {
    if (!Strings.isNullOrEmpty(getOmHost())) {
      return getOmHost();
    } else {
      Collection<String> serviceIds = conf.getTrimmedStringCollection(
          OZONE_OM_SERVICE_IDS_KEY);
      if (serviceIds.size() == 1) {
        // Only one OM service ID configured, we can use that
        // If more than 1, it will fail in createClient step itself
        return serviceIds.iterator().next();
      } else {
        return conf.get(OZONE_OM_ADDRESS_KEY);
      }
    }
  }

  public String getSnapshotNameWithIndicator() {
    return snapshotNameWithIndicator;
  }

  public String getKeyName() {
    return keyName;
  }

  public boolean isPrefix() {
    return isPrefix;
  }

  public void ensureBucketAddress() throws OzoneClientException {
    if (!keyName.isEmpty()) {
      throw new OzoneClientException(
          "Invalid bucket name. Delimiters (/) not allowed in bucket name");
    } else if (volumeName.isEmpty()) {
      throw new OzoneClientException(
          "Volume name is required.");
    } else if (bucketName.isEmpty()) {
      throw new OzoneClientException(
          "Bucket name is required.");
    }
  }

  // Ensure prefix address with a prefix flag
  // Allow CLI to differentiate key and prefix address
  public void ensurePrefixAddress() throws OzoneClientException {
    if (keyName.isEmpty()) {
      throw new OzoneClientException(
          "prefix name is missing.");
    } else if (volumeName.isEmpty()) {
      throw new OzoneClientException(
          "Volume name is missing");
    } else if (bucketName.isEmpty()) {
      throw new OzoneClientException(
          "Bucket name is missing");
    }
    isPrefix = true;
  }

  public void ensureKeyAddress() throws OzoneClientException {
    if (keyName.isEmpty()) {
      throw new OzoneClientException(
          "Key name is missing.");
    } else if (volumeName.isEmpty()) {
      throw new OzoneClientException(
          "Volume name is missing");
    } else if (bucketName.isEmpty()) {
      throw new OzoneClientException(
          "Bucket name is missing");
    }
  }

  /**
   * Checking for a volume and a bucket
   * but also accepting a snapshot
   * indicator and a snapshot name.
   * If the keyName can't be considered
   * a valid snapshot, an exception is thrown.
   *
   * @throws OzoneClientException
   */
  public void ensureSnapshotAddress()
      throws OzoneClientException {
    if (!keyName.isEmpty()) {
      if (OmUtils.isBucketSnapshotIndicator(keyName)) {
        snapshotNameWithIndicator = keyName;
      } else {
        throw new OzoneClientException(
            "Delimiters (/) not allowed following " +
                "a bucket name. Only a snapshot name with " +
                "a snapshot indicator is accepted");
      }
    } else if (volumeName.isEmpty()) {
      throw new OzoneClientException(
          "Volume name is missing.");
    } else if (bucketName.isEmpty()) {
      throw new OzoneClientException(
          "Bucket name is missing.");
    }
  }

  public void ensureVolumeAddress() throws OzoneClientException {
    if (!keyName.isEmpty()) {
      throw new OzoneClientException(
          "Invalid volume name. Delimiters (/) not allowed in volume name");
    } else if (volumeName.isEmpty()) {
      throw new OzoneClientException(
          "Volume name is required");
    } else if (!bucketName.isEmpty()) {
      throw new OzoneClientException(
          "Invalid volume name. Delimiters (/) not allowed in volume name");
    }
  }

  public void ensureRootAddress() throws  OzoneClientException {
    if (!keyName.isEmpty() || !bucketName.isEmpty()
        || !volumeName.isEmpty()) {
      throw new OzoneClientException(
          "Invalid URI. Volume/bucket/key elements should not been used");
    }
  }

  public OzoneObj toOzoneObj(OzoneObj.StoreType storeType) {
    return OzoneObjInfo.Builder.newBuilder()
        .setBucketName(bucketName)
        .setVolumeName(volumeName)
        .setKeyName(keyName)
        .setResType(getResourceType())
        .setStoreType(storeType)
        .build();
  }

  private OzoneObj.ResourceType getResourceType() {
    if (!keyName.isEmpty()) {
      return isPrefix ? OzoneObj.ResourceType.PREFIX :
          OzoneObj.ResourceType.KEY;
    }
    if (!bucketName.isEmpty()) {
      return OzoneObj.ResourceType.BUCKET;
    }
    if (!volumeName.isEmpty()) {
      return OzoneObj.ResourceType.VOLUME;
    }
    return null;
  }

  public void print(PrintWriter out) {
    if (!volumeName.isEmpty()) {
      out.printf("Volume Name : %s%n", volumeName);
    }
    if (!bucketName.isEmpty()) {
      out.printf("Bucket Name : %s%n", bucketName);
    }
    if (!keyName.isEmpty()) {
      out.printf("Key Name : %s%n", keyName);
    }
  }

  public void ensureVolumeOrBucketAddress() throws OzoneClientException {
    if (!keyName.isEmpty()) {
      if (OmUtils.isBucketSnapshotIndicator(keyName)) {
        // If snapshot, ensure snapshot URI
        ensureSnapshotAddress();
        return;
      }
      throw new OzoneClientException(
          "Key address is not supported.");
    } else if (volumeName.isEmpty()) {
      // Volume must be present
      // Bucket may or may not be present
      // Depending on operation is on volume or bucket
      throw new OzoneClientException(
            "Volume name is missing.");
    }
  }
}
