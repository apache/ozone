/**
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
package org.apache.hadoop.ozone.shell;

import java.io.IOException;
import java.io.PrintStream;
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

import com.google.common.annotations.VisibleForTesting;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_HTTP_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_RPC_SCHEME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;
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
      throws IOException, OzoneClientException {
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
        client = createRpcClient(conf);
      } else if (ozoneURI.getPort() == -1) {
        client = createRpcClientFromHostPort(ozoneURI.getHost(),
            OmUtils.getOmRpcPort(conf), conf);
      } else {
        client = createRpcClientFromHostPort(ozoneURI.getHost(),
            ozoneURI.getPort(), conf);
      }
    } else {// When host is not specified

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
   * @throws OzoneClientException
   */
  public OzoneClient createClientForS3Commands(
      OzoneConfiguration conf,
      String omServiceID
  )
      throws IOException, OzoneClientException {
    if (omServiceID != null) {
      // OM HA cluster
      if (OmUtils.isOmHAServiceId(conf, omServiceID)) {
        return OzoneClientFactory.getRpcClient(omServiceID, conf);
      } else {
        throw new OzoneClientException("Service ID specified does not match" +
            " with " + OZONE_OM_SERVICE_IDS_KEY + " defined in the " +
            "configuration. Configured " + OZONE_OM_SERVICE_IDS_KEY + " are" +
            conf.getTrimmedStringCollection(OZONE_OM_SERVICE_IDS_KEY));
      }
    } else {
      // If om service id is not specified, consider it as a non-HA cluster.
      // But before that check if serviceId is defined. If it is defined
      // throw an error om service ID needs to be specified.
      if (OmUtils.isServiceIdsDefined(conf)) {
        throw new OzoneClientException("Service ID must not"
            + " be omitted when " + OZONE_OM_SERVICE_IDS_KEY + " is defined. " +
            "Configured " + OZONE_OM_SERVICE_IDS_KEY + " are " +
            conf.getTrimmedStringCollection(OZONE_OM_SERVICE_IDS_KEY));
      }
      return OzoneClientFactory.getRpcClient(conf);
    }
  }

  /**
   * verifies user provided URI.
   *
   * @param uri - UriString
   * @return URI
   */
  protected URI parseURI(String uri)
      throws OzoneClientException {
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
    if(firstSlash != 0) {
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

  public String getKeyName() {
    return keyName;
  }

  public boolean isPrefix() {
    return isPrefix;
  }

  public void ensureBucketAddress() throws OzoneClientException {
    if (keyName.length() > 0) {
      throw new OzoneClientException(
          "Invalid bucket name. Delimiters (/) not allowed in bucket name");
    } else if (volumeName.length() == 0) {
      throw new OzoneClientException(
          "Volume name is required.");
    } else if (bucketName.length() == 0) {
      throw new OzoneClientException(
          "Bucket name is required.");
    }
  }

  // Ensure prefix address with a prefix flag
  // Allow CLI to differentiate key and prefix address
  public void ensurePrefixAddress() throws OzoneClientException {
    if (keyName.length() == 0) {
      throw new OzoneClientException(
          "prefix name is missing.");
    } else if (volumeName.length() == 0) {
      throw new OzoneClientException(
          "Volume name is missing");
    } else if (bucketName.length() == 0) {
      throw new OzoneClientException(
          "Bucket name is missing");
    }
    isPrefix = true;
  }

  public void ensureKeyAddress() throws OzoneClientException {
    if (keyName.length() == 0) {
      throw new OzoneClientException(
          "Key name is missing.");
    } else if (volumeName.length() == 0) {
      throw new OzoneClientException(
          "Volume name is missing");
    } else if (bucketName.length() == 0) {
      throw new OzoneClientException(
          "Bucket name is missing");
    }
  }

  public void ensureVolumeAddress() throws OzoneClientException {
    if (keyName.length() != 0) {
      throw new OzoneClientException(
          "Invalid volume name. Delimiters (/) not allowed in volume name");
    } else if (volumeName.length() == 0) {
      throw new OzoneClientException(
          "Volume name is required");
    } else if (bucketName.length() != 0) {
      throw new OzoneClientException(
          "Invalid volume name. Delimiters (/) not allowed in volume name");
    }
  }

  public void ensureRootAddress() throws OzoneClientException {
    if (keyName.length() != 0 || bucketName.length() != 0
        || volumeName.length() != 0) {
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

  public void print(PrintStream out) {
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
}
