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

package org.apache.hadoop.ozone;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ConfigurationFieldsTests;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.recon.ReconConfigKeys;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.server.http.HttpServer2;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.recon.ReconServerConfigKeys;
import org.apache.hadoop.ozone.s3.S3GatewayConfigKeys;
import org.apache.hadoop.ozone.s3secret.S3SecretConfigKeys;
import org.junit.jupiter.api.Test;

/**
 * Tests if configuration constants documented in ozone-defaults.xml.
 */
public class TestOzoneConfigurationFields extends ConfigurationFieldsTests {

  /**
   * A set of property keys that are allowed to contain a newline in their
   * value. Keep this empty unless a value genuinely requires a line break,
   * otherwise the embedded newline corrupts the runtime string (see HDDS-8046).
   */
  private final Set<String> xmlPropsAllowedToContainNewline = new HashSet<>();

  @Override
  public void initializeMemberVariables() {
    xmlFilename = "ozone-default.xml";
    configurationClasses =
        new Class[] {OzoneConfigKeys.class, ScmConfigKeys.class,
            OMConfigKeys.class, HddsConfigKeys.class,
            ReconConfigKeys.class, ReconServerConfigKeys.class,
            S3GatewayConfigKeys.class,
            S3SecretConfigKeys.class,
        };
    errorIfMissingConfigProps = true;
    errorIfMissingXmlProps = true;
    xmlPropsToSkipCompare.add("ozone.om.nodes.EXAMPLEOMSERVICEID");
    xmlPropsToSkipCompare.add("ozone.om.decommissioned.nodes" +
        ".EXAMPLEOMSERVICEID");
    xmlPropsToSkipCompare.add("ozone.scm.nodes.EXAMPLESCMSERVICEID");
    xmlPrefixToSkipCompare.add("ipc.client.rpc-timeout.ms");
    xmlPropsToSkipCompare.add("ozone.om.leader.election.minimum.timeout" +
        ".duration"); // Deprecated config
    // Currently replication and type configs moved to server side.
    configurationPropsToSkipCompare
        .add(OzoneConfigKeys.OZONE_REPLICATION);
    configurationPropsToSkipCompare
        .add(OzoneConfigKeys.OZONE_REPLICATION_TYPE);
    configurationPropsToSkipCompare
        .add(OzoneConfigKeys.OZONE_CLIENT_REQUIRED_OM_VERSION_MIN_KEY);
    configurationPropsToSkipCompare
        .add(OzoneConfigKeys.OZONE_CLIENT_REQUIRED_OM_VERSION_MIN_DEFAULT);
    // This property is tested in TestHttpServer2 instead
    xmlPropsToSkipCompare.add(HttpServer2.HTTP_IDLE_TIMEOUT_MS_KEY);

    // TODO: Remove this once ranger configs are finalized in HDDS-5836
    configurationPrefixToSkipCompare.add("ozone.om.ranger");

    // Hadoop core-default.xml ships these values across multiple lines; they are
    // outside Ozone's control, so opt them out of the embedded-newline check.
    xmlPropsAllowedToContainNewline.add("fs.s3a.aws.credentials.provider");
    xmlPropsAllowedToContainNewline.add("hadoop.security.sensitive-config-keys");
    xmlPropsAllowedToContainNewline.add("hadoop.system.tags");
    xmlPropsAllowedToContainNewline.add("hadoop.tags.system");

    addPropertiesNotInXml();
  }

  /**
   * Verifies no default value embeds a line break, which would corrupt the
   * runtime string (see HDDS-8046). Uses {@link OzoneConfiguration} to include both
   * generated configuration files and the handwritten {@code ozone-default.xml}.
   * Legitimate cases opt out via {@link #xmlPropsAllowedToContainNewline}.
   */
  @Test
  public void testXmlValuesHaveNoEmbeddedNewlines() {
    Configuration conf = new OzoneConfiguration();
    conf.setAllowNullValueProperties(true);

    Set<String> xmlValuesWithNewlines = new TreeSet<>();
    for (Map.Entry<String, String> entry : conf) {
      String value = entry.getValue();
      if (value == null) {
        continue;
      }
      if (xmlPropsAllowedToContainNewline.contains(entry.getKey())) {
        continue;
      }
      if (value.indexOf('\n') != -1 || value.indexOf('\r') != -1) {
        xmlValuesWithNewlines.add(entry.getKey());
      }
    }

    assertThat(xmlValuesWithNewlines).withFailMessage(
        "These properties have an embedded line break in their <value>, which "
            + "corrupts the runtime string: " + xmlValuesWithNewlines
            + " Put the value on a single line. If the newline is genuinely required, "
            + "add the property to xmlPropsAllowedToContainNewline with a reason + Jira. "
            + "See HDDS-8082.")
        .isEmpty();
  }

  private void addPropertiesNotInXml() {
    configurationPropsToSkipCompare.addAll(Arrays.asList(
        HddsConfigKeys.HDDS_CONTAINER_PERSISTDATA,
        HddsConfigKeys.HDDS_GRPC_TLS_TEST_CERT,
        HddsConfigKeys.HDDS_X509_GRACE_DURATION_TOKEN_CHECKS_ENABLED,
        OMConfigKeys.OZONE_OM_NODES_KEY,
        OMConfigKeys.OZONE_OM_DECOMMISSIONED_NODES_KEY,
        OMConfigKeys.OZONE_OM_LISTENER_NODES_KEY,
        ScmConfigKeys.OZONE_SCM_NODES_KEY,
        ScmConfigKeys.OZONE_SCM_ADDRESS_KEY,
        ScmConfigKeys.OZONE_CHUNK_READ_NETTY_CHUNKED_NIO_FILE_KEY,
        OMConfigKeys.OZONE_FS_TRASH_INTERVAL_KEY,
        OMConfigKeys.OZONE_FS_TRASH_CHECKPOINT_INTERVAL_KEY,
        OMConfigKeys.OZONE_OM_FEATURES_DISABLED,
        OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS,
        OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE,
        OzoneConfigKeys.OZONE_CLIENT_REQUIRED_OM_VERSION_MIN_KEY,
        OzoneConfigKeys.OZONE_RECOVERING_CONTAINER_SCRUBBING_SERVICE_WORKERS,
        OzoneConfigKeys.OZONE_RECOVERING_CONTAINER_SCRUBBING_SERVICE_TIMEOUT,
        OzoneConfigKeys.OZONE_RECOVERING_CONTAINER_TIMEOUT,
        OzoneConfigKeys.OZONE_GPRC_METRICS_PERCENTILES_INTERVALS_KEY,
        ReconConfigKeys.RECON_SCM_CONFIG_PREFIX,
        ReconConfigKeys.OZONE_RECON_DATANODE_ADDRESS_KEY,
        ReconConfigKeys.OZONE_RECON_DATANODE_BIND_HOST_KEY,
        ReconConfigKeys.OZONE_RECON_PROMETHEUS_HTTP_ENDPOINT,
        ReconConfigKeys.OZONE_RECON_TASK_SAFEMODE_WAIT_THRESHOLD,
        ReconServerConfigKeys.OZONE_RECON_SCM_DB_DIR,
        ReconServerConfigKeys.OZONE_RECON_METRICS_HTTP_CONNECTION_TIMEOUT,
        ReconServerConfigKeys
            .OZONE_RECON_METRICS_HTTP_CONNECTION_REQUEST_TIMEOUT,
        ReconServerConfigKeys.RECON_OM_SOCKET_TIMEOUT,
        ReconServerConfigKeys.RECON_OM_CONNECTION_TIMEOUT,
        ReconServerConfigKeys.RECON_OM_CONNECTION_REQUEST_TIMEOUT,
        ReconServerConfigKeys.RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY,
        ReconServerConfigKeys.RECON_OM_SNAPSHOT_TASK_INTERVAL_DELAY,
        ReconServerConfigKeys.RECON_OM_SNAPSHOT_TASK_FLUSH_PARAM,
        ReconServerConfigKeys.OZONE_RECON_SCM_CONTAINER_ID_BATCH_SIZE,
        OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY,
        OMConfigKeys.OZONE_OM_HA_PREFIX,
        OMConfigKeys.OZONE_OM_GRPC_PORT_KEY,
        // TODO HDDS-2856
        OMConfigKeys.OZONE_RANGER_OM_IGNORE_SERVER_CERT,
        OMConfigKeys.OZONE_RANGER_OM_CONNECTION_TIMEOUT,
        OMConfigKeys.OZONE_RANGER_OM_CONNECTION_REQUEST_TIMEOUT,
        OMConfigKeys.OZONE_RANGER_HTTPS_ADDRESS_KEY,
        OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_USER,
        OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD,
        OMConfigKeys.OZONE_THREAD_NUMBER_DIR_DELETION,
        OMConfigKeys.OZONE_THREAD_NUMBER_KEY_DELETION,
        ScmConfigKeys.OZONE_SCM_PIPELINE_PLACEMENT_IMPL_KEY,
        ScmConfigKeys.OZONE_SCM_HA_PREFIX,
        S3GatewayConfigKeys.OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED,
        DatanodeConfiguration.HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_PERCENT,
        OzoneConfigKeys.HDDS_SCM_CLIENT_RPC_TIME_OUT,
        OzoneConfigKeys.HDDS_SCM_CLIENT_MAX_RETRY_TIMEOUT,
        OzoneConfigKeys.HDDS_SCM_CLIENT_FAILOVER_MAX_RETRY
    ));
  }
}

