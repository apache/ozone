/*
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
package org.apache.hadoop.ozone;

import org.apache.hadoop.conf.TestConfigurationFieldsBase;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.recon.ReconConfigKeys;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.recon.ReconServerConfigKeys;
import org.apache.hadoop.ozone.s3.S3GatewayConfigKeys;

import java.util.Arrays;
import org.junit.Rule;
import org.junit.rules.Timeout;

/**
 * Tests if configuration constants documented in ozone-defaults.xml.
 */
public class TestOzoneConfigurationFields extends TestConfigurationFieldsBase {

  /**
    * Set a timeout for each test.
    */
  @Rule
  public Timeout timeout = Timeout.seconds(300);

  @Override
  public void initializeMemberVariables() {
    xmlFilename = "ozone-default.xml";
    configurationClasses =
        new Class[] {OzoneConfigKeys.class, ScmConfigKeys.class,
            OMConfigKeys.class, HddsConfigKeys.class,
            ReconConfigKeys.class, ReconServerConfigKeys.class,
            S3GatewayConfigKeys.class,
            SCMHTTPServerConfig.class,
            SCMHTTPServerConfig.ConfigStrings.class,
            ScmConfig.ConfigStrings.class
        };
    errorIfMissingConfigProps = true;
    errorIfMissingXmlProps = true;
    xmlPropsToSkipCompare.add("hadoop.tags.custom");
    xmlPropsToSkipCompare.add("ozone.om.nodes.EXAMPLEOMSERVICEID");
    xmlPropsToSkipCompare.add("ozone.om.decommissioned.nodes" +
        ".EXAMPLEOMSERVICEID");
    xmlPropsToSkipCompare.add("ozone.scm.nodes.EXAMPLESCMSERVICEID");
    xmlPropsToSkipCompare.add("ozone.scm.nodes.EXAMPLESCMSERVICEID");
    xmlPrefixToSkipCompare.add("ipc.client.rpc-timeout.ms");
    xmlPropsToSkipCompare.add("ozone.om.leader.election.minimum.timeout" +
        ".duration"); // Deprecated config
    configurationPropsToSkipCompare
        .add(ScmConfig.ConfigStrings.HDDS_SCM_INIT_DEFAULT_LAYOUT_VERSION);
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

    addPropertiesNotInXml();
  }

  private void addPropertiesNotInXml() {
    configurationPropsToSkipCompare.addAll(Arrays.asList(
        HddsConfigKeys.HDDS_CONTAINER_PERSISTDATA,
        HddsConfigKeys.HDDS_GRPC_TLS_TEST_CERT,
        HddsConfigKeys.HDDS_KEY_ALGORITHM,
        HddsConfigKeys.HDDS_SECURITY_PROVIDER,
        HddsConfigKeys.HDDS_X509_CRL_NAME, // HDDS-2873
        OMConfigKeys.OZONE_OM_NODES_KEY,
        OMConfigKeys.OZONE_OM_DECOMMISSIONED_NODES_KEY,
        ScmConfigKeys.OZONE_SCM_NODES_KEY,
        ScmConfigKeys.OZONE_SCM_ADDRESS_KEY,
        ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY,
        OMConfigKeys.OZONE_FS_TRASH_INTERVAL_KEY,
        OMConfigKeys.OZONE_FS_TRASH_CHECKPOINT_INTERVAL_KEY,
        OMConfigKeys.OZONE_OM_S3_GPRC_SERVER_ENABLED,
        OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE,
        OzoneConfigKeys.OZONE_S3_AUTHINFO_MAX_LIFETIME_KEY,
        OzoneConfigKeys.OZONE_CLIENT_REQUIRED_OM_VERSION_MIN_KEY,
        OzoneConfigKeys.OZONE_RECOVERING_CONTAINER_SCRUBBING_SERVICE_WORKERS,
        OzoneConfigKeys.OZONE_RECOVERING_CONTAINER_SCRUBBING_SERVICE_TIMEOUT,
        OzoneConfigKeys.OZONE_RECOVERING_CONTAINER_TIMEOUT,
        ReconConfigKeys.RECON_SCM_CONFIG_PREFIX,
        ReconConfigKeys.OZONE_RECON_ADDRESS_KEY,
        ReconConfigKeys.OZONE_RECON_DATANODE_ADDRESS_KEY,
        ReconConfigKeys.OZONE_RECON_DATANODE_BIND_HOST_KEY,
        ReconConfigKeys.OZONE_RECON_PROMETHEUS_HTTP_ENDPOINT,
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
        OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY,
        OMConfigKeys.OZONE_OM_HA_PREFIX,
        OMConfigKeys.OZONE_OM_TRANSPORT_CLASS,
        OMConfigKeys.OZONE_OM_GRPC_PORT_KEY,
        // TODO HDDS-2856
        OMConfigKeys.OZONE_RANGER_OM_IGNORE_SERVER_CERT,
        OMConfigKeys.OZONE_RANGER_OM_CONNECTION_TIMEOUT,
        OMConfigKeys.OZONE_RANGER_OM_CONNECTION_REQUEST_TIMEOUT,
        OMConfigKeys.OZONE_RANGER_HTTPS_ADDRESS_KEY,
        OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_USER,
        OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD,
        ScmConfigKeys.OZONE_SCM_PIPELINE_PLACEMENT_IMPL_KEY
    ));
  }
}
