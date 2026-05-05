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

package org.apache.hadoop.ozone.s3secret;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Map;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.audit.AuditAction;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.audit.Auditor;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.s3.OzoneClientCache;
import org.apache.hadoop.ozone.s3.util.AuditUtils;

/**
 * Base implementation of endpoint for working with S3 secret.
 */
public class S3SecretEndpointBase implements Auditor {

  private final OzoneConfiguration conf;
  private OzoneClient client;

  @Context
  private ContainerRequestContext context;

  protected static final AuditLogger AUDIT =
      new AuditLogger(AuditLoggerType.S3GLOGGER);

  @Inject
  S3SecretEndpointBase(OzoneConfiguration conf) {
    this.conf = new OzoneConfiguration(conf);
    this.conf.setBoolean(S3Auth.S3_AUTH_CHECK, false);
  }

  @PostConstruct
  void initialize() throws IOException {
    client = OzoneClientCache.createClient(conf);
  }

  protected String userNameFromRequest() {
    return context.getSecurityContext().getUserPrincipal().getName();
  }

  private AuditMessage.Builder auditMessageBaseBuilder(AuditAction op,
      Map<String, String> auditMap) {
    AuditMessage.Builder builder = new AuditMessage.Builder()
        .forOperation(op)
        .withParams(auditMap);
    if (context != null) {
      builder.atIp(AuditUtils.getClientIpAddress(context));
    }
    return builder;
  }

  @Override
  public AuditMessage buildAuditMessageForSuccess(AuditAction op,
      Map<String, String> auditMap) {
    AuditMessage.Builder builder = auditMessageBaseBuilder(op, auditMap)
        .withResult(AuditEventStatus.SUCCESS);
    return builder.build();
  }

  @Override
  public AuditMessage buildAuditMessageForFailure(AuditAction op,
      Map<String, String> auditMap, Throwable throwable) {
    AuditMessage.Builder builder = auditMessageBaseBuilder(op, auditMap)
        .withResult(AuditEventStatus.FAILURE)
        .withException(throwable);
    return builder.build();
  }

  public OzoneClient getClient() {
    return client;
  }

  @VisibleForTesting
  public void setClient(OzoneClient ozoneClient) {
    this.client = ozoneClient;
  }

  @VisibleForTesting
  public void setContext(ContainerRequestContext context) {
    this.context = context;
  }

  protected Map<String, String> getAuditParameters() {
    return AuditUtils.getAuditParameters(context);
  }
}
