/*
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
package org.apache.hadoop.ozone.om.request.validation;

import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.Versioned;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.upgrade.LayoutVersionManager;

/**
 * Class to extract version out of OM request.
 */
public enum VersionExtractor {
  /**
   * Extracts current metadata layout version.
   */
  LAYOUT_VERSION_EXTRACTOR {
    @Override
    public Versioned extractVersion(OMRequest req, ValidationContext ctx) {
      LayoutVersionManager layoutVersionManager = ctx.versionManager();
      return ctx.versionManager().getFeature(layoutVersionManager.getMetadataLayoutVersion());
    }

    @Override
    public Class<OMLayoutFeature> getVersionClass() {
      return OMLayoutFeature.class;
    }
  },

  /**
   * Extracts client version from the OMRequests.
   */
  CLIENT_VERSION_EXTRACTOR {
    @Override
    public Versioned extractVersion(OMRequest req, ValidationContext ctx) {
      return req.getVersion() > ClientVersion.CURRENT_VERSION ?
          ClientVersion.FUTURE_VERSION : ClientVersion.fromProtoValue(req.getVersion());
    }

    @Override
    public Class<ClientVersion> getVersionClass() {
      return ClientVersion.class;
    }
  };

  public abstract Versioned extractVersion(OMRequest req, ValidationContext ctx);
  public abstract Class<? extends Versioned> getVersionClass();
}
