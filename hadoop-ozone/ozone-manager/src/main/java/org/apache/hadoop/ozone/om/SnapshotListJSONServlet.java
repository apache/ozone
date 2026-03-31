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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.snapshot.ListSnapshotResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides REST access to Ozone Snapshot List.
 */
public class SnapshotListJSONServlet extends HttpServlet {

  private static final Logger LOG =
      LoggerFactory.getLogger(SnapshotListJSONServlet.class);
  private static final long serialVersionUID = 1L;

  private transient OzoneManager om;

  /**
   * Jackson mix-in to ignore protobuf getter in SnapshotInfo.
   */
  @JsonIgnoreProperties({"protobuf", "createTransactionInfo", "lastTransactionInfo"})
  abstract static class SnapshotInfoMixin {
  }

  @Override
  public void init() throws ServletException {
    this.om = (OzoneManager) getServletContext()
        .getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE);
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) {
    try {
      String volume = request.getParameter("volume");
      String bucket = request.getParameter("bucket");

      if (volume == null || volume.isEmpty() || bucket == null || bucket.isEmpty()) {
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        response.getWriter().write("Both volume and bucket parameters are required.");
        return;
      }

      String prefix = request.getParameter("prefix");
      String startItem = request.getParameter("startItem");
      String maxKeysStr = request.getParameter("maxKeys");

      int maxKeys = 100;
      if (maxKeysStr != null) {
        maxKeys = Integer.parseInt(maxKeysStr);
      }

      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.addMixIn(SnapshotInfo.class, SnapshotInfoMixin.class);
      objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
      response.setContentType("application/json; charset=utf8");
      PrintWriter writer = response.getWriter();
      try {
        ListSnapshotResponse listSnapshotResponse = om.listSnapshot(volume, bucket, prefix, startItem, maxKeys);
        writer.write(objectMapper.writeValueAsString(listSnapshotResponse.getSnapshotInfos()));
      } finally {
        if (writer != null) {
          writer.close();
        }
      }
    } catch (IOException e) {
      LOG.error("Caught an exception while processing SnapshotList request", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    } catch (Exception e) {
      LOG.error("Unexpected error while processing SnapshotList request", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }
}
