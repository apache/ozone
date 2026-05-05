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

package org.apache.hadoop.ozone.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.hadoop.ozone.audit.AuditLogger.PerformanceStringBuilder;
import org.apache.logging.log4j.message.Message;
import org.apache.ratis.util.MemoizedSupplier;

/**
 * Defines audit message structure.
 */
public final class AuditMessage implements Message {

  private static final long serialVersionUID = 1L;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final transient Supplier<String> messageSupplier;
  private final String op;
  private final Throwable throwable;

  private AuditMessage(String user, String ip, String op,
      Map<String, String> params, String ret, Throwable throwable,
      PerformanceStringBuilder performance) {
    this.op = op;
    this.messageSupplier = MemoizedSupplier.valueOf(
        () -> formMessage(user, ip, op, params, ret, performance));
    this.throwable = throwable;
  }

  @Override
  public String getFormattedMessage() {
    return messageSupplier.get();
  }

  @Override
  public String getFormat() {
    return null;
  }

  @Override
  public Object[] getParameters() {
    return new Object[0];
  }

  @Override
  public Throwable getThrowable() {
    return throwable;
  }

  public String getOp() {
    return op;
  }

  /**
   * Builder class for AuditMessage.
   */
  public static class Builder {
    private Throwable throwable;
    private String user;
    private String ip;
    private String op;
    private Map<String, String> params;
    private String ret;
    private PerformanceStringBuilder performance;

    public Builder setUser(String usr) {
      this.user = usr;
      return this;
    }

    public Builder atIp(String ipAddr) {
      this.ip = ipAddr;
      return this;
    }

    public Builder forOperation(AuditAction action) {
      this.op = action.getAction();
      return this;
    }

    public Builder withParams(Map<String, String> args) {
      this.params = args;
      return this;
    }

    public Map<String, String> getParams() {
      return params;
    }

    public Builder withResult(AuditEventStatus result) {
      this.ret = result.getStatus();
      return this;
    }

    public Builder withException(Throwable ex) {
      this.throwable = ex;
      return this;
    }

    public Builder setPerformance(PerformanceStringBuilder perf) {
      this.performance = perf;
      return this;
    }

    public AuditMessage build() {
      return new AuditMessage(user, ip, op, params, ret, throwable,
          performance);
    }
  }

  private String formMessage(String userStr, String ipStr, String opStr,
      Map<String, String> paramsMap, String retStr,
      PerformanceStringBuilder performanceMap) {
    String perf = performanceMap != null
        ? " | perf=" + performanceMap.build() : "";
    String params = formatParamsAsJson(paramsMap);
    return "user=" + userStr + " | ip=" + ipStr + " | " + "op=" + opStr
        + " " + params + " | ret=" + retStr + perf;
  }

  private String formatParamsAsJson(Map<String, String> paramsMap) {
    if (paramsMap == null || paramsMap.isEmpty()) {
      return "{}";
    }
    try {
      return OBJECT_MAPPER.writeValueAsString(paramsMap);
    } catch (Exception e) {
      return paramsMap.toString();
    }
  }
}
