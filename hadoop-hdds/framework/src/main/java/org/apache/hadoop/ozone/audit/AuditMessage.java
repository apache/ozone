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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.audit;

import org.apache.logging.log4j.message.Message;

import java.util.Map;

/**
 * Defines audit message structure.
 */
public final class AuditMessage implements Message {

  private final String message;
  private final String user;
  private final String ip;
  private final String op;
  private final Map<String, String> params;
  private final String ret;
  private final Throwable throwable;

  private AuditMessage(String user, String ip, String op,
      Map<String, String> params, String ret, Throwable throwable) {
    this.user = user;
    this.ip = ip;
    this.op = op;
    this.params = params;
    this.ret = ret;
    this.message = formMessage(user, ip, op, params, ret);
    this.throwable = throwable;
  }

  @Override
  public String getFormattedMessage() {
    return message;
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

    public Builder withResult(AuditEventStatus result) {
      this.ret = result.getStatus();
      return this;
    }

    public Builder withException(Throwable ex) {
      this.throwable = ex;
      return this;
    }

    public AuditMessage build() {
      return new AuditMessage(user, ip, op, params, ret, throwable);
    }
  }

  private String formMessage(String userStr, String ipStr, String opStr,
      Map<String, String> paramsMap, String retStr) {
    return "user=" + userStr + " | ip=" + ipStr + " | " + "op=" + opStr
        + " " + paramsMap + " | " + "ret=" + retStr;

  }
}
