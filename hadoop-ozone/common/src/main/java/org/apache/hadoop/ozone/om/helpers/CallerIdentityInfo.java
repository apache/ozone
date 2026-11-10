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

package org.apache.hadoop.ozone.om.helpers;

import java.util.Objects;
import net.jcip.annotations.Immutable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetCallerIdentityResponse;

/**
 * Utility class to handle GetCallerIdentityResponse protobuf message.
 */
@Immutable
public class CallerIdentityInfo {

  private final String account;
  private final String arn;
  private final String userId;

  public CallerIdentityInfo(String account, String arn, String userId) {
    this.account = account;
    this.arn = arn;
    this.userId = userId;
  }

  public String getAccount() {
    return account;
  }

  public String getArn() {
    return arn;
  }

  public String getUserId() {
    return userId;
  }

  public static CallerIdentityInfo fromProtobuf(GetCallerIdentityResponse response) {
    return new CallerIdentityInfo(response.getAccount(), response.getArn(), response.getUserId());
  }

  public GetCallerIdentityResponse getProtobuf() {
    return GetCallerIdentityResponse.newBuilder()
        .setAccount(account)
        .setArn(arn)
        .setUserId(userId)
        .build();
  }

  @Override
  public String toString() {
    return "CallerIdentityInfo{" + "account='" + account + "', arn='" + arn + "', userId='" + userId + "'}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final CallerIdentityInfo that = (CallerIdentityInfo) o;
    return Objects.equals(account, that.account) && Objects.equals(arn, that.arn) &&
        Objects.equals(userId, that.userId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(account, arn, userId);
  }
}
