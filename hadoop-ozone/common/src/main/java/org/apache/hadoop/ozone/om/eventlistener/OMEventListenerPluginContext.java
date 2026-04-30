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

package org.apache.hadoop.ozone.om.eventlistener;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;

/**
 * A narrow set of functionality we are ok with exposing to plugin
 * implementations.
 */
public interface OMEventListenerPluginContext {

  boolean isLeaderReady();

  // TODO: should we allow plugins to pass in maxResults or just limit
  // them to some predefined value for safety?  e.g. 10K
  List<OmCompletedRequestInfo> listCompletedRequestInfo(Long startKey, int maxResults) throws IOException;

  // XXX: this probably doesn't belong here
  String getThreadNamePrefix();
}
