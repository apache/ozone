/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import React from 'react';
import { Result } from 'antd';

interface DisabledStateProps {
  reason: 'disabled' | 'not-configured';
}

const DisabledState: React.FC<DisabledStateProps> = ({ reason }) => {
  if (reason === 'disabled') {
    return (
      <div className="disabled-state-container">
        <Result
          status="warning"
          title="Recon AI is Disabled"
          subTitle="The chatbot feature is currently disabled in the server configuration. Please contact your administrator to enable it."
        />
      </div>
    );
  }

  return (
    <div className="disabled-state-container">
      <Result
        status="warning"
        title="Recon AI is Not Configured"
        subTitle="The chatbot feature is enabled, but no LLM client is available. Please check your API keys and provider configuration."
      />
    </div>
  );
};

export default DisabledState;
