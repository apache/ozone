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

import { DisconnectOutlined } from '@ant-design/icons';
import React from 'react';

type CapacityDetailErrorProps = {
  message?: string;
  testId?: string;
}

const CapacityDetailError: React.FC<CapacityDetailErrorProps> = ({
  message = 'Pending deletion details are currently unavailable.',
  testId
}) => {
  return (
    <div className='capacity-detail-error' data-testid={testId}>
      <DisconnectOutlined className='capacity-detail-error-icon' />
      <span className='capacity-detail-error-message'>{message}</span>
    </div>
  );
};

export default CapacityDetailError;
