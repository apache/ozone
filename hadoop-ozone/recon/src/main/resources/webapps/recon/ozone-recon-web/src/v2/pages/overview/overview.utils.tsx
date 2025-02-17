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
import React, { ReactElement } from 'react';
import { Tooltip } from "antd";
import {
  CheckCircleFilled,
  CloseCircleFilled,
  ExclamationCircleFilled,
  WarningFilled
} from "@ant-design/icons";

export const getHealthIcon = (value: string): React.ReactElement => {
  const values = value.split('/');
  if (values.length == 2 && values[0] < values[1]) {
    return (
      <>
        <div className='icon-warning' style={{
          fontSize: '20px',
          alignItems: 'center'
        }}>
          <WarningFilled style={{
            marginRight: '5px'
          }} />
          Unhealthy
        </div>
      </>
    )
  }
  return (
    <div className='icon-success' style={{
      fontSize: '20px',
      alignItems: 'center'
    }}>
      <CheckCircleFilled style={{
        marginRight: '5px'
      }} />
      Healthy
    </div>
  )
}

export function getTaskIndicatorIcon(taskRunning: boolean, taskStatus: boolean): ReactElement {
  if (taskRunning) {
    return (
      <Tooltip title="OmTableInsight task is still running, values might be outdated">
        <ExclamationCircleFilled
          style={{
            color: '#FBC02D',
            fontSize: '12px',
            paddingRight: '10px'
          }} />
      </Tooltip>
    );
  }
  if (taskStatus) {
    return (
      <Tooltip title="OmTableInsight task completed">
        <CheckCircleFilled
          style={{
            color: '#00C853',
            fontSize: '12px',
            paddingRight: '10px'
          }} />
      </Tooltip>
    );
  }
  return (
    <Tooltip title="OmTableInisght task failed, value is inaccurate">
      <CloseCircleFilled style={{ color: '#F44336', fontSize: '12px', paddingRight: '10px' }} />
    </Tooltip>
  );
}