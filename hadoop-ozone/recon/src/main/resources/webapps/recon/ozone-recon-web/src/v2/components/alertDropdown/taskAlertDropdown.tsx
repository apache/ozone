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

import { TaskStatus, TaskStatusResponse } from '@/v2/types/overview.types';
import { BellOutlined, CheckCircleFilled, CloseCircleFilled, WarningFilled } from '@ant-design/icons';
import { Badge, Button, Dropdown, Menu } from 'antd';
import React, { useEffect } from 'react';

//----------Types---------//
type TaskAlertDropdownProps = {
  data: TaskStatusResponse;
  failedTasks: number;
  notStartedTasks: number
}

const TaskAlertDropdown: React.FC<TaskAlertDropdownProps> = ({ data, failedTasks, notStartedTasks }) => {

  function getTaskIcon(task: TaskStatus) {
    if (task.lastTaskSuccessful === null) {
      return <WarningFilled style={{ color: '#E49F00' }} />;
    }
    if (!task.lastTaskSuccessful) {
      return <CloseCircleFilled style={{ color: '#ff595e' }} />;
    }
    return <CheckCircleFilled style={{ color: '#1AA57A' }} />;
  }

  function generateMenu(tasks: TaskStatusResponse) {
    return <Menu style={{ display: 'table' }}>
      {...tasks.map(task => (
        <Menu.Item>
          {
            getTaskIcon(task)
          }
          { task.taskName }
        </Menu.Item>
      ))}
    </Menu>
  }

  return (
    <Dropdown overlay={generateMenu(data)} trigger={['click']}>
      <Badge count={
        failedTasks >= notStartedTasks
          ? failedTasks
          : notStartedTasks
        } style={{ backgroundColor: 
          failedTasks >= notStartedTasks
            ? '#FF595E'
            : '#E49F00'
        }}>
          <BellOutlined style={{ fontSize: '20px' }}/>
        </Badge>
    </Dropdown>
  );
}

export default TaskAlertDropdown;