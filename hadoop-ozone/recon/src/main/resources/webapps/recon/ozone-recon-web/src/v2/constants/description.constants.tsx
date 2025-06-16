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

import React from "react";
import { InfoCircleOutlined } from "@ant-design/icons";
import { Popover } from "antd";

export const QuotaInNamespace = (
  <>
    Quota In Namespace
    <Popover
      content={
        <div>
          Number of keys and directories that can be stored in the volume or bucket. <br />
          Directories are also counted towards the quota usage.
        </div>
      }
      placement="left">
      <InfoCircleOutlined className="quota-info-icon" />
    </Popover>
  </>
);

export const QuotaUsed = (
  <>
    Quota Used
    <Popover
      content={
        <span>
          The space occupied out of the allocated quota in the volume or bucket. <br />
          This displays the unreplicated space usage.
        </span>
      }
      placement="left">
      <InfoCircleOutlined className="quota-info-icon" />
    </Popover>
  </>
);

export const QuotaAllowed = (
  <>
    Quota Allowed
    <Popover
      content="The total space that is allocated to the volume or bucket as a part of it's quota."
      placement="left">
      <InfoCircleOutlined className="quota-info-icon" />
    </Popover>
  </>
);