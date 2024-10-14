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
import { Progress } from 'antd';
import filesize from 'filesize';
import Icon from '@ant-design/icons';
import Tooltip from 'antd/lib/tooltip';

import { FilledIcon } from '@/utils/themeIcons';
import { getCapacityPercent } from '@/utils/common';
import type { StorageReport } from '@/v2/types/overview.types';

import './storageBar.less';

const size = filesize.partial({
  standard: 'iec',
  round: 1
});

type StorageReportProps = {
  showMeta?: boolean;
  strokeWidth?: number;
} & StorageReport


const StorageBar: React.FC<StorageReportProps> = ({
  capacity = 0,
  used = 0,
  remaining = 0,
  committed = 0,
  showMeta = false,
  strokeWidth = 3
}) => {

  const nonOzoneUsed = capacity - remaining - used;
  const totalUsed = capacity - remaining;
  const tooltip = (
    <>
      <div>
        <Icon component={FilledIcon} className='ozone-used-bg-v2' />
        Ozone Used ({size(used)})
      </div>
      <div>
        <Icon component={FilledIcon} className='non-ozone-used-bg-v2' />
        Non Ozone Used ({size(nonOzoneUsed)})
      </div>
      <div>
        <Icon component={FilledIcon} className='remaining-bg-v2' />
        Remaining ({size(remaining)})
      </div>
      <div>
        <Icon component={FilledIcon} className='committed-bg-v2' />
        Container Pre-allocated ({size(committed)})
      </div>
    </>
  );

  return (
      <Tooltip
        title={tooltip}
        placement='bottomLeft'
        className='storage-cell-container-v2' >
        {(showMeta) &&
          <div>
            {size(used + nonOzoneUsed)} / {size(capacity)}
          </div>
        }
        <Progress
          strokeLinecap='round'
          percent={getCapacityPercent(totalUsed, capacity)}
          success={{ percent: getCapacityPercent(used, capacity) }}
          className='capacity-bar-v2' strokeWidth={strokeWidth} />
      </Tooltip>
  );
}


export default StorageBar;
