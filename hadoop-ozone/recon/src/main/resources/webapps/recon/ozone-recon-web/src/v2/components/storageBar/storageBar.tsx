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
import Tooltip from 'antd/lib/tooltip';

import { getCapacityPercent } from '@/utils/common';
import type { DatanodeStorageReport } from '@/v2/types/datanodeStorageReport.types';

import './storageBar.less';

const size = filesize.partial({
  standard: 'iec',
  round: 1
});

type StorageReportProps = {
  showMeta?: boolean;
  strokeWidth?: number;
} & DatanodeStorageReport


const StorageBar: React.FC<StorageReportProps> = ({
  capacity = 0,
  used = 0,
  remaining = 0,
  committed = 0,
  filesystemCapacity,
  filesystemUsed,
  filesystemAvailable,
  reserved,
  showMeta = false,
  strokeWidth = 3
}) => {

  const hasFilesystemView = (filesystemCapacity ?? 0) > 0 &&
    filesystemAvailable !== undefined &&
    filesystemAvailable !== null;

  const fsCap = hasFilesystemView ? (filesystemCapacity as number) : undefined;
  const fsAvail = hasFilesystemView ? (filesystemAvailable as number) : undefined;
  const fsUsed = hasFilesystemView
    ? (filesystemUsed !== undefined ? filesystemUsed : (fsCap as number) - (fsAvail as number))
    : undefined;
  const fsReserved = hasFilesystemView ? (reserved as number) : undefined;
  const tooltip = (
    <>
      <table cellPadding={5}>
        <tbody>
          {hasFilesystemView && (
            <>
              <tr>
                <td>Filesystem Capacity</td>
                <td><strong>{size(fsCap as number)}</strong></td>
              </tr>
              <tr>
                <td>Filesystem Used</td>
                <td><strong>{size(fsUsed as number)}</strong></td>
              </tr>
              <tr>
                <td>Filesystem Available</td>
                <td><strong>{size(fsAvail as number)}</strong></td>
              </tr>
              <tr>
                <td>Filesystem Reserved</td>
                <td><strong>{size(fsReserved)}</strong></td>
              </tr>
            </>
          )}
          <tr>
            <td>Ozone Capacity</td>
            <td><strong>{size(capacity)}</strong></td>
          </tr>
          <tr>
            <td>Ozone Used</td>
            <td><strong>{size(used)}</strong></td>
          </tr>
          <tr>
            <td>Ozone Available</td>
            <td><strong>{size(remaining)}</strong></td>
          </tr>
          <tr>
            <td>Container Pre-allocated</td>
            <td><strong>{size(committed)}</strong></td>
          </tr>
        </tbody>
      </table>
    </>
  );

  const percentage = getCapacityPercent((capacity - remaining), capacity)

  return (
      <Tooltip
        title={tooltip}
        placement='bottomLeft'
        className='storage-cell-container-v2' >
        {(showMeta) &&
          <div>
            {`${size(capacity - remaining)} / ${size(capacity)}`}
          </div>
        }
        <Progress
          strokeLinecap='round'
          percent={percentage}
          strokeColor={(percentage > 80) ? '#FF4D4E' : '#52C41A'}
          className={(percentage > 80) ? 'capacity-bar-v2-error' : 'capacity-bar-v2'} strokeWidth={strokeWidth} />
      </Tooltip>
  );
}


export default StorageBar;
