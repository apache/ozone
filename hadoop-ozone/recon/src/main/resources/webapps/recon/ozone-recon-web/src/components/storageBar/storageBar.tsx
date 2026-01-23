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
import { withRouter } from 'react-router-dom';
import { RouteComponentProps } from 'react-router';
import Tooltip from 'antd/lib/tooltip';

import { FilledIcon } from '@/utils/themeIcons';
import { getCapacityPercent } from '@/utils/common';
import './storageBar.less';

const size = filesize.partial({
  standard: 'iec',
  round: 1
});

interface IStorageBarProps extends RouteComponentProps<object> {
  total: number;
  used: number;
  remaining: number;
  committed: number;
  showMeta?: boolean;
}

const defaultProps = {
  total: 0,
  used: 0,
  remaining: 0,
  committed: 0,
  showMeta: true
};

class StorageBar extends React.Component<IStorageBarProps> {
  static defaultProps = defaultProps;

  render() {
    const { total, used, remaining, committed, showMeta } = this.props;
    const nonOzoneUsed = total - remaining - used;
    const totalUsed = total - remaining;
    const tooltip = (
      <div>
        <div><Icon component={FilledIcon} className='ozone-used-bg' /> Ozone Used ({size(used)})</div>
        <div><Icon component={FilledIcon} className='non-ozone-used-bg' /> Non Ozone Used ({size(nonOzoneUsed)})</div>
        <div><Icon component={FilledIcon} className='remaining-bg' /> Remaining ({size(remaining)})</div>
        <div><Icon component={FilledIcon} className='committed-bg' /> Container Pre-allocated ({size(committed)})</div>
      </div>
    );
    const metaElement = showMeta ? <div>{size(used)} + {size(nonOzoneUsed)} / {size(total)}</div> : null;
    return (
      <div className='storage-cell-container'>
        <Tooltip title={tooltip} placement='bottomLeft'>
          {metaElement}
          <Progress
            strokeLinecap='round'
            percent={getCapacityPercent(totalUsed, total)}
            success={{ percent: getCapacityPercent(used, total) }}
            className='capacity-bar' strokeWidth={3} />
        </Tooltip>
      </div>
    );
  }
}

export default withRouter(StorageBar);
