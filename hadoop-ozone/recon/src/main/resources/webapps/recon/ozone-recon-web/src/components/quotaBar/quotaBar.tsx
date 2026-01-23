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
import Icon from '@ant-design/icons';
import { withRouter } from 'react-router-dom';
import { RouteComponentProps } from 'react-router';
import Tooltip from 'antd/lib/tooltip';
import filesize from 'filesize';

import { FilledIcon } from '@/utils/themeIcons';
import { getCapacityPercent } from '@/utils/common';
import './quotaBar.less';

const size = filesize.partial({ standard: 'iec' });

interface IQuotaBarProps extends RouteComponentProps<object> {
  quota: number;
  used: number;
  quotaType: string;
  showMeta?: boolean;
}

const defaultProps = {
  quota: -1,
  used: 0,
  quotaType: 'namespace',
  showMeta: true
};

class QuotaBar extends React.Component<IQuotaBarProps> {
  static defaultProps = defaultProps;

  render() {
    const { quota, used, quotaType, showMeta } = this.props;
    const remaining = quota - used;

    const renderQuota = (quota: number) => {
      // Quota not set / invalid
      if (quota <= -1) {
        return '-';
      }

      if (quotaType === 'size') {
        return size(quota);
      }

      return quota;
    };

    const tooltip = (
      <div>
        <div><Icon component={FilledIcon} className='quota-used-bg'/> Used ({renderQuota(used)})</div>
        <div><Icon component={FilledIcon} className='quota-remaining-bg'/> Remaining ({renderQuota(remaining)})</div>
      </div>
    );
    const metaElement = showMeta ? <div>{renderQuota(used)} / {renderQuota(quota)}</div> : null;
    return (
      <div className='quota-cell-container'>
        <Tooltip title={tooltip} placement='bottomLeft'>
          {metaElement}
          <Progress
            type='line'
            status='normal'
            strokeLinecap='square'
            percent={getCapacityPercent(used, quota)}
            className='capacity-bar' strokeWidth={3}/>
        </Tooltip>
      </div>
    );
  }
}

export default withRouter(QuotaBar);
