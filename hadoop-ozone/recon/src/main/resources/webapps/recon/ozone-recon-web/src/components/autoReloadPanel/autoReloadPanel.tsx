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
import moment from 'moment';
import { Tooltip, Button, Switch } from 'antd';
import { withRouter } from 'react-router-dom';
import { RouteComponentProps } from 'react-router';

import './autoReloadPanel.less';
import { PlayCircleOutlined, ReloadOutlined } from '@ant-design/icons';

interface IAutoReloadPanelProps extends RouteComponentProps<object> {
  onReload: () => void;
  lastRefreshed: number;
  lastUpdatedOMDBDelta: number;
  lastUpdatedOMDBFull: number;
  isLoading: boolean;
  omStatus: string;
  togglePolling: (isEnabled: boolean) => void;
  omSyncLoad: () => void;
}

class AutoReloadPanel extends React.Component<IAutoReloadPanelProps> {
  autoReloadToggleHandler = (checked: boolean, _event: Event) => {
    const { togglePolling } = this.props;
    togglePolling(checked);
  };

  render() {
    const { onReload, lastRefreshed, lastUpdatedOMDBDelta, lastUpdatedOMDBFull, isLoading, omSyncLoad, omStatus } = this.props;
    const autoReloadEnabled = sessionStorage.getItem('autoReloadEnabled') === 'false' ? false : true;

    const lastRefreshedText = lastRefreshed === 0 || lastRefreshed === undefined ? 'NA' :
      (
        <Tooltip
          placement='bottom' title={moment(lastRefreshed).format('ll LTS')}
        >
          {moment(lastRefreshed).format('LT')}
        </Tooltip>
      );

    const omSyncStatusDisplay = omStatus === '' ? '' : omStatus ? <div>OM DB update is successfully triggered.</div> : <div>OM DB update is already running.</div>;

    const omDBDeltaFullToolTip = <span>
      {omSyncStatusDisplay}
      {'Delta Update'}: {moment(lastUpdatedOMDBDelta).fromNow()}, {moment(lastUpdatedOMDBDelta).format('LT')}
      <br />
      {'Full Update'}: {moment(lastUpdatedOMDBFull).fromNow()}, {moment(lastUpdatedOMDBFull).format('LT')}
    </span>

    const lastUpdatedOMLatest = lastUpdatedOMDBDelta > lastUpdatedOMDBFull ? lastUpdatedOMDBDelta : lastUpdatedOMDBFull;

    const lastUpdatedDeltaFullToolTip = lastUpdatedOMDBDelta === 0 || lastUpdatedOMDBDelta === undefined || lastUpdatedOMDBFull === 0 || lastUpdatedOMDBFull === undefined ? 'NA' :
      (
        <Tooltip
          placement='bottom' title={omDBDeltaFullToolTip}
        >
          {moment(lastUpdatedOMLatest).format('LT')}
        </Tooltip>
      );

    const lastUpdatedDeltaFullText = lastUpdatedOMDBDelta === 0 || lastUpdatedOMDBDelta === undefined || lastUpdatedOMDBFull === 0 || lastUpdatedOMDBFull === undefined ? '' :
      (
        <>
          &nbsp; | DB Synced at {lastUpdatedDeltaFullToolTip}
          &nbsp;<Button shape='circle' icon={<PlayCircleOutlined />} size='small' loading={isLoading} onClick={omSyncLoad} disabled={omStatus === '' ? false : true} />
        </>
      );

    return (
      <div className='auto-reload-panel'>
        Auto Refresh
        &nbsp;<Switch defaultChecked={autoReloadEnabled} size='small' className='toggle-switch' onChange={this.autoReloadToggleHandler} />
        &nbsp; | Refreshed at {lastRefreshedText}
        &nbsp;<Button shape='circle' icon={<ReloadOutlined />} size='small' loading={isLoading} onClick={onReload} />
        {lastUpdatedDeltaFullText}
      </div>
    );
  }
}

export default withRouter(AutoReloadPanel);
