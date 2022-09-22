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

import {Tooltip, Button, Switch} from 'antd';
import './autoReloadPanel.less';
import {withRouter} from 'react-router-dom';
import {RouteComponentProps} from 'react-router';
import moment from 'moment';

interface IAutoReloadPanelProps extends RouteComponentProps<object> {
  onReload: () => void;
  lastRefreshed: number;
  lastUpdatedOMDBDelta:number;
  lastUpdatedOMDBFull:number;
  lastUpdatedOMLatest:number;
  isLoading: boolean;
  togglePolling: (isEnabled: boolean) => void;
}

class AutoReloadPanel extends React.Component<IAutoReloadPanelProps> {
  autoReloadToggleHandler = (checked: boolean, _event: Event) => {
    const {togglePolling} = this.props;
    togglePolling(checked);
  };

  render() {
    const {onReload, lastRefreshed, lastUpdatedOMDBDelta,lastUpdatedOMDBFull,isLoading,lastUpdatedOMLatest} = this.props;
    
     const lastRefreshedRext = lastRefreshed === 0 || lastRefreshed === undefined ? 'NA' :
      (
        <Tooltip
          placement='bottom' title={moment(lastRefreshed).format('ll LTS')}
        >
          {moment(lastRefreshed).format('LTS')}
        </Tooltip>
      );

      const omDBDeltaFullToolTip = <span>{'Last Delta Update'} : {moment(lastUpdatedOMDBDelta).format('ll LTS') }<br/>
      {'Last Full Update'} : {moment(lastUpdatedOMDBFull).format('ll LTS')}</span>

      const lastUpdatedDeltaFullToolTip = lastUpdatedOMDBDelta === 0 || lastUpdatedOMDBDelta === undefined || lastUpdatedOMDBFull === 0 || lastUpdatedOMDBFull === undefined ? 'NA' :
      (
        <Tooltip
          placement='bottom' title={omDBDeltaFullToolTip}
        >
          {moment(lastUpdatedOMLatest).format('LTS')}
        </Tooltip>
      );

     const lastUpdatedDeltaFullText = lastUpdatedOMDBDelta === 0 || lastUpdatedOMDBDelta === undefined || lastUpdatedOMDBFull===0 || lastUpdatedOMDBFull === undefined ? '' :
     (
      <>
      &nbsp; | OM DB Last updated at {lastUpdatedDeltaFullToolTip}
      &nbsp;<Button shape='circle' icon='reload' size='small' loading={isLoading} />
      </>
     );

    return (
      <div className='auto-reload-panel'>
        Auto Reload
        &nbsp;<Switch defaultChecked size='small' className='toggle-switch' onChange={this.autoReloadToggleHandler}/>
        &nbsp; | Refreshed {lastRefreshedRext}
        &nbsp;<Button shape='circle' icon='reload' size='small' loading={isLoading} onClick={onReload}/>
        {lastUpdatedDeltaFullText}
      </div>
    );
  }
}

export default withRouter(AutoReloadPanel);
