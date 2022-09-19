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
  lastUpdated: number;
  lastUpdatedOMDBDelta:number;
  lastUpdatedOMDBFull:number;
  lastUpdatedOMDBDeltaText:string;
  lastUpdatedOMDBFullText:string;
  isLoading: boolean;
  togglePolling: (isEnabled: boolean) => void;
}

class AutoReloadPanel extends React.Component<IAutoReloadPanelProps> {
  autoReloadToggleHandler = (checked: boolean, _event: Event) => {
    const {togglePolling} = this.props;
    togglePolling(checked);
  };

  render() {
    const {onReload, lastUpdated, lastUpdatedOMDBDelta,lastUpdatedOMDBFull,isLoading,lastUpdatedOMDBDeltaText,lastUpdatedOMDBFullText} = this.props;
    
     const lastUpdatedText = lastUpdated === 0 || lastUpdated === undefined ? 'NA' :
      (
        <Tooltip
          placement='bottom' title={moment(lastUpdated).format('ll LTS')}
        >
          {moment(lastUpdated).format('LTS')}
        </Tooltip>
      );

      const omDBDeltaFullToolTip = <span>{lastUpdatedOMDBDeltaText} : {moment(lastUpdatedOMDBDelta).format('ll LTS') }<br/>
      {lastUpdatedOMDBFullText} : {moment(lastUpdatedOMDBFull).format('ll LTS')}</span>

      const lastUpdatedDeltaToolTip = lastUpdatedOMDBDelta === 0 || lastUpdatedOMDBDelta === undefined || lastUpdatedOMDBFull === 0 || lastUpdatedOMDBFull === undefined ? 'NA' :
      (
        <Tooltip
          placement='bottom' title={omDBDeltaFullToolTip}
        >
          {moment(lastUpdatedOMDBDelta).format('LTS')}
        </Tooltip>
      );

     const lastUpdatedDeltaText = lastUpdatedOMDBDelta === 0 || lastUpdatedOMDBDelta === undefined || lastUpdatedOMDBFull===0 || lastUpdatedOMDBFull === undefined ? '' :
     (
      <>
      &nbsp; | OM DB Last updated at {lastUpdatedDeltaToolTip}
      &nbsp;<Button shape='circle' icon='reload' size='small' loading={isLoading} />
      </>
     );

    return (
      <div className='auto-reload-panel'>
        Auto Reload
        &nbsp;<Switch defaultChecked size='small' className='toggle-switch' onChange={this.autoReloadToggleHandler}/>
        &nbsp; | Last updated at {lastUpdatedText}
        &nbsp;<Button shape='circle' icon='reload' size='small' loading={isLoading} onClick={onReload}/>
        {lastUpdatedDeltaText}
      </div>
    );
  }
}

export default withRouter(AutoReloadPanel);
