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
import logo from '../../logo.png';
import {Layout, Menu, Icon} from 'antd';
import './navBar.less';
import {withRouter, Link} from 'react-router-dom';
import {RouteComponentProps} from 'react-router';
import axios from 'axios';
import {showDataFetchError} from 'utils/common';

const {Sider} = Layout;

interface INavBarProps extends RouteComponentProps<object> {
  collapsed: boolean;
  onCollapse: (arg: boolean) => void;
  isHeatmapAvailable: boolean;
  isLoading: boolean;
}

class NavBar extends React.Component<INavBarProps> {
  constructor(props = {}) {
    super(props);
    this.state = {
      isHeatmapAvailable: false,
      isLoading: false
    };
  }

  componentDidMount(): void {
    this.setState({
      isLoading: true
    });
    this.fetchDisableFeatures();
  }
  
  fetchDisableFeatures = () => {
    this.setState({
      isLoading: true
    });

    const disabledfeaturesEndpoint = `/api/v1/features/disabledFeatures`;
    axios.get(disabledfeaturesEndpoint).then(response => {
      const disabledFeaturesFlag = response.data && response.data.includes('HEATMAP');
      // If disabledFeaturesFlag is true then disable Heatmap Feature in Ozone Recon
      this.setState({
        isLoading: false,
        isHeatmapAvailable: !disabledFeaturesFlag
      });
    }).catch(error => {
      this.setState({
        isLoading: false
      });
      showDataFetchError(error.toString());
    });
  };

  render() {
    const {location} = this.props;
    return (
      <Sider
        collapsible
        collapsed={this.props.collapsed}
        collapsedWidth={50}
        style={{
          overflow: 'auto', height: '100vh', position: 'fixed', left: 0
        }}
        onCollapse={this.props.onCollapse}
      >
        <div className='logo'>
          <img src={logo} alt='Ozone Recon Logo' width={32} height={32}/>
          <span className='logo-text'>Ozone Recon</span>
        </div>
        <Menu
          theme='dark' defaultSelectedKeys={['/Dashboard']}
          mode='inline' selectedKeys={[location.pathname]}
        >
          <Menu.Item key='/Overview'>
            <Icon type='dashboard'/>
            <span>Overview</span>
            <Link to='/Overview'/>
          </Menu.Item>
          <Menu.Item key='/Datanodes'>
            <Icon type='cluster'/>
            <span>Datanodes</span>
            <Link to='/Datanodes'/>
          </Menu.Item>
          <Menu.Item key='/Pipelines'>
            <Icon type='deployment-unit'/>
            <span>Pipelines</span>
            <Link to='/Pipelines'/>
          </Menu.Item>
          <Menu.Item key='/Containers'>
            <Icon type='container'/>
            <span>Containers</span>
            <Link to='/Containers'/>
          </Menu.Item>
          <Menu.Item key='/Insights'>
            <Icon type='bar-chart'/>
            <span>Insights</span>
            <Link to='/Insights'/>
          </Menu.Item>
          <Menu.Item key='/DiskUsage'>
            <Icon type='pie-chart'/>
            <span>Disk Usage</span>
            <Link to='/DiskUsage'/>
          </Menu.Item>
          {
            this.state.isHeatmapAvailable ?
              <Menu.Item key='/Heatmap'>
                <Icon type='bar-chart' />
                <span>Heatmap</span>
                <Link to='/Heatmap' />
              </Menu.Item> : ""
          }
        </Menu>
      </Sider>
    );
  }
}

export default withRouter(NavBar);
