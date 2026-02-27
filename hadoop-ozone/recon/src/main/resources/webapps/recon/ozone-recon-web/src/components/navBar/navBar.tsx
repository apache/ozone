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
import axios from 'axios';
import {Layout, Menu} from 'antd';
import {
  BarChartOutlined,
  ClusterOutlined,
  ContainerOutlined,
  DashboardOutlined,
  DatabaseOutlined,
  DeploymentUnitOutlined,
  FolderOpenOutlined,
  InboxOutlined,
  LayoutOutlined,
  PieChartOutlined
} from '@ant-design/icons';
import {Link, withRouter} from 'react-router-dom';
import {RouteComponentProps} from 'react-router';


import logo from '../../logo.png';
import {showDataFetchError} from '@/utils/common';
import './navBar.less';

const { Sider } = Layout;

interface INavBarProps extends RouteComponentProps<object> {
  collapsed: boolean;
  onCollapse: (arg: boolean) => void;
  isHeatmapEnabled: boolean;
  isLoading: boolean;
  location: object;
}

class NavBar extends React.Component<INavBarProps> {
  constructor(props = {}) {
    super(props);
    this.state = {
      isHeatmapEnabled: false,
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
        isHeatmapEnabled: !disabledFeaturesFlag
      });
    }).catch(error => {
      this.setState({
        isLoading: false
      });
      showDataFetchError(error);
    });
  };

  render() {
    const { location } = this.props;
    const { isHeatmapEnabled } = this.state;
    const menuItems = [(
      <Menu.Item key='/Overview'
        icon={<DashboardOutlined />}>
        <span>Overview</span>
        <Link to='/Overview' />
      </Menu.Item>
    ), (
      <Menu.Item key='/Volumes'
        icon={<InboxOutlined />}>
        <span>Volumes</span>
        <Link to='/Volumes' />
      </Menu.Item>
    ), (
      <Menu.Item key='/Buckets'
        icon={<FolderOpenOutlined />}>
        <span>Buckets</span>
        <Link to='/Buckets' />
      </Menu.Item>
    ), (
      <Menu.Item key='/Datanodes'
        icon={<ClusterOutlined />}>
        <span>Datanodes</span>
        <Link to='/Datanodes' />
      </Menu.Item>
    ), (
      <Menu.Item key='/Pipelines'
        icon={<DeploymentUnitOutlined />}>
        <span>Pipelines</span>
        <Link to='/Pipelines' />
      </Menu.Item>
    ), (
      <Menu.Item key='/Containers'
        icon={<ContainerOutlined />}>
        <span>Containers</span>
        <Link to='/Containers' />
      </Menu.Item>
    ), (
      <Menu.SubMenu key='InsightsMenu'
        title="Insights"
        icon={<BarChartOutlined />}>
        <Menu.Item key='/Insights'
          icon={<BarChartOutlined />}>
          <span>Insights</span>
          <Link to='/Insights' />
        </Menu.Item>
        <Menu.Item key='/Om'
          icon={<DatabaseOutlined />}>
          <span>OM DB Insights</span>
          <Link to='/Om' />
        </Menu.Item>
      </Menu.SubMenu>
    ), (
      <Menu.Item key='/NamespaceUsage'
        icon={<PieChartOutlined />}>
        <span>Namespace Usage</span>
        <Link to='/NamespaceUsage' />
      </Menu.Item>
    ), (
      isHeatmapEnabled
        ? <>
          <Menu.Item key='/Heatmap'
            icon={<LayoutOutlined />}>
            <span>Heatmap</span>
            <Link to={{
              pathname: '/Heatmap',
              state: { isHeatmapEnabled: true }
            }}
            />
          </Menu.Item></>
        : <></>
    )]
    return (
      <Sider
        collapsible
        collapsed={this.props.collapsed}
        collapsedWidth={50}
        style={{
          overflow: 'auto',
          height: '100vh',
          position: 'fixed',
          left: 0
        }}
        onCollapse={this.props.onCollapse}
      >
        <div className='logo'>
          <img src={logo} alt='Ozone Recon Logo' width={32} height={32} />
          <span className='logo-text'>Ozone Recon</span>
        </div>
        <Menu
          theme='dark' defaultSelectedKeys={['/Dashboard']}
          mode='inline' selectedKeys={[location.pathname]}
        >
          {...menuItems}
        </Menu>
      </Sider>
    );
  }
}

export default withRouter(NavBar);
