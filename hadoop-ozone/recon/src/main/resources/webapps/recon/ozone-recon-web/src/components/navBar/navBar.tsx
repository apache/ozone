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
import { Layout, Menu, MenuProps } from 'antd';
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
import { withRouter, Link } from 'react-router-dom';
import { RouteComponentProps } from 'react-router';


import logo from '../../logo.png';
import { showDataFetchError } from '@/utils/common';
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
      showDataFetchError(error.toString());
    });
  };

  render() {
    const { location } = this.props;
    const { isHeatmapEnabled } = this.state;
    const menuItems: MenuProps['items'] = [{
      key: '/Overview',
      icon: <DashboardOutlined />,
      label: <><span>Overview</span><Link to='/Overview' /></>
    }, {
      key: '/Volumes',
      icon: <InboxOutlined />,
      label: <><span>Volumes</span><Link to='/Volumes' /></>
    }, {
      key: '/Buckets',
      icon: <FolderOpenOutlined />,
      label: <><span>Buckets</span><Link to='/Buckets' /></>
    }, {
      key: '/Datanodes',
      icon: <ClusterOutlined />,
      label: <><span>Datanodes</span><Link to='/Datanodes' /></>
    }, {
      key: '/Pipelines',
      icon: <DeploymentUnitOutlined />,
      label: <><span>Pipelines</span><Link to='/Pipelines' /></>
    }, {
      key: '/Containers',
      icon: <ContainerOutlined />,
      label: <><span>Containers</span><Link to='/Containers' /></>
    }, {
      key: 'InsightsMenu',
      icon: <BarChartOutlined />,
      label: <><span>Insights</span></>,
      children: [
        {
          key: '/Insights',
          icon: <BarChartOutlined />,
          label: <><span>Insights</span><Link to='/Insights' /></>
        },
        {
          key: '/Om',
          icon: <DatabaseOutlined />,
          label: <><span>OM DB Insights</span><Link to='/Om' /></>
        }
      ]
    }, {
      key: '/DiskUsage',
      icon: <PieChartOutlined />,
      label: <><span>Disk Usage</span><Link to='/DiskUsage' /></>
    }, isHeatmapEnabled ? {
      key: '/Heatmap',
      icon: <LayoutOutlined />,
      label: <>
        <span>Heatmap</span>
        <Link to={{
          pathname: '/Heatmap',
          state: { isHeatmapEnabled: true }
        }}
        /></>
    } : {}]
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
          items={menuItems}
        >
        </Menu>
      </Sider>
    );
  }
}

export default withRouter(NavBar);
