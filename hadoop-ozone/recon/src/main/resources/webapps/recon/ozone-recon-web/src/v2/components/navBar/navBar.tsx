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

import React, {useEffect} from 'react';
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
import {Link, useLocation} from 'react-router-dom';


import logo from '@/logo.png';
import {showDataFetchError} from '@/utils/common';
import {useApiData} from '@/v2/hooks/useAPIData.hook';

import './navBar.less';


// ------------- Types -------------- //
type NavBarProps = {
  collapsed: boolean;
  onCollapse: (arg0: boolean) => void;
}

const NavBar: React.FC<NavBarProps> = ({
  collapsed = false,
  onCollapse = () => { }
}) => {
  const location = useLocation();
  
  const { data: disabledFeatures, error } = useApiData<string[]>(
    '/api/v1/features/disabledFeatures',
    [],
    {
      onError: (error) => showDataFetchError(error)
    }
  );

  const isHeatmapEnabled = !disabledFeatures.includes('HEATMAP');

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
    isHeatmapEnabled &&
    <Menu.Item key='/Heatmap'
      icon={<LayoutOutlined />}>
      <span>Heatmap</span>
      <Link to={{
        pathname: '/Heatmap',
        state: { isHeatmapEnabled: isHeatmapEnabled }
      }}
      />
    </Menu.Item>
  )]
  return (
    <Layout.Sider
      collapsible
      collapsed={collapsed}
      collapsedWidth={50}
      style={{
        overflow: 'auto',
        height: '100vh',
        position: 'fixed',
        left: 0
      }}
      onCollapse={onCollapse}
    >
      <div className='logo-v2'>
        <img src={logo} alt='Ozone Recon Logo' width={30} height={30} />
        <span className='logo-text-v2'>Ozone Recon</span>
      </div>
      <Menu
        theme='dark'
        defaultSelectedKeys={['/Dashboard']}
        mode='inline'
        selectedKeys={[location.pathname]} >
        {...menuItems}
      </Menu>
    </Layout.Sider>
  );
}

export default NavBar;
