/**
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

import React, { Dispatch, SetStateAction, useEffect, useState } from 'react';
import { Menu, Layout } from 'antd'
import { DashboardOutlined, DoubleLeftOutlined, EditOutlined } from '@ant-design/icons';
import { useLocation } from 'react-router-dom';
import { MenuItem, getNavMenuItem, findSelectedKey } from '../../utils/menuUtils';

type SiderProps = {
  setHeader: Dispatch<SetStateAction<string>>
};

// Logo components using the favicon
const expandedSidebarLogo = (
  <span style={{ padding: '8px 0px 8px 12px', display: 'block', alignItems: 'center' }}>
    <img 
      src="/shared/icons/favicon.ico" 
      alt="Ozone Logo" 
      style={{ 
        width: '24px', 
        height: '24px', 
        marginRight: '8px',
        display: 'inline-block',
        verticalAlign: 'middle'
      }} 
    />
    <span style={{ color: '#fff', fontSize: '16px', fontWeight: 'bold' }}>Ozone</span>
  </span>
);

const collapsedSidebarLogo = (
  <span style={{ padding: '12px 12px', display: 'block', textAlign: 'center' }}>
    <img 
      src="/shared/icons/favicon.ico" 
      alt="Ozone Logo" 
      style={{ 
        width: '24px', 
        height: '24px'
      }} 
    />
  </span>
)

const Sidebar: React.FC<SiderProps> = ({
  setHeader
}) => {

  const [collapsed, setCollapsed] = useState<boolean>(false);
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const location = useLocation();

  const navigationItems: MenuItem[] = [
    getNavMenuItem('Overview', 'Overview', '/', <DashboardOutlined />, undefined),
    getNavMenuItem('Configurations', 'Configurations', undefined, <EditOutlined />, [
      getNavMenuItem('Application Settings', 'Application Settings', '/appconfig', undefined, undefined),
      getNavMenuItem('Profile Settings', 'Profile Settings', '/profileconfig', undefined, undefined)
    ])
  ];

  useEffect(() => {
    const { selectedKey: newSelectedKey, header: newHeader } = findSelectedKey(navigationItems, location.pathname)

    if (newSelectedKey && newSelectedKey !== selectedKey) {
      setSelectedKey(newSelectedKey);
    }
    // we are sure that header will be present if selectedKey is found,
    // But just in case nothing breaks due to any change it is better to explicitly check
    // that newHeader is not null
    if (newHeader) {
      setHeader(newHeader);
    }
  }, [location.pathname, selectedKey])

  return (
    <Layout.Sider
      prefixCls='navbar'
      collapsible={true}
      collapsed={collapsed}
      collapsedWidth={56}
      onCollapse={(value: boolean) => setCollapsed(value)}
      width={'15%'}
      trigger={<DoubleLeftOutlined />}>
      {(collapsed ? collapsedSidebarLogo : expandedSidebarLogo)}
      <Menu
        theme='dark'
        defaultSelectedKeys={['Overview']}
        selectedKeys={selectedKey ? [selectedKey] : []}
        mode='inline'
        items={navigationItems}
        onSelect={({ keyPath }) => {
          setHeader([...keyPath].map(keyPath.pop, keyPath).join(' / '))
        }} />
    </Layout.Sider>
  );
};

export default Sidebar; 