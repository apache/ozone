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

import React, { useEffect, useState } from 'react';
import { Layout, Menu, type MenuProps } from 'antd';
import { DoubleLeftOutlined } from '@ant-design/icons';
import { useLocation, useNavigate } from 'react-router-dom';
import { MenuItem, findSelectedKey, getMenuItemPath } from '../../utils/menuUtils';

export interface SidebarProps {
  /** Navigation items to render in the rail. Each item may carry a `path`. */
  items: MenuItem[];
  /** Branding shown when the rail is expanded (e.g. a logo + product name). */
  logo?: React.ReactNode;
  /** Branding shown when the rail is collapsed (defaults to `logo`). */
  collapsedLogo?: React.ReactNode;
  /** Called with the active item's label whenever the active route changes. */
  onHeaderChange?: (header: string) => void;
  /** Controlled collapsed state. When omitted the component manages its own. */
  collapsed?: boolean;
  /** Initial collapsed state when uncontrolled. */
  defaultCollapsed?: boolean;
  /** Called whenever the collapsed state changes. */
  onCollapse?: (collapsed: boolean) => void;
  /** Expanded rail width. Defaults to `'15%'`. */
  width?: string | number;
  /** Collapsed rail width in px. Defaults to `56`. */
  collapsedWidth?: number;
}

/**
 * Application navigation rail.
 *
 * The rail is router-aware (via `react-router-dom`): it highlights the item that
 * matches the current location and navigates on selection, so applications do
 * not have to wire selection/navigation themselves. It is still fully
 * configurable — the consuming application supplies the menu `items` (with
 * `path`s) and its own `logo` — so no app-specific routes or branding are baked
 * in. Must be rendered within a react-router context (e.g. `BrowserRouter`).
 */
export const Sidebar: React.FC<SidebarProps> = ({
  items,
  logo,
  collapsedLogo,
  onHeaderChange,
  collapsed: collapsedProp,
  defaultCollapsed = false,
  onCollapse,
  width = '15%',
  collapsedWidth = 56,
}) => {
  const location = useLocation();
  const navigate = useNavigate();
  const [internalCollapsed, setInternalCollapsed] = useState<boolean>(defaultCollapsed);
  const isControlled = collapsedProp !== undefined;
  const collapsed = isControlled ? collapsedProp : internalCollapsed;

  const { selectedKey, header } = findSelectedKey(items, location.pathname);

  useEffect(() => {
    if (header) {
      onHeaderChange?.(header);
    }
  }, [header, onHeaderChange]);

  const handleCollapse = (value: boolean) => {
    if (!isControlled) {
      setInternalCollapsed(value);
    }
    onCollapse?.(value);
  };

  const handleSelect: MenuProps['onSelect'] = ({ key }) => {
    const path = getMenuItemPath(items, key);
    if (path) {
      navigate(path);
    }
  };

  const branding = collapsed ? (collapsedLogo ?? logo) : logo;

  return (
    <Layout.Sider
      prefixCls="navbar"
      collapsible
      collapsed={collapsed}
      collapsedWidth={collapsedWidth}
      onCollapse={handleCollapse}
      width={width}
      trigger={<DoubleLeftOutlined />}
    >
      {branding}
      <Menu
        theme="dark"
        mode="inline"
        items={items}
        selectedKeys={selectedKey ? [selectedKey] : []}
        onSelect={handleSelect}
      />
    </Layout.Sider>
  );
};

export default Sidebar;
