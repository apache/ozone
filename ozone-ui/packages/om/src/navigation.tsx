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

import { type MenuItem } from '@ozone-ui/shared';
import {
  ApiOutlined,
  BarChartOutlined,
  BlockOutlined,
  BookOutlined,
  ClusterOutlined,
  ControlOutlined,
  DashboardOutlined,
  HistoryOutlined,
} from '@ant-design/icons';

/** Common footprint for the navigation glyphs. */
const ICON_SIZE = 18;
const iconStyle = { fontSize: ICON_SIZE };

/** A leaf navigation item paired with the icon it renders in the rail. */
const navItem = (key: string, label: string, path: string, icon: MenuItem['icon']): MenuItem => ({
  key,
  label,
  path,
  icon,
});

/**
 * Ozone Manager navigation rail. Mirrors the "Sidebar Navigation" in the design:
 * primary items, then a "Diagnostics" group and a "Links" group.
 */
export const navItems: MenuItem[] = [
  navItem('overview', 'Overview', '/', <DashboardOutlined style={iconStyle} />),
  navItem(
    'configuration',
    'Configuration',
    '/configuration',
    <ControlOutlined style={iconStyle} />
  ),
  {
    type: 'group',
    key: 'group-diagnostics',
    label: 'Diagnostics',
    children: [
      navItem('rpc', 'Remote Procedure Call', '/rpc', <ApiOutlined style={iconStyle} />),
      navItem(
        'ozone-manager',
        'Ozone Manager',
        '/ozone-manager',
        <ClusterOutlined style={iconStyle} />
      ),
      navItem('jmx', 'JMX', '/jmx-info', <BarChartOutlined style={iconStyle} />),
      navItem('stacks', 'Stacks', '/stacks', <BlockOutlined style={iconStyle} />),
    ],
  },
  {
    type: 'group',
    key: 'group-links',
    label: 'Links',
    children: [
      navItem(
        'documentation',
        'Documentation',
        '/documentation',
        <BookOutlined style={iconStyle} />
      ),
      navItem('log-levels', 'Log levels', '/log-levels', <HistoryOutlined style={iconStyle} />),
    ],
  },
];

/** Product branding shown in the top utility bar. */
export const SIDEBAR_WIDTH = 248;
