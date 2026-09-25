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
  CameraOutlined,
  ClusterOutlined,
  ControlOutlined,
  DashboardOutlined,
  DeleteOutlined,
  FileTextOutlined,
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
 * Overview and Configuration at the top, then a "Metrics" group of per-subsystem
 * metrics views and a "Common tools" group. The OM Metrics page is the
 * "Ozone Manager" item under the Metrics group.
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
    key: 'group-metrics',
    label: 'Metrics',
    children: [
      navItem('rpc', 'Remote Procedure Call', '/metrics/rpc', <ApiOutlined style={iconStyle} />),
      navItem(
        'ratis-event-timeline',
        'Ratis Event Timeline',
        '/metrics/ratis-event-timeline',
        <HistoryOutlined style={iconStyle} />
      ),
      navItem(
        'om-metrics',
        'Ozone Manager',
        '/metrics/ozone-manager',
        <ClusterOutlined style={iconStyle} />
      ),
      navItem('deletion', 'Deletion', '/metrics/deletion', <DeleteOutlined style={iconStyle} />),
      navItem('snapshots', 'Snapshots', '/metrics/snapshots', <CameraOutlined style={iconStyle} />),
    ],
  },
  {
    type: 'group',
    key: 'group-common-tools',
    label: 'Common tools',
    children: [
      navItem('jmx', 'JMX', '/jmx', <BarChartOutlined style={iconStyle} />),
      navItem('stacks', 'Stacks', '/stacks', <BlockOutlined style={iconStyle} />),
      navItem('log-levels', 'Log Levels', '/log-levels', <FileTextOutlined style={iconStyle} />),
    ],
  },
];

/** Expanded width of the navigation rail, in px. */
export const SIDEBAR_WIDTH = 248;
