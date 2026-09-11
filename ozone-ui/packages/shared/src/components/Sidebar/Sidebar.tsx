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
import { Layout, Menu, Space, type MenuProps } from 'antd';
import { useLocation, useNavigate } from 'react-router-dom';
import { MenuItem, findSelectedKey, getMenuItemPath } from '../../utils/menuUtils';
import { radius, semanticColors, spacing } from '../../theme/tokens';

const CollapseIcon: React.FC<{ collapsed: boolean }> = ({ collapsed }) => (
  <svg
    width={20}
    height={20}
    viewBox="0 0 20 20"
    fill="currentColor"
    aria-hidden
    style={{
      pointerEvents: 'none',
      transform: collapsed ? 'rotate(180deg)' : undefined,
      transition: 'transform 0.2s ease',
    }}
  >
    <path
      fillRule="evenodd"
      clipRule="evenodd"
      d="M15.5167 3.33337L8.93087 9.96337L15.5125 16.6667L16.6667 15.43L11.3067 9.97004L16.6617 4.57837L15.5167 3.33337ZM9.91754 3.33337L3.33337 9.96337L9.91421 16.6667L11.0684 15.43L5.70837 9.97004L11.0642 4.57837L9.91754 3.33337Z"
    />
  </svg>
);

/**
 * Mark the selected leaf item with a left accent bar and keep every other leaf
 * aligned with a matching transparent border, so selection reads as a small
 * indicator rather than a full-row highlight.
 */
const decorateItems = (items: MenuItem[], selectedKey: string | null): MenuItem[] =>
  items.map((item) => {
    if (item.type === 'divider') {
      return item;
    }
    if (item.children) {
      return { ...item, children: decorateItems(item.children, selectedKey) };
    }
    const isSelected = item.key != null && item.key === selectedKey;
    return {
      ...item,
      style: {
        // Square accent bar on the left, hover/selection pill rounded on the
        // right only, with a small right inset so the rounding is visible.
        borderLeft: `3px solid ${isSelected ? semanticColors.navIndicator : 'transparent'}`,
        borderRadius: `0 ${radius.lg}px ${radius.lg}px 0`,
        marginInlineEnd: spacing.sm,
        ...item.style,
      },
    };
  });

/**
 * Split top-level items into sections at group boundaries (and any dividers),
 * so each section renders as its own menu with vertical spacing (an antd
 * `Space`) between them — matching the grouped rail in the design.
 */
const splitSections = (items: MenuItem[]): MenuItem[][] => {
  const sections: MenuItem[][] = [];
  let current: MenuItem[] = [];
  const flush = () => {
    if (current.length) {
      sections.push(current);
      current = [];
    }
  };
  for (const item of items) {
    if (item.type === 'group') {
      flush();
      sections.push([item]);
    } else if (item.type === 'divider') {
      flush();
    } else {
      current.push(item);
    }
  }
  flush();
  return sections;
};

/**
 * Flatten to just the leaf items (dropping group wrappers/titles and dividers).
 * Used when the rail is collapsed so it reads as a compact icon-only list with
 * no section headings.
 */
const flattenLeaves = (items: MenuItem[]): MenuItem[] => {
  const out: MenuItem[] = [];
  for (const item of items) {
    if (item.type === 'divider') {
      continue;
    }
    if (item.children) {
      out.push(...flattenLeaves(item.children));
    } else {
      out.push(item);
    }
  }
  return out;
};

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

  // Collapsed: one compact icon-only list (no group titles or section gaps).
  // Expanded: grouped sections spaced apart with an antd Space.
  const decorated = decorateItems(items, selectedKey);
  const sections = collapsed ? [flattenLeaves(decorated)] : splitSections(decorated);

  return (
    <Layout.Sider
      prefixCls="navbar"
      collapsible
      collapsed={collapsed}
      collapsedWidth={collapsedWidth}
      onCollapse={handleCollapse}
      width={width}
      trigger={null}
    >
      <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
        {branding}
        {/* One menu per section, spaced apart with an antd Space (vertical). */}
        <Space
          direction="vertical"
          size={spacing.lg}
          style={{ display: 'flex', flex: 1, width: '100%' }}
        >
          {sections.map((section, index) => (
            <Menu
              key={section[0]?.key ?? `section-${index}`}
              theme="light"
              mode="inline"
              style={{ background: 'transparent', borderInlineEnd: 'none', width: '100%' }}
              items={section as MenuProps['items']}
              selectedKeys={selectedKey ? [selectedKey] : []}
              onSelect={handleSelect}
            />
          ))}
        </Space>
        {/* Left-aligned collapse trigger that blends into the rail surface. */}
        <div
          role="button"
          tabIndex={0}
          aria-label={collapsed ? 'Expand navigation' : 'Collapse navigation'}
          onClick={() => handleCollapse(!collapsed)}
          onKeyDown={(e) => {
            if (e.key === 'Enter' || e.key === ' ') {
              e.preventDefault();
              handleCollapse(!collapsed);
            }
          }}
          style={{
            flexShrink: 0,
            display: 'flex',
            alignItems: 'center',
            // Keep the trigger left-aligned in both states so the chevron flips
            // in place rather than sliding across as the rail width animates.
            justifyContent: 'flex-start',
            height: 48,
            paddingInline: spacing.lg,
            cursor: 'pointer',
            color: semanticColors.navItemColor,
            background: 'transparent',
            userSelect: 'none',
          }}
        >
          <CollapseIcon collapsed={collapsed} />
        </div>
      </div>
    </Layout.Sider>
  );
};

export default Sidebar;
