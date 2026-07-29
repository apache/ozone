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
import React from 'react';

export type MenuItem = {
  key?: string;
  label?: string;
  path?: string;
  icon?: React.ReactNode;
  /** `group` renders a non-clickable section label; `divider` a separator line. */
  type?: 'group' | 'divider';
  /** Inline style forwarded to the underlying Ant Design menu item. */
  style?: React.CSSProperties;
  children?: MenuItem[];
};

export const getNavMenuItem = (
  label: string,
  key: string,
  path?: string,
  icon?: React.ReactNode,
  children?: MenuItem[]
): MenuItem => ({
  key,
  label,
  path,
  icon,
  children,
});

export const findSelectedKey = (
  items: MenuItem[],
  pathname: string
): {
  selectedKey: string | null;
  header: string | null;
} => {
  for (const item of items) {
    if (item.path !== undefined && item.path === pathname) {
      return { selectedKey: item.key ?? null, header: item.label ?? null };
    }
    if (item.children) {
      const result = findSelectedKey(item.children, pathname);
      if (result.selectedKey) {
        return result;
      }
    }
  }
  return { selectedKey: null, header: null };
};

/** Resolve the route path associated with a menu item key, if any. */
export const getMenuItemPath = (items: MenuItem[], key: string): string | undefined => {
  for (const item of items) {
    if (item.key === key) {
      return item.path;
    }
    if (item.children) {
      const path = getMenuItemPath(item.children, key);
      if (path !== undefined) {
        return path;
      }
    }
  }
  return undefined;
};
