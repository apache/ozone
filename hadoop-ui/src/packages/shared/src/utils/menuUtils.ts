import React from 'react';

export type MenuItem = {
  key: string;
  label: string;
  path?: string;
  icon?: React.ReactNode;
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
  children
});

export const findSelectedKey = (
  items: MenuItem[], 
  pathname: string
): { selectedKey: string | null, header: string | null } => {
  for (const item of items) {
    if (item.path === pathname) {
      return { selectedKey: item.key, header: item.label };
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