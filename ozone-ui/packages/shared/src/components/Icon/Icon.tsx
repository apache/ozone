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

/**
 * Names of the icons bundled with the design system. These are generic
 * line-style glyphs (24x24, `currentColor`) covering the recurring icons in the
 * Ozone screens. Rendered as inline SVG (no network requests, tree-shakeable).
 */
export type IconName =
  | 'dashboard'
  | 'health'
  | 'settings'
  | 'docs'
  | 'service'
  | 'reports'
  | 'notifications'
  | 'user'
  | 'search'
  | 'close'
  | 'menu'
  | 'chevron-right'
  | 'chevron-left'
  | 'chevron-down'
  | 'chevron-up'
  | 'external-link'
  | 'copy';

/** SVG path data for each icon, drawn on a 24x24 viewBox with `currentColor`. */
const paths: Record<IconName, React.ReactNode> = {
  dashboard: <path d="M4 13h6V4H4v9Zm0 7h6v-5H4v5Zm10 0h6v-9h-6v9Zm0-16v5h6V4h-6Z" />,
  health: (
    <path
      d="M3 12h4l2 5 4-12 2 7h6"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  ),
  settings: (
    <path
      d="M12 15.5a3.5 3.5 0 1 0 0-7 3.5 3.5 0 0 0 0 7Zm7.4-2.5a5.6 5.6 0 0 0 0-2l2-1.6-2-3.4-2.4 1a5.7 5.7 0 0 0-1.7-1L14 3h-4l-.3 2.5a5.7 5.7 0 0 0-1.7 1l-2.4-1-2 3.4L3.6 11a5.6 5.6 0 0 0 0 2l-2 1.6 2 3.4 2.4-1c.5.4 1.1.7 1.7 1L10 21h4l.3-2.5c.6-.3 1.2-.6 1.7-1l2.4 1 2-3.4-2-1.6Z"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.6"
      strokeLinejoin="round"
    />
  ),
  docs: (
    <path
      d="M6 2h8l4 4v16H6V2Zm8 1.5V7h3.5M8 12h8M8 16h8M8 8h3"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.6"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  ),
  service: (
    <path
      d="M4 7h16M4 12h16M4 17h16"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
    />
  ),
  reports: (
    <path
      d="M5 21V9m7 12V3m7 18v-7"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
    />
  ),
  notifications: (
    <path
      d="M12 22a2.5 2.5 0 0 0 2.5-2.5h-5A2.5 2.5 0 0 0 12 22Zm7-6-2-2v-4a5 5 0 0 0-10 0v4l-2 2v1h14v-1Z"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.6"
      strokeLinejoin="round"
    />
  ),
  user: (
    <path
      d="M12 12a4 4 0 1 0 0-8 4 4 0 0 0 0 8Zm-7 8a7 7 0 0 1 14 0"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.6"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  ),
  search: (
    <path
      d="m21 21-4.3-4.3M11 18a7 7 0 1 0 0-14 7 7 0 0 0 0 14Z"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.8"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  ),
  close: (
    <path
      d="M6 6l12 12M18 6 6 18"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
    />
  ),
  menu: (
    <path
      d="M4 6h16M4 12h16M4 18h16"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
    />
  ),
  'chevron-right': (
    <path
      d="m9 6 6 6-6 6"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  ),
  'chevron-left': (
    <path
      d="m15 6-6 6 6 6"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  ),
  'chevron-down': (
    <path
      d="m6 9 6 6 6-6"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  ),
  'chevron-up': (
    <path
      d="m6 15 6-6 6 6"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  ),
  'external-link': (
    <path
      d="M14 5h5v5M19 5l-7 7M12 5H6a1 1 0 0 0-1 1v12a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-6"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.6"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  ),
  copy: (
    <path
      d="M9 9V5a1 1 0 0 1 1-1h9a1 1 0 0 1 1 1v9a1 1 0 0 1-1 1h-4M4 10a1 1 0 0 1 1-1h9a1 1 0 0 1 1 1v9a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1v-9Z"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.6"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  ),
};

export interface IconProps extends Omit<React.SVGProps<SVGSVGElement>, 'name'> {
  /** Which bundled icon to render. */
  name: IconName;
  /** Pixel size (width and height). Defaults to 16. */
  size?: number;
}

/**
 * Renders a design-system icon as inline SVG using `currentColor`, so it
 * inherits the surrounding text colour and can be sized via the `size` prop.
 */
export const Icon: React.FC<IconProps> = ({ name, size = 16, style, ...rest }) => (
  <svg
    width={size}
    height={size}
    viewBox="0 0 24 24"
    fill="currentColor"
    aria-hidden={rest['aria-label'] ? undefined : true}
    role={rest['aria-label'] ? 'img' : undefined}
    style={{ display: 'inline-block', verticalAlign: 'middle', flexShrink: 0, ...style }}
    {...rest}
  >
    {paths[name]}
  </svg>
);

export default Icon;
