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
import { Button, Tooltip, type ButtonProps } from 'antd';

export interface IconButtonProps extends Omit<ButtonProps, 'icon' | 'shape' | 'children' | 'size'> {
  /** The icon to render (e.g. an `<Icon />` or an Ant Design icon). */
  icon: React.ReactNode;
  /** Accessible label; also used as the tooltip when `tooltip` is not set. */
  label: string;
  /** Optional tooltip text (defaults to `label`). Pass `null` to disable. */
  tooltip?: string | null;
  /** Button footprint. `large` = 40px, `standard` (default) = 32px. */
  size?: 'large' | 'standard';
}

/**
 * Square icon-only button. Wraps Ant Design's `Button` with a consistent
 * footprint, an accessible label and an optional tooltip. Matches the
 * "Icon Button" component used in the top bar, tables and toolbars.
 */
export const IconButton: React.FC<IconButtonProps> = ({
  icon,
  label,
  tooltip,
  size = 'standard',
  type = 'text',
  style,
  ...rest
}) => {
  const dimension = size === 'large' ? 40 : 32;
  const button = (
    <Button
      type={type}
      aria-label={label}
      icon={icon}
      style={{
        width: dimension,
        height: dimension,
        display: 'inline-flex',
        alignItems: 'center',
        justifyContent: 'center',
        padding: 0,
        ...style,
      }}
      {...rest}
    />
  );

  const tip = tooltip === undefined ? label : tooltip;
  return tip ? <Tooltip title={tip}>{button}</Tooltip> : button;
};

export default IconButton;
