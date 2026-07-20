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
import { Typography } from 'antd';
import type { LinkProps } from 'antd/es/typography/Link';
import { textStyles } from '../../theme/tokens';
import Icon from '../Icon/Icon';

export interface TextLinkProps extends LinkProps {
  /** Render as an external link (opens in a new tab, shows an external icon). */
  external?: boolean;
  /** Text size. Defaults to `standard`. */
  size?: 'standard' | 'small';
}

/**
 * Inline text link built on Ant Design's `Typography.Link`, themed with the
 * design-system link colour and type scale. Set `external` for links that open
 * in a new tab (adds a trailing external-link glyph).
 */
export const TextLink: React.FC<TextLinkProps> = ({
  external = false,
  size = 'standard',
  style,
  children,
  ...rest
}) => {
  const scale = size === 'small' ? textStyles.bodySmall : textStyles.bodyStandard;
  const externalProps = external ? { target: '_blank', rel: 'noopener noreferrer' } : {};

  return (
    <Typography.Link
      {...rest}
      style={{
        fontSize: scale.fontSize,
        lineHeight: `${scale.lineHeight}px`,
        display: 'inline-flex',
        alignItems: 'center',
        gap: 4,
        ...style,
      }}
      {...externalProps}
    >
      {children}
      {external && <Icon name="external-link" size={size === 'small' ? 12 : 14} />}
    </Typography.Link>
  );
};

export default TextLink;
