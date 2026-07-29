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
import { Input, type InputProps } from 'antd';
import { semanticColors } from '../../theme/tokens';
import Icon from '../Icon/Icon';

export interface SearchInputProps extends Omit<InputProps, 'prefix'> {
  /** Input width. Defaults to 256 (the "Input Field" width used in table toolbars). */
  width?: number | string;
}

/**
 * Text field with a leading search glyph, matching the "standard-text-field"
 * search input used in the Ozone table toolbars. All standard Ant Design `Input`
 * props are supported.
 */
export const SearchInput: React.FC<SearchInputProps> = ({
  width = 256,
  placeholder = 'Search...',
  style,
  ...rest
}) => (
  <Input
    allowClear
    placeholder={placeholder}
    prefix={<Icon name="search" size={16} style={{ color: semanticColors.textTertiary }} />}
    style={{ width, ...style }}
    {...rest}
  />
);

export default SearchInput;
