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
import { Alert as AntAlert, type AlertProps as AntAlertProps } from 'antd';
import { radius } from '../../theme/tokens';

export type AlertProps = AntAlertProps;

/**
 * Inline status banner. Thin wrapper over Ant Design's `Alert` applying the
 * design-system radius; use the standard `type` (`info` / `success` /
 * `warning` / `error`), `message`, `description`, `showIcon` and `closable`
 * props. Matches the "Alert / Standard" component in the mockups.
 */
export const Alert: React.FC<AlertProps> = ({ showIcon = true, style, ...rest }) => (
  <AntAlert showIcon={showIcon} style={{ borderRadius: radius.md, ...style }} {...rest} />
);

export default Alert;
