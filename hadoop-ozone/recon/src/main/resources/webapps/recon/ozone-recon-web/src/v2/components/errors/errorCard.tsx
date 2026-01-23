/*
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
import { DisconnectOutlined } from "@ant-design/icons"
import { Card } from 'antd';

type ErrorCardProps = {
  title: string;
  compact?: boolean;
};

// ------------- Styles -------------- //
const cardHeadStyle: React.CSSProperties = { fontSize: '14px' };
const compactCardBodyStyle: React.CSSProperties = {
  padding: '24px',
  justifyContent: 'space-between'
}
const cardBodyStyle: React.CSSProperties = {
  padding: '80px'
}

const ErrorCard: React.FC<ErrorCardProps> = ({ title, compact }) => {
  return (
    <Card
      size='small'
      title={title}
      headStyle={cardHeadStyle}
      bodyStyle={(compact) ? compactCardBodyStyle : cardBodyStyle}
      data-testid={`error-${title}`}>
      <DisconnectOutlined id="error-icon" />
    </Card>
  )
};

export default ErrorCard;
