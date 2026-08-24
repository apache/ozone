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

import React, { HTMLAttributes } from 'react';
import { Card, Col, Row, Table } from 'antd';

import { ColumnType } from 'antd/es/table';
import { Link } from 'react-router-dom';
import ErrorCard from '@/v2/components/errors/errorCard';
import { CheckCircleFilled, WarningFilled } from '@ant-design/icons';

// ------------- Types -------------- //
type TableData = {
  key: React.Key;
  name: string;
  value: string;
  action?: React.ReactElement | string;
}

type OverviewTableCardProps = {
  title: string;
  loading?: boolean;
  available: number;
  total: number;
  linkToUrl?: string;
  showHeader?: boolean;
  error?: string | null;
}

// ------------- Styles -------------- //
const cardStyle: React.CSSProperties = {
  height: '100%'
}
const cardHeadStyle: React.CSSProperties = {
  fontSize: '14px'
}
const cardBodyStyle: React.CSSProperties = {
  padding: '16px',
  justifyTracks: 'space-between'
}


// ------------- Component -------------- //
const OverviewSummaryCard: React.FC<OverviewTableCardProps> = ({
  available,
  total,
  title = '',
  loading = false,
  linkToUrl = '',
  error
}) => {

  if (error || available === undefined || total === undefined) {
    return <ErrorCard title={title} />;
  }

  const titleElement = (linkToUrl)
    ? (
      <div className='card-title-div'>
        {title}
        <Link
          to={{
            pathname: linkToUrl
          }}
          style={{
            fontWeight: 400
          }}>View More</Link>
      </div>)
    : title;
  
  const healthCheck = available == total;
  const healthIndicator = (
    <div className={`icon-${healthCheck? 'success' : 'warning'}`} style={{
      fontSize: '20px',
      alignItems: 'center'
    }}>
      {
        !healthCheck ? (
          <>
            <WarningFilled style={{
            marginRight: '5px'
          }} />
          Unhealthy
          </>
        ) : (
          <>
            <CheckCircleFilled style={{
            marginRight: '5px'
          }} />
          Healthy
          </>
        )
      }
    </div>
  )

  return (
    <Card
      size='small'
      className={'overview-card'}
      loading={loading}
      title={titleElement}
      headStyle={cardHeadStyle}
      bodyStyle={cardBodyStyle}
      style={cardStyle}>
      <Row gutter={[11, 0]}>
        <Col span={12}>
          Health
          {healthIndicator}
        </Col>
        <Col className='health-availability' span={10}>
          Availability
          <span className='health-availability-value'>
            {available}/{total}
          </span>
        </Col>
      </Row>
    </Card>
  )
}

export default OverviewSummaryCard;