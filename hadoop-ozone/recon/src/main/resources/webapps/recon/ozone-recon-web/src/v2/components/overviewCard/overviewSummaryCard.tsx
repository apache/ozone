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
import { Card, Row, Table } from 'antd';

import { ColumnType } from 'antd/es/table';
import { Link } from 'react-router-dom';
import ErrorMessage from '@/v2/components/errors/errorCard';
import ErrorCard from '@/v2/components/errors/errorCard';

// ------------- Types -------------- //
type TableData = {
  key: React.Key;
  name: string;
  value: string;
  action?: React.ReactElement | string;
}

type OverviewTableCardProps = {
  title: string;
  columns: ColumnType<TableData>[];
  tableData: TableData[];
  hoverable?: boolean;
  loading?: boolean;
  data?: string | React.ReactElement;
  linkToUrl?: string;
  showHeader?: boolean;
  state?: Record<string, any>;
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
  data = '',
  title = '',
  hoverable = false,
  loading = false,
  columns = [],
  tableData = [],
  linkToUrl = '',
  showHeader = false,
  state,
  error
}) => {

  if (error) {
    return <ErrorCard title={title} />;
  }

  const titleElement = (linkToUrl)
    ? (
      <div className='card-title-div'>
        {title}
        <Link
          to={{
            pathname: linkToUrl,
            state: state
          }}
          style={{
            fontWeight: 400
          }}>View Insights</Link>
      </div>)
    : title

  return (
    <Card
      size='small'
      className={'overview-card'}
      loading={loading}
      hoverable={hoverable}
      title={titleElement}
      headStyle={cardHeadStyle}
      bodyStyle={cardBodyStyle}
      style={cardStyle}>
      {
        (data) &&
        <Row gutter={[0, 50]}>
          {data}
        </Row>
      }
      <Table
        showHeader={showHeader || false}
        tableLayout='fixed'
        size="small"
        pagination={false}
        dataSource={tableData}
        columns={columns}
        onRow={(record: TableData) => ({
          'data-testid': `overview-${title}-${record.name}`
        } as HTMLAttributes<HTMLElement>)}/>
    </Card>
  )
}

export default OverviewSummaryCard;