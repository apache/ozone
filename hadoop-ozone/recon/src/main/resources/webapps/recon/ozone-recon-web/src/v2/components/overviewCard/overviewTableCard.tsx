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
import { Card, Row, Table } from 'antd';

import { ColumnType } from 'antd/es/table';
import { Link } from 'react-router-dom';

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
}

// ------------- Component -------------- //
const OverviewTableCard = (props: OverviewTableCardProps = {
  data: '',
  title: '',
  hoverable: false,
  loading: false,
  columns: [],
  tableData: [],
  linkToUrl: '',
  showHeader: false
}) => {
  let {
    data,
    title,
    loading,
    hoverable,
    tableData,
    columns,
    linkToUrl,
    showHeader
  } = props;

  console.log(showHeader);
  const titleElement = (linkToUrl)
    ? (
      <div style={{
        display: 'flex',
        justifyContent: 'space-between'
      }}>
        {title}
        <Link
          to={linkToUrl}
          style={{
            fontWeight: 400
          }}>View Insights</Link>
      </div> )
    : title

  return (
    <Card
      size='small'
      className={'overview-card'}
      loading={loading}
      hoverable={hoverable}
      title={titleElement}
      headStyle={{
        fontSize: '14px'
      }}
      bodyStyle={{
        padding: '16px',
        justifyTracks: 'space-between'
      }}
      style={{
        height: '100%'
      }}>
      {(data)
        ? <Row gutter={[0, 50]}>
          {data}
        </Row>
        : <></>}
      <Row>
        <Table
          showHeader={showHeader||false}
          tableLayout='fixed'
          size="small"
          pagination={false}
          dataSource={tableData}
          columns={columns} />
      </Row>
    </Card>
  )
}

export default OverviewTableCard;