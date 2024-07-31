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
import filesize from 'filesize';
import { Card, Row, Col, Table, Tag } from 'antd';

import EChart from '@/v2/components/eChart/eChart';
import OverviewCardWrapper from '@/v2/components/overviewCard/overviewCardWrapper';

import { StorageReport } from '@/v2/types/overview.types';

// ------------- Types -------------- //

type OverviewStorageCardProps = {
  loading?: boolean;
  storageReport: StorageReport;
}

const size = filesize.partial({ round: 1 });

// ------------- Component -------------- //
const OverviewStorageCard = (props: OverviewStorageCardProps = {
  loading: false,
  storageReport: {
    capacity: 0,
    used: 0,
    remaining: 0,
    committed: 0
  }
}) => {
  const { loading, storageReport } = props;

  const ozoneUsedPercentage = Math.floor(storageReport.used / storageReport.capacity * 100);
  const nonOzoneUsedPercentage = Math.floor(
    (storageReport.capacity
      - storageReport.remaining
      - storageReport.used)
    / storageReport.capacity * 100
  );
  const committedPercentage = Math.floor(storageReport.committed / storageReport.capacity * 100)
  const usagePercentage = Math.floor((
    storageReport.capacity
    - storageReport.remaining
  )
    / storageReport.capacity * 100
  );

  let capacityData = [{
    value: ozoneUsedPercentage,
    itemStyle: {
      color: '#52C41A'
    }
  }, {
    value: nonOzoneUsedPercentage,
    itemStyle: {
      color: '#1890FF'
    }
  }, {
    value: committedPercentage,
    itemStyle: {
      color: '#FF595E'
    }
  }]
  console.log(capacityData);
  // Hacky fix because guage chart shows a dot if value is zero
  // So remove all zero values
  capacityData = capacityData.filter((val) => val.value > 0)

  const eChartOptions = {
    title: {
      left: 'center',
      bottom: 'bottom',
      text: `${size(storageReport.capacity - storageReport.remaining)} / ${size(storageReport.capacity)}`,
      textStyle: {
        fontWeight: 'normal',
        fontFamily: 'Roboto'
      }
    },
    series: [
      {
        type: 'gauge',
        startAngle: 90,
        endAngle: -270,
        radius: '70%',
        center: ['50%', '45%'],
        bottom: '50%',
        pointer: {
          show: false
        },
        progress: {
          show: true,
          overlap: true,
          roundCap: true,
          clip: true
        },
        splitLine: {
          show: false
        },
        axisTick: {
          show: false
        },
        axisLabel: {
          show: false,
          distance: 50
        },
        detail: {
          rich: {
            value: {
              fontSize: 24,
              fontWeight: 400,
              fontFamily: 'Roboto',
              color: '#1B232A'
            },
            percent: {
              fontSize: 20,
              fontWeight: 400,
              color: '#1B232A'
            }
          },
          formatter: `{value|${usagePercentage}}{percent|%}`,
          offsetCenter: [0, 0]
        },
        data: capacityData
      }
    ]
  }

  const cardChildren = (
    <Card
      className={'overview-card'}
      loading={loading}
      hoverable={true}
      title='Cluster Capacity'
      style={{
        boxSizing: 'border-box',
        height: '100%'
      }}
      bodyStyle={{
        padding: '5px'
      }}>
      <Row justify='space-between'>
        <Col>
          <EChart
            option={eChartOptions}
            style={{
              width: '200px',
              height: '200px',
              marginLeft: '40%'
            }} />
        </Col>
        <Col span={12}>
          <Table
            tableLayout='unset'
            size="small"
            pagination={false}
            columns={[
              {
                title: 'Usage',
                dataIndex: 'usage',
                key: 'usage'
              },
              {
                title: 'Size',
                dataIndex: 'size',
                key: 'size',
                align: 'right'
              },
            ]}
            dataSource={[
              {
                key: 'ozone-used',
                usage: <Tag key='ozone-used' color='green'>Ozone Used</Tag>,
                size: size(storageReport.used)
              },
              {
                key: 'non-ozone-used',
                usage: <Tag key='non-ozone-used' color='blue'>Non Ozone Used</Tag>,
                size: size(storageReport.capacity
                  - storageReport.remaining
                  - storageReport.used)
              },
              {
                key: 'remaining',
                usage: <Tag key='remaining' color='#E6EBF8'>
                  <span style={{ color: '#4c7cf5' }}>Remaining</span>
                </Tag>,
                size: size(storageReport.remaining)
              },
              {
                key: 'pre-allocated',
                usage: <Tag key='pre-allocated' color='red'>Container Pre-allocated</Tag>,
                size: size(storageReport.committed)
              }
            ]} />
        </Col>
      </Row>
    </Card>
  )

  return (
    <OverviewCardWrapper
      linkToUrl={'/DiskUsage'}
      title='Report'
      children={cardChildren} />
  )
}

export default OverviewStorageCard;