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

import React, { HTMLAttributes, useMemo, useState } from 'react';
import filesize from 'filesize';
import { Card, Row, Col, Table, Tag, Modal } from 'antd';

import EChart from '@/v2/components/eChart/eChart';

import { StorageReport } from '@/v2/types/overview.types';
import { InfoCircleFilled } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import ErrorCard from '@/v2/components/errors/errorCard';

// ------------- Types -------------- //
type OverviewStorageCardProps = {
  loading?: boolean;
  storageReport: StorageReport;
  error?: string | null;
}

const size = filesize.partial({ round: 1 });

function getUsagePercentages(
  { used, remaining, capacity, committed }: StorageReport): ({
    ozoneUsedPercentage: number,
    nonOzoneUsedPercentage: number,
    committedPercentage: number,
    usagePercentage: number
  }) {
  return {
    ozoneUsedPercentage: Math.floor(used / capacity * 100),
    nonOzoneUsedPercentage: Math.floor((capacity - remaining - used) / capacity * 100),
    committedPercentage: Math.floor(committed / capacity * 100),
    usagePercentage: Math.round((capacity - remaining) / capacity * 100)
  }
}

// ------------- Styles -------------- //
const cardHeadStyle: React.CSSProperties = { fontSize: '14px' };
const cardBodyStyle: React.CSSProperties = { padding: '16px' };
const cardStyle: React.CSSProperties = {
  boxSizing: 'border-box',
  height: '100%'
}
const cardErrorStyle: React.CSSProperties = {
  borderColor: '#FF4D4E',
  borderWidth: '1.4px'
}
const eChartStyle: React.CSSProperties = {
  width: '280px',
  height: '200px'
}


// ------------- Component -------------- //
const OverviewStorageCard: React.FC<OverviewStorageCardProps> = ({
  loading = false,
  storageReport = {
    capacity: 0,
    used: 0,
    remaining: 0,
    committed: 0
  },
  error
}) => {

  if (error) {
    return <ErrorCard title='Cluster Capacity' />
  }

  const [isInfoOpen, setInfoOpen] = useState<boolean>(false);

  const {
    ozoneUsedPercentage,
    nonOzoneUsedPercentage,
    committedPercentage,
    usagePercentage
  } = useMemo(() =>
    getUsagePercentages(storageReport),
    [
      storageReport.capacity,
      storageReport.committed,
      storageReport.remaining,
      storageReport.used,
    ]
  )

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
  // Remove all zero values
  // because guage chart shows a dot if value is zero
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

  const showInfo = () => {
    setInfoOpen(true);
  }

  const closeInfo = () => {
    setInfoOpen(false);
  }

  const titleElement = (
    <div className='card-title-div'>
      <div>
        <InfoCircleFilled
          onClick={showInfo}
          style={{ paddingRight: '12px', color: '#1da57a' }} />
        Cluster Capacity
      </div>
      <Link
        to={{ pathname: '/NamespaceUsage' }}
        style={{
          fontWeight: 400
        }}> View Usage </Link>
    </div>
  )

  const tableData = [
    {
      key: 'ozone-used',
      usage: <Tag key='ozone-used' color='green'>Ozone Used</Tag>,
      size: size(storageReport.used),
      desc: 'Size of Data used by Ozone for storing actual files in the Datanodes'
    },
    {
      key: 'non-ozone-used',
      usage: <Tag key='non-ozone-used' color='blue'>Non Ozone Used</Tag>,
      size: size(storageReport.capacity - storageReport.remaining - storageReport.used),
      desc: 'Size of data used by Ozone for other files like logs, DB data etc.'
    },
    {
      key: 'remaining',
      usage: <Tag key='remaining' color='#E6EBF8'>
        <span style={{ color: '#4c7cf5' }}>Remaining</span>
      </Tag>,
      size: size(storageReport.remaining),
      desc: 'Space which is free after considering replication and Non-Ozone used space'
    },
    {
      key: 'pre-allocated',
      usage: <Tag key='pre-allocated' color='red'>Container Pre-allocated</Tag>,
      size: size(storageReport.committed),
      desc: 'Space which is pre-allocated for containers'
    }
  ];

  return (
    <>
      <Modal
        title='Cluster Capacity Info'
        visible={isInfoOpen}
        onOk={closeInfo}
        onCancel={closeInfo}
        footer={null}
        centered={true}
        width={700}
        data-testid='capacity-info-modal'>
        <p>Cluster capacity fetches the data from Datanode reports that Recon receives.</p>
        <p>
          The displayed sizes <strong>include</strong> the replicated data size.<br />
          Ex: A <strong>1KB key will display 3KB</strong> in <strong>RATIS (THREE)</strong> replication
        </p>
        <Table
          size='small'
          pagination={false}
          columns={[{
            title: 'Label',
            dataIndex: 'usage',
            key: 'label'
          }, {
            title: 'Description',
            dataIndex: 'desc',
            key: 'desc',
            align: 'left'
          }]}
          dataSource={tableData} />
      </Modal>
      <Card
        size='small'
        className={'overview-card'}
        loading={loading}
        hoverable={false}
        title={titleElement}
        headStyle={cardHeadStyle}
        bodyStyle={cardBodyStyle}
        style={(usagePercentage > 79) ? { ...cardStyle, ...cardErrorStyle } : cardStyle} >
        <Row justify='space-between'>
          <Col
            className='echart-col'
            xs={24} sm={24} md={12} lg={12} xl={12}>
            <EChart
              option={eChartOptions}
              style={eChartStyle} />
          </Col>
          <Col xs={24} sm={24} md={12} lg={12} xl={12}>
            <Table
              size='small'
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
              dataSource={tableData}
              onRow={(record) => ({
                'data-testid': `capacity-${record.key}`
              }) as HTMLAttributes<HTMLElement>} />
          </Col>
        </Row>
      </Card>
    </>
  )
}

export default OverviewStorageCard;
