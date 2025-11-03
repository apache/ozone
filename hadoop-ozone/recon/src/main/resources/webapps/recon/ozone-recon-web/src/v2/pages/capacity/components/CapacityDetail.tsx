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

import { EChart } from '@/components/eChart/eChart';
import { GraphLegendIcon } from '@/utils/themeIcons';
import { cardHeadStyle, statisticValueStyle } from '@/v2/pages/capacity/constants/styles.constants';
import { Card, Divider, Row, Select, Statistic } from 'antd';
import filesize from 'filesize';
import React from 'react';

type DataDetailItem = {
  title: string | React.ReactNode;
  size: number;
  breakdown: Segment[];
}

type CapacityDetailProps = {
  title: string;
  showDropdown: boolean;
  dataDetails: DataDetailItem[];
  dropdownItems?: string[];
  handleSelect?: React.Dispatch<React.SetStateAction<string | null>>
  loading: boolean;
};

const getEchartOptions = (title: string | React.ReactNode, data: DataDetailItem) => {
  const option = {
    grid: {
      left: 2,
      right: 4,
      top: 16,
      bottom: 0
    },
    xAxis: {
      // Use linear scale to support zero values safely
      type: 'value',
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { show: false }
    },
    yAxis: {
      type: 'category',
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { show: false },
    },
  };

  const breakdownLen = data.breakdown.length;
  const series = data.breakdown.map((breakdown, idx) => ({
    type: 'bar',
    ...(breakdownLen > 1 && { stack: title }),
    itemStyle: {
      ...(idx === breakdownLen - 1 && { borderRadius: [0, 50, 50, 0] }),
      ...(idx === 0 && { borderRadius: [50, 0, 0, 50] }),
      ...(breakdownLen === 1 && { borderRadius: [50, 50, 50, 50] }),
      color: breakdown.color,
    },
    data: [breakdown.value],
    barWidth: '10px',
    barGap: '2px'
  }));

  return {
    ...option,
    series
  } as any
}


const CapacityDetail: React.FC<CapacityDetailProps> = (
  { title, showDropdown, dropdownItems, dataDetails, handleSelect, loading }
) => {

  const options = dropdownItems?.map((item) => ({
    label: item,
    value: item,
  })) ?? [];

  return (
    <Card title={title} size='small' headStyle={cardHeadStyle} loading={loading}>
      { showDropdown && options.length > 0 &&
        <div className='node-select-container'>
          <strong>Node Selector:</strong>
          <Select
            defaultValue={options?.[0]?.value}
            options={options}
            onChange={handleSelect}
            style={{ marginBottom: '16px' }}
          />
        </div>
      }
      <div className='cluster-card-data-container vertical-layout'>
        {dataDetails.map((data, idx) => {
          const size = filesize(data.size, { round: 1 }).split(' ');
          return (
            <div key={`data-detail-${data.title}-${idx}`} className='data-detail-item'>
              <Statistic
                title={data.title}
                value={size[0]}
                suffix={size[1]}
                valueStyle={statisticValueStyle}
                className='data-detail-statistic'
              />
              <Row className='data-detail-breakdown-container'>
                {data.breakdown.map((item, idx) => (
                  <Statistic
                    key={`data-detail-breakdown-${item.label}-${idx}`}
                    title={item.label}
                    prefix={<GraphLegendIcon color={item.color} height={12}/>}
                    value={filesize(item.value, { round: 1 })}
                    className='data-detail-breakdown-statistic'
                  />
                ))}
                <EChart
                  option={getEchartOptions(data.title, data)}
                  style={{ height: '40px', width: '100%', margin: '10px 0px' }} />
                {idx < dataDetails.length - 1 && <Divider />}
              </Row>
            </div>
          )
        })}
      </div>
    </Card>
  );
}

export default CapacityDetail;
