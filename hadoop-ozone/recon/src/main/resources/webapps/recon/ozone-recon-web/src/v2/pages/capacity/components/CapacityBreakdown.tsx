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

import { GraphLegendIcon } from '@/utils/themeIcons';
import StackedProgress from '@/v2/pages/capacity/components/StackedProgress';
import { cardHeadStyle, statisticValueStyle } from '@/v2/pages/capacity/constants/styles.constants';
import { Segment } from '@/v2/types/capacity.types';
import { Card, Statistic } from 'antd';
import filesize from 'filesize';
import React from 'react';

type GridItem = {
  title: string | React.ReactNode;
  value: number;
  color?: string;
  format?: 'bytes' | 'number' | 'percentage';
};

type ClusterCardProps = {
  title: string | React.ReactNode;
  items: GridItem[];
  loading: boolean;
};

const getProgressSegments = (items: GridItem[]) => {
  return items.filter(item => item.color).map((item) => ({
    value: item.value,
    color: item.color!,
    label: item.title
  } as Segment));
}

const CapacityBreakdown: React.FC<ClusterCardProps> = ({ title, items, loading }) => {

  return (
    <Card title={title} size='small' headStyle={cardHeadStyle} loading={loading}>
      <div className='cluster-card-data-container'>
        {items.map((item, idx) => {
          // Split the size into the value and the unit
          const size = filesize((item.value > 0 ? item.value : 0), { round: 1 }).split(' ');
          return (
            <Statistic
              key={`cluster-statistic-${item.title}-${idx}`}
              title={item.title}
              prefix={item.color ? <GraphLegendIcon color={item.color} /> : undefined}
              value={size[0]}
              suffix={size[1]}
              valueStyle={statisticValueStyle}
              className='cluster-card-statistic'
            />
          )
        })}
      </div>
      <StackedProgress segments={getProgressSegments(items)} />
    </Card>
  );
};

export default CapacityBreakdown;