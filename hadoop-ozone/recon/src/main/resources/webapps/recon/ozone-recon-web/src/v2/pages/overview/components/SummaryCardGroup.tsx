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

import OverviewSummaryCard from "@/v2/components/overviewCard/overviewSummaryCard";
import { getTaskIndicatorIcon } from "@/v2/pages/overview/overview.utils";
import { Col } from "antd";
import filesize from "filesize";
import React from "react";

type OverviewSummaryCardGroupProps = {
  loading?: boolean;
  openSummarytotalRepSize: number;
  openSummarytotalUnrepSize: number;
  openSummarytotalOpenKeys: number;
  deletePendingSummarytotalRepSize: number;
  deletePendingSummarytotalUnrepSize: number;
  deletePendingSummarytotalDeletedKeys: number;
  taskRunning: boolean;
  taskStatus: boolean;
}

const size = filesize.partial({ round: 1 });

const getSummaryTableValue = (
  value: number | string | undefined,
  colType: 'value' | undefined = undefined
): string => {
  if (!value) return 'N/A';
  if (colType === 'value') return String(value as string)
  return size(value as number)
}

const OverviewSummaryCardGroup: React.FC<OverviewSummaryCardGroupProps> = ({
  loading = true,
  openSummarytotalRepSize,
  openSummarytotalUnrepSize,
  openSummarytotalOpenKeys,
  deletePendingSummarytotalRepSize,
  deletePendingSummarytotalUnrepSize,
  deletePendingSummarytotalDeletedKeys,
  taskRunning,
  taskStatus
}) => {

  const taskIcon: React.ReactElement = getTaskIndicatorIcon(taskRunning, taskStatus);
  return (
    <>
      <Col xs={24} sm={24} md={24} lg={12} xl={12}>
        <OverviewSummaryCard
          title='Open Keys Summary'
          loading={loading}
          columns={[
            {
              title: 'Name',
              dataIndex: 'name',
              key: 'name'
            },
            {
              title: 'Size',
              dataIndex: 'value',
              key: 'size',
              align: 'right'
            }
          ]}
          tableData={[
            {
              key: 'total-replicated-data',
              name: 'Total Replicated Data',
              value: getSummaryTableValue(openSummarytotalRepSize)
            },
            {
              key: 'total-unreplicated-data',
              name: 'Total Unreplicated Data',
              value: getSummaryTableValue(openSummarytotalUnrepSize)
            },
            {
              key: 'open-keys',
              name: 'Open Keys',
              value: getSummaryTableValue(
                openSummarytotalOpenKeys,
                'value'
              )
            }
          ]}
          linkToUrl='/Om'
          state={{ activeTab: '2' }}
          taskIcon={taskIcon} />
      </Col>
      <Col xs={24} sm={24} md={24} lg={12} xl={12}>
        <OverviewSummaryCard
          title='Delete Pending Keys Summary'
          loading={loading}
          columns={[
            {
              title: 'Name',
              dataIndex: 'name',
              key: 'name'
            },
            {
              title: 'Size',
              dataIndex: 'value',
              key: 'size',
              align: 'right'
            }
          ]}
          tableData={[
            {
              key: 'total-replicated-data',
              name: 'Total Replicated Data',
              value: getSummaryTableValue(deletePendingSummarytotalRepSize)
            },
            {
              key: 'total-unreplicated-data',
              name: 'Total Unreplicated Data',
              value: getSummaryTableValue(deletePendingSummarytotalUnrepSize)
            },
            {
              key: 'delete-pending-keys',
              name: 'Delete Pending Keys',
              value: getSummaryTableValue(
                deletePendingSummarytotalDeletedKeys,
                'value'
              )
            }
          ]}
          linkToUrl='/Om'
          state={{ activeTab: '3' }}
          taskIcon={taskIcon} />
      </Col>
    </>
  )
}

export default OverviewSummaryCardGroup;