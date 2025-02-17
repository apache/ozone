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

import OverviewSimpleCard from "@/v2/components/overviewCard/overviewSimpleCard";
import { getTaskIndicatorIcon } from "@/v2/pages/overview/overview.utils";
import { Col } from "antd";
import React, { JSXElementConstructor, ReactElement } from "react";

// ------------- Types -------------- //
type OmData = {
  volumes: number | ReactElement<any, string | JSXElementConstructor<any>>;
  buckets: number | ReactElement<any, string | JSXElementConstructor<any>>;
  keys: number | ReactElement<any, string | JSXElementConstructor<any>>;
};

type OverviewOMCardProps = {
  data: OmData;
  taskRunning: boolean;
  taskStatus: boolean;
  loading?: boolean;
};


const OverviewOMCardGroup: React.FC<OverviewOMCardProps> = ({
  loading = true,
  data,
  taskRunning,
  taskStatus
}) => {
  const taskIcon: ReactElement = getTaskIndicatorIcon(taskRunning, taskStatus);
  return (
    <>
      <Col flex="1 0 20%">
        <OverviewSimpleCard
          title='Volumes'
          icon='inbox'
          loading={loading}
          data={data.volumes}
          linkToUrl='/Volumes'
          taskIcon={taskIcon} />
      </Col>
      <Col flex="1 0 20%">
        <OverviewSimpleCard
          title='Buckets'
          icon='folder-open'
          loading={loading}
          data={data.buckets}
          linkToUrl='/Buckets'
          taskIcon={taskIcon} />
      </Col>
      <Col flex="1 0 20%">
        <OverviewSimpleCard
          title='Keys'
          icon='file-text'
          loading={loading}
          data={data.keys}
          taskIcon={taskIcon} />
      </Col>
    </>
  )
}

export default OverviewOMCardGroup;