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

import React, { ReactElement } from 'react';
import { Card, Row, Col } from 'antd';
import {
  ClusterOutlined,
  DeploymentUnitOutlined,
  DatabaseOutlined,
  ContainerOutlined,
  InboxOutlined,
  FolderOpenOutlined,
  FileTextOutlined,
  QuestionCircleOutlined,
  DeleteOutlined
} from '@ant-design/icons';

import StorageBar from '@/components/storageBar/storageBar';
import OverviewCardWrapper from '@/v2/components/overviewCardWrapper';
import { IOverviewCardProps } from '@/v2/types/overview';
import { IStorageReport } from '@/types/datanode.types';
import './overviewCard.less';

const IconSelector = ({ iconType, ...extras }: { iconType: string }) => {
  const Icons = {
    'cluster': <ClusterOutlined {...extras} />,
    'deployment-unit': <DeploymentUnitOutlined {...extras} />,
    'database': <DatabaseOutlined {...extras} />,
    'container': <ContainerOutlined {...extras} />,
    'inbox': <InboxOutlined {...extras} />,
    'folder-open': <FolderOpenOutlined {...extras} />,
    'file-text': <FileTextOutlined {...extras} />,
    'delete': <DeleteOutlined {...extras} />
  }

  const selectIcon = (type: string) => {
    // Setting the default Icon as a question mark in case no match found
    let ico = <QuestionCircleOutlined style={{
      fontSize: '50px',
      float: 'right'
    }} />
    const found = Object.entries(Icons).find(([k]) => k.toLowerCase() === type.toLowerCase())
    if (found) {
      [, ico] = found;
    }
    return ico;
  }
  return selectIcon(iconType);
}

const OverviewCard = (props: IOverviewCardProps = {
  icon: '',
  data: '',
  title: '',
  hoverable: false,
  loading: false,
  linkToUrl: '',
  error: false
}) => {
  let { icon, data, title, loading, hoverable, storageReport, linkToUrl, error } = props;
  let meta = <Card.Meta title={title} description={data} />

  let errorClass = error ? 'card-error' : '';
  if (typeof data === 'string' && data === 'N/A') {
    errorClass = 'card-error';
  }

  if (storageReport) {
    meta = (
      <div className='ant-card-percentage'>
        {meta}
        <div className='storage-bar'>
          <StorageBar total={storageReport.capacity} used={storageReport.used} remaining={storageReport.remaining} showMeta={false} />
        </div>
      </div>
    );
  }

  const cardChildren = (
    <Card className={`overview-card ${errorClass}`} loading={loading} hoverable={hoverable}>
      <Row justify='space-between'>
        <Col span={18}>
          <Row>
            {meta}
          </Row>
        </Col>
        <Col span={6}>
          <IconSelector iconType={icon} style={{
            fontSize: '50px',
            float: 'right'
          }} />
        </Col>
      </Row>
    </Card>
  )

  return (
    <OverviewCardWrapper
      linkToUrl={linkToUrl as string}
      title={title}
      children={cardChildren} />
  )
}

export default OverviewCard;