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

import StorageBar from '@/v2/components/storageBar/storageBar';
import OverviewCardWrapper from '@/v2/components/overviewCard/overviewCardWrapper';
import { StorageReport } from '@/v2/types/overview.types';

// ------------- Types -------------- //
type IconOptions = {
  [key: string]: React.ReactElement
}

type OverviewCardProps = {
  data: string | React.ReactElement;
  title: string;
  icon?: string;
  hoverable?: boolean;
  loading?: boolean;
  linkToUrl?: string;
  storageReport?: StorageReport;
  error?: boolean;
}


// Since AntD no longer supports string icon component
// we are using a utility function to map the strings to
// the appropriate Icon to render
const IconSelector = ({
  iconType, style
}: {
  iconType: string;
  style: React.CSSProperties
}) => {
  const Icons: IconOptions = {
    'cluster': <ClusterOutlined style={style} />,
    'deployment-unit': <DeploymentUnitOutlined style={style} />,
    'database': <DatabaseOutlined style={style} />,
    'container': <ContainerOutlined style={style} />,
    'inbox': <InboxOutlined style={style} />,
    'folder-open': <FolderOpenOutlined style={style} />,
    'file-text': <FileTextOutlined style={style} />,
    'delete': <DeleteOutlined style={style} />
  };

  const selectIcon = (iconType: string): React.ReactElement => {
    // Setting the default Icon as a question mark in case no match found
    let ico = <QuestionCircleOutlined style={{
      fontSize: '50px',
      float: 'right'
    }} />

    const found = Object.entries(Icons).find(
      ([k]) => k.toLowerCase() === iconType.toLowerCase()
    );

    if (found) {
      [, ico] = found;
    }
    return ico;
  }
  return selectIcon(iconType);
}


// ------------- Component -------------- //
const OverviewCard = (props: OverviewCardProps = {
  icon: '',
  data: '',
  title: '',
  hoverable: false,
  loading: false,
  linkToUrl: '',
  error: false
}) => {
  let { icon, data, title, loading, hoverable, storageReport, linkToUrl, error } = props;
  let meta = <Card.Meta description={data} />

  let errorClass = error ? 'card-error' : '';
  if (typeof data === 'string' && data === 'N/A') {
    errorClass = 'card-error';
  }

  if (storageReport) {
    meta = (
      <div className='ant-card-percentage'>
        {meta}
        <div className='storage-bar'>
          <StorageBar
            capacity={storageReport.capacity}
            used={storageReport.used}
            remaining={storageReport.remaining}
            showMeta={false} />
        </div>
      </div>
    );
  }

  const cardChildren = (
    <Card
      className={`overview-card ${errorClass}`}
      loading={loading}
      hoverable={hoverable}
      title={title}
      style={{
        boxSizing: 'border-box'
      }}
      bodyStyle={{
        padding: '5px'
      }}
      headStyle={{
        height: '10px'
      }}>
      <Row justify='space-between'>
        <Col span={18}>
          <Row>
            {meta}
          </Row>
        </Col>
        {(icon)
          ? <Col span={6}>
            <IconSelector iconType={icon} style={{
              fontSize: '50px',
              float: 'right'
            }} />
          </Col>
          : <></>
        }
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