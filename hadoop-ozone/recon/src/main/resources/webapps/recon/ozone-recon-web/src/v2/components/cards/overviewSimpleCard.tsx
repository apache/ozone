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
import { Card, Col, Row } from 'antd';
import { Link } from 'react-router-dom';
import {
  ClusterOutlined,
  ContainerOutlined,
  DatabaseOutlined,
  DeleteOutlined,
  DeploymentUnitOutlined,
  FileTextOutlined,
  FolderOpenOutlined,
  InboxOutlined,
  QuestionCircleOutlined
} from '@ant-design/icons';
import { numberWithCommas } from '@/utils/common';
import ErrorCard from '@/v2/components/errors/errorCard';


// ------------- Types -------------- //
type IconOptions = {
  [key: string]: React.ReactElement
}

type OverviewCardProps = {
  icon: string;
  data: number;
  title: string;
  hoverable?: boolean;
  loading?: boolean;
  linkToUrl?: string;
  error?: string | null;
}

// ------------- Styles -------------- //
const defaultIconStyle: React.CSSProperties = {
  fontSize: '50px',
  float: 'right'
};
const iconStyle: React.CSSProperties = {
  fontSize: '20px',
  paddingRight: '4px',
  float: 'inline-start'
};
const cardHeadStyle: React.CSSProperties = { fontSize: '14px' };
const cardBodyStyle: React.CSSProperties = {
  padding: '16px',
  justifyTracks: 'space-between'
};
const dataColStyle: React.CSSProperties = { fontSize: '24px' };
const titleLinkStyle: React.CSSProperties = { fontWeight: 400 }

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
    let ico = <QuestionCircleOutlined style={defaultIconStyle} />

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
const OverviewSimpleCard: React.FC<OverviewCardProps> = ({
  icon = '',
  data = 0,
  title = '',
  hoverable = false,
  loading = false,
  linkToUrl = '',
  error
}) => {

  if (error) {
    return <ErrorCard title={title} compact={true}/>
  }

  const titleElement = (linkToUrl)
    ? (
      <div className='card-title-div'>
        {title}
        <Link
          to={linkToUrl}
          style={titleLinkStyle}>
          View More
        </Link>
      </div>)
    : title;

  return (
    <Card
      size='small'
      loading={loading}
      hoverable={hoverable}
      title={(linkToUrl) ? titleElement : title}
      headStyle={cardHeadStyle}
      bodyStyle={cardBodyStyle}
      data-testid={`overview-${title}`}>
      <Row
        align='middle'>
        <Col>
          <IconSelector iconType={icon} style={iconStyle} />
        </Col>
        <Col style={dataColStyle}>
          {numberWithCommas(data)}
        </Col>
      </Row>
    </Card>
  );
};

export default OverviewSimpleCard;
