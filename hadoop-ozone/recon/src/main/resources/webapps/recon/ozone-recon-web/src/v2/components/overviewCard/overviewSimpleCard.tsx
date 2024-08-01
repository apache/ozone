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
import { ClusterOutlined, ContainerOutlined, DatabaseOutlined, DeleteOutlined, DeploymentUnitOutlined, FileTextOutlined, FolderOpenOutlined, InboxOutlined, QuestionCircleOutlined } from '@ant-design/icons';


// ------------- Types -------------- //
type IconOptions = {
  [key: string]: React.ReactElement
}

type OverviewCardProps = {
  icon: string;
  data: number | React.ReactElement;
  title: string;
  hoverable?: boolean;
  loading?: boolean;
  linkToUrl?: string;
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
const OverviewSimpleCard = (props: OverviewCardProps = {
  icon: '',
  data: 0,
  title: '',
  hoverable: false,
  loading: false,
  linkToUrl: ''
}) => {
  let { icon, data, title, loading, hoverable, linkToUrl } = props;

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
          }} >View More</Link>
      </div>)
    : title

  return (
    <Card
      size='small'
      className={'overview-card'}
      loading={loading}
      hoverable={hoverable}
      title={(linkToUrl) ? titleElement : title}
      headStyle={{
        fontSize: '14px'
      }}
      bodyStyle={{
        padding: '16px',
        justifyTracks: 'space-between'
      }}>
      <Row
        align='middle'>
        <Col>
          <IconSelector iconType={icon} style={{
            fontSize: '20px',
            paddingRight: '4px',
            float: 'inline-start'
          }} />
        </Col>
        <Col style={{
          fontSize: '24px'
        }}>
          {data}
        </Col>
      </Row>
    </Card>
  );
}

export default OverviewSimpleCard;