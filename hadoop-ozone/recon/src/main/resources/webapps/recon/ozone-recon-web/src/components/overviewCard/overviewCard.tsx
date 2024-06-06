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

import React, {ReactElement} from 'react';
import {Icon, Card, Row, Col} from 'antd';
import {withRouter, Link} from 'react-router-dom';
import {RouteComponentProps} from 'react-router';
import StorageBar from '../storageBar/storageBar';
import {IStorageReport} from 'types/datanode.types';
import './overviewCard.less';

const {Meta} = Card;

interface IOverviewCardProps extends RouteComponentProps<object> {
  icon: string;
  data: string | ReactElement;
  title: string;
  hoverable?: boolean;
  loading?: boolean;
  linkToUrl?: string;
  storageReport?: IStorageReport;
  error?: boolean;
}

const defaultProps = {
  hoverable: false,
  loading: false,
  linkToUrl: '',
  error: false
};

interface IOverviewCardWrapperProps {
  linkToUrl: string;
  title: string
}

class OverviewCardWrapper extends React.Component<IOverviewCardWrapperProps> {
  // To set Current ACtive Tab for OM DB Insights
  setCurrentActiveTab = (title: any) => {
    if (title === "Open Keys Summary") {
      return {
        active: '2'
      }
    }
    else if (title === "Pending Deleted Keys Summary") {
      return {
        active: '3'
      }
    }
    else if (title === "Ozone Manager") {
      return {
        active: '4'
      }
    }
  };

  render() {
    const {linkToUrl, children} = this.props;
    if (linkToUrl) {
      if (linkToUrl === '/Om') {
        return (
          <Link to={{
            pathname: linkToUrl,
            state: { activeTab: children._owner.stateNode.props ? this.setCurrentActiveTab(children._owner.stateNode.props.title).active : '1' }
          }} >
            {children}
          </Link>
        )
      }
      else {
        return (
          <Link to={linkToUrl}>
            {children}
          </Link>
        )
      }
    }

    return children;
  }
}

class OverviewCard extends React.Component<IOverviewCardProps> {
  static defaultProps = defaultProps;

  render() {
    let {icon, data, title, loading, hoverable, storageReport, linkToUrl, error} = this.props;
    let meta = <Meta title={data} description={title}/>;
    const errorClass = error ? 'card-error' : '';
    if (storageReport) {
      meta = (
        <div className='ant-card-percentage'>
          {meta}
          <div className='storage-bar'>
            <StorageBar total={storageReport.capacity} used={storageReport.used} remaining={storageReport.remaining} showMeta={false}/>
          </div>
        </div>
      );
    }

    linkToUrl = linkToUrl ? linkToUrl : '';

    return (
      <OverviewCardWrapper linkToUrl={linkToUrl}>
        <Card className={`overview-card ${errorClass}`} loading={loading} hoverable={hoverable}>
          <Row type='flex' justify='space-between'>
            <Col span={18}>
              <Row>
                {meta}
              </Row>
            </Col>
            <Col span={6}>
              <Icon type={icon} style={{fontSize: '50px', float: 'right'}}/>
            </Col>
          </Row>
        </Card>
      </OverviewCardWrapper>
    );
  }
}

export default withRouter(OverviewCard);
