/**
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
import {Icon, Card, Row, Col, Progress} from 'antd';
import {withRouter, Link} from 'react-router-dom';
import {RouteComponentProps} from 'react-router';
import './OverviewCard.less';

const {Meta} = Card;

interface OverviewCardProps extends RouteComponentProps<any> {
  icon: string;
  data: string | ReactElement;
  title: string;
  hoverable?: boolean;
  loading?: boolean;
  linkToUrl?: string;
  capacityPercent?: number;
  error?: boolean;
}

const defaultProps = {
  hoverable: false,
  loading: false,
  capacityPercent: -1,
  linkToUrl: '',
  error: false
};

interface OverviewCardWrapperProps {
  linkToUrl: string;
}

class OverviewCardWrapper extends React.Component<OverviewCardWrapperProps> {
  render() {
    let {linkToUrl, children} = this.props;
    if (linkToUrl) {
      return <Link to={linkToUrl}>
        {children}
      </Link>
    } else {
      return children;
    }
  }
}

class OverviewCard extends React.Component<OverviewCardProps> {
  static defaultProps = defaultProps;

  render() {
    let {icon, data, title, loading, hoverable, capacityPercent, linkToUrl, error} = this.props;
    let meta = <Meta title={data} description={title}/>;
    const errorClass = error ? 'card-error' : '';
    if (capacityPercent && capacityPercent > -1) {
      meta = <div className="ant-card-percentage">
        {meta}
        <Progress strokeLinecap="square" percent={capacityPercent} className="capacity-bar" strokeWidth={3}/>
      </div>;
    }
    linkToUrl = linkToUrl || '';

    return (
        <OverviewCardWrapper linkToUrl={linkToUrl}>
          <Card className={`overview-card ${errorClass}`} loading={loading} hoverable={hoverable}>
            <Row type="flex" justify="space-between">
              <Col span={18}>
                <Row>
                  {meta}
                </Row>
              </Col>
              <Col span={6}>
                <Icon type={icon} style={{"fontSize": "50px"}}/>
              </Col>
            </Row>
          </Card>
        </OverviewCardWrapper>
    );
  }
}

export default withRouter(OverviewCard);
