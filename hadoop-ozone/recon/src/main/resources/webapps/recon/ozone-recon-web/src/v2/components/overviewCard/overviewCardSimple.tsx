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
import { Card } from 'antd';

import OverviewCardWrapper from '@/v2/components/overviewCard/overviewCardWrapper';

// ------------- Types -------------- //

type OverviewCardProps = {
  data: string | React.ReactElement;
  title: string;
  hoverable?: boolean;
  loading?: boolean;
  linkToUrl?: string;
  error?: boolean;
}

// ------------- Component -------------- //
const OverviewCardSimple = (props: OverviewCardProps = {
  data: '',
  title: '',
  hoverable: false,
  loading: false,
  linkToUrl: '',
  error: false
}) => {
  let { data, title, loading, hoverable, linkToUrl, error } = props;
  let meta = <Card.Meta description={data} />

  let errorClass = error ? 'card-error' : '';
  if (typeof data === 'string' && data === 'N/A') {
    errorClass = 'card-error';
  }

  const cardChildren = (
    <Card
      className={`overview-card ${errorClass}`}
      loading={loading}
      hoverable={hoverable}
      title={title}
      bodyStyle={{
        padding: '5% 3%',
        justifyTracks: 'space-between'
      }}>
      {meta}
    </Card>
  )

  return (
    <OverviewCardWrapper
      linkToUrl={linkToUrl as string}
      title={title}
      children={cardChildren} />
  )
}

export default OverviewCardSimple;