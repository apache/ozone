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
import { Table, Drawer } from 'antd';

interface IRightDrawerProps extends RouteComponentProps<object> {
  visible: boolean;
  keys: [];
  values: [];
  path: string;
}

export class DetailPanel extends React.Component<IRightDrawerProps> {
  state = { visible: false };

  componentWillReceiveProps(props) {
    const { visible } = props;
    this.setState({ visible });
  }

  onClose = () => {
    this.setState({
      visible: false
    });
  };

  render() {
    const { Column } = Table;
    const { keys, values, path } = this.props;
    const { visible } = this.state;
    const content = [];
    for (const [i, v] of keys.entries()) {
      content.push({
        key: v,
        value: values[i]
      });
    }

    return (
      <div className='site-drawer-render-in-current-wrapper'>
        <Drawer
          title={`Metadata Summary for ${path}`}
          placement='right'
          width='25%'
          closable={true}
          visible={visible}
          getContainer={false}
          style={{ position: 'absolute' }}
          onClose={this.onClose}
        >
          <Table dataSource={content} locale={{ filterTitle: '' }}>
            <Column title='Property' dataIndex='key' />
            <Column title='Value' dataIndex='value' />
          </Table>
        </Drawer>
      </div>
    );
  }
}
