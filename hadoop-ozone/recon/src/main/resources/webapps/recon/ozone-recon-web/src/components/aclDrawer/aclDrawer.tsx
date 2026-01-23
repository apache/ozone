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
import { Table, Drawer, Tag } from 'antd';

import { ACLIdentity, ACLIdentityTypeList, IAcl } from '@/types/om.types';
import { aclRightColorMap, aclIdentityTypeColorMap } from '@/constants/aclDrawer.constants';

interface IAclDrawerProps extends RouteComponentProps<object> {
  visible: boolean;
  acls: IAcl[] | undefined;
  objName: string;
  objType: string;
}

export class AclPanel extends React.Component<IAclDrawerProps> {
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

  renderAclList(text: string, acl: IAcl) {
    return acl.aclList.map(aclRight => {
      return (
        <Tag key={aclRight} color={aclRightColorMap[aclRight]}>{aclRight}</Tag>
      );
    });
  }

  renderAclIdentityType(text: string, acl: IAcl) {
    return (
      <Tag color={aclIdentityTypeColorMap[acl.type]}>{acl.type}</Tag>
    );
  }

  render() {
    const { Column } = Table;
    const { objName, objType, acls } = this.props;
    const { visible } = this.state;

    return (
      <div className='site-drawer-render-in-current-wrapper'>
        <Drawer
          title={`ACL for ${objType} ${objName}`}
          placement='right'
          width='40%'
          closable={false}
          visible={visible}
          getContainer={false}
          style={{ position: 'absolute' }}
          onClose={this.onClose}
        >
          <Table
            dataSource={acls}
            rowKey={(record: IAcl) => `${record.name ?? ''}-${record.type ?? ''}-${record.scope ?? ''}`}
            locale={{ filterTitle: "" }}>
            <Column
              key='name'
              title='Name'
              dataIndex='name'
              sorter={(a: IAcl, b: IAcl) => a.name.localeCompare(b.name)}
            />
            <Column
              key='type'
              filterMultiple
              title='ACL Type'
              dataIndex='type'
              filters={ACLIdentityTypeList.map(state => ({ text: state, value: state }))}
              sorter={(a: IAcl, b: IAcl) => a.type.localeCompare(b.type)}
              render={this.renderAclIdentityType}
              onFilter={(value: ACLIdentity, record: IAcl) => record.type === value}
            />
            <Column key='scope' title='ACL Scope' dataIndex='scope' />
            <Column key='acls' title='ACLs' dataIndex='aclList' render={this.renderAclList} />
          </Table>
        </Drawer>
      </div>
    );
  }
}
