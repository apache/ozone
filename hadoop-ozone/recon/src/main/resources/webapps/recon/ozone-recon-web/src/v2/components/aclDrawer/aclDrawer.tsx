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

import React, { useEffect, useState } from 'react';
import { Table, Drawer, Tag } from 'antd';

import { AclRightsColorMap, AclIdColorMap } from '@/v2/constants/acl.constants';
import { Acl, ACLIdentity, ACLIdentityTypeList } from '@/v2/types/acl.types';
import { ColumnType } from 'antd/es/table';

// ------------- Types -------------- //
type AclDrawerProps = {
  visible: boolean;
  acls: Acl[] | undefined;
  entityName: string;
  entityType: string;
  onClose: () => void;
}


// ------------- Component -------------- //
const AclPanel: React.FC<AclDrawerProps> = ({
  visible,
  acls,
  entityType,
  entityName,
  onClose
}) => {
  const [isVisible, setIsVisible] = useState<boolean>(false);

  useEffect(() => {
    setIsVisible(visible);
  }, [visible]);

  const renderAclList = (_: string, acl: Acl) => {
    return acl.aclList.map(aclRight => (
      <Tag key={aclRight} color={AclRightsColorMap[aclRight as keyof typeof AclRightsColorMap]}>
        {aclRight}
      </Tag>
    ))
  }

  const renderAclIdentityType = (acl: string) => {
    return (
      <Tag color={AclIdColorMap[acl as keyof typeof AclIdColorMap]}>
        {acl}
      </Tag>
    )
  }

  const COLUMNS: ColumnType<Acl>[] = [
    {
      title: 'Name',
      dataIndex: 'name',
      key: 'name',
      sorter: (a: Acl, b: Acl) => a.name.localeCompare(b.name),
    },
    {
      title: 'ACL Type',
      dataIndex: 'type',
      key: 'type',
      filterMultiple: true,
      filters: ACLIdentityTypeList.map(state => ({ text: state, value: state })),
      onFilter: (value: ACLIdentity, record: Acl) => (record.type === value),
      sorter: (a: Acl, b: Acl) => a.type.localeCompare(b.type),
      render: renderAclIdentityType
    },
    {
      title: 'ACL Scope',
      dataIndex: 'scope',
      key: 'scope',
    },
    {
      title: 'ACLs',
      dataIndex: 'aclList',
      key: 'acls',
      render: renderAclList
    }
  ];

  return (
    <div className='site-drawer-render-in-current-wrapper'>
      <Drawer
        title={`ACL for ${entityType} ${entityName}`}
        placement='right'
        width='40%'
        closable={true}
        visible={isVisible}
        getContainer={false}
        style={{ position: 'absolute' }}
        onClose={onClose}
      >
        <Table
          dataSource={acls}
          rowKey={(record: Acl) => `${record.name ?? ''}-${record.type ?? ''}-${record.scope ?? ''}`}
          locale={{ filterTitle: '' }}
          columns={COLUMNS}>
        </Table>
      </Drawer>
    </div>
  );
};

export default AclPanel;