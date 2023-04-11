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
import {Table, Drawer, Tag} from 'antd';
import {IAcl} from "../../types/om.types";
import {aclRightColorMap, aclTypeColorMap} from "../../constants/aclDrawer.constants";

interface IAclDrawerProps extends RouteComponentProps<object> {
    visible: boolean;
    acls: IAcl[] | undefined;
    objName: string;
    objType: string;
}

export class AclPanel extends React.Component<IAclDrawerProps> {
    state = {visible: false};

    componentWillReceiveProps(props) {
        const {visible} = props;
        this.setState({visible});
    }

    onClose = () => {
        this.setState({
            visible: false
        });
    };

    renderAclList(_, acl: IAcl) {
        return acl.aclList.map((aclRight) => {
            return (
                <Tag color={aclRightColorMap[aclRight]}>{aclRight}</Tag>
            )
        })
    }

    renderAclType(_, acl: IAcl) {
        return (
            <Tag color={aclTypeColorMap[acl.type]}>{acl.type}</Tag>
        )
    }

    render() {
        const {Column} = Table;
        const {objName, objType, acls} = this.props;
        const {visible} = this.state;

        return (
            <div className='site-drawer-render-in-current-wrapper'>
                <Drawer
                    title={`ACL for ${objType} ${objName}`}
                    placement='right'
                    width='40%'
                    closable={false}
                    visible={visible}
                    getContainer={false}
                    style={{position: 'absolute'}}
                    onClose={this.onClose}
                >
                    <Table dataSource={acls}>
                        <Column title='Name' dataIndex='name' key='name'/>
                        <Column title='ACL Type' dataIndex='type' render={this.renderAclType} key='type'/>
                        <Column title='ACLs' dataIndex='aclList' render={this.renderAclList} key='aclList'/>
                    </Table>
                </Drawer>
            </div>
        );
    }
}
