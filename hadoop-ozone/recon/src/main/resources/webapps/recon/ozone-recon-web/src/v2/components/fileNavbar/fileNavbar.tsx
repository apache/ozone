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

import React, { Key, useCallback, useState } from 'react';

import Tree, { DataNode, DirectoryTreeProps } from 'antd/es/tree';
import { DUSubpath } from '@/v2/types/diskUsage.types';
import { TreeProps } from 'antd/lib/tree';
import { Breadcrumb, Button, Menu } from 'antd';
import { MenuProps } from 'antd/es/menu';


type File = {
  path: string;
  subPaths: DUSubpath[];
  updateHandler: (arg0: string) => void;
};


const FileNavBar: React.FC<File> = ({
  path = '/',
  subPaths = [],
  updateHandler
}) => {
  const [currPath, setCurrPath] = useState<string[]>([]);

  function generateBreadCrumbMenu() {
    // We are not at root path
    if (path !== '/') {
      //Remove leading / and split to avoid empty string
      //Without the substring this will produce ['', 'pathLoc'] for /pathLoc
      const splitPath = path.substring(1).split('/');
      setCurrPath(
        ['/', ...splitPath]
      );
    }
    else {
      setCurrPath(['/']);
    }
  }

  const handleMenuClick: MenuProps['onClick'] = ({ key }) => {
    updateHandler(key as string);
  }

  function handleBreadcrumbClick(idx: number, lastPath: string) {
    /**
     * The following will generate  path like //vol1/buck1/dir1/key...
     * since the first element of the currPos is ['/']
     * we are joining that with / as well causing //
     */
    console.log(idx, lastPath);
    const constructedPath = [...currPath.slice(0, idx), lastPath].join('/');
    if (idx === 0) {
      //Root path clicked
      updateHandler('/');
    }
    else {
      // Pass the string without the leading /
      updateHandler(constructedPath.substring(1));
    }
  }

  function generateSubMenu(lastPath: string) {
    const menuItems = subPaths.map((subpath) => {
      const splitSubpath = subpath.path.split('/');
      return (
        <Menu.Item key={subpath.path}>
          {splitSubpath[splitSubpath.length - 1]}
        </Menu.Item>
      )
    });
    return (
      <Breadcrumb.Item key={lastPath}
        overlay={
          <Menu onClick={handleMenuClick}>
            {menuItems}
          </Menu>
        }>
        {lastPath}
      </Breadcrumb.Item>
    )
  }

  React.useEffect(() => {
    generateBreadCrumbMenu()
  }, [path]); //Anytime the path changes we need to generate the state

  function generateBreadCrumbs(){
    let breadCrumbs = [];
    currPath.forEach((location, idx) => {
      breadCrumbs.push(
        <Breadcrumb.Item
          key={location}>
          <button
            className='breadcrumb-nav-item'
            onClick={() => {handleBreadcrumbClick(idx, location)}}>{location}</button>
        </Breadcrumb.Item>
      );
    });
    //If current path has subpaths
    if(subPaths.length !== 0) {
      //create a submenu for the last path
      breadCrumbs[breadCrumbs.length - 1] = generateSubMenu(currPath[currPath.length - 1]);
    }
    return breadCrumbs;
  }

  return (
    <Breadcrumb
      separator=">"
      className='breadcrumb-nav'>
      {generateBreadCrumbs()}
    </Breadcrumb>
  )
}

export default FileNavBar;