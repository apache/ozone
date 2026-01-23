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

import React, { useState } from 'react';

import { NUSubpath } from '@/v2/types/namespaceUsage.types';
import { Breadcrumb, Menu, Input } from 'antd';
import { CaretDownOutlined, HomeFilled } from '@ant-design/icons';
import { MenuProps } from 'antd/es/menu';


type File = {
  path: string;
  subPaths: NUSubpath[];
  updateHandler: (arg0: string) => void;
};


const DUBreadcrumbNav: React.FC<File> = ({
  path = '/',
  subPaths = [],
  updateHandler
}) => {
  const [currPath, setCurrPath] = useState<string[]>([]);

  function generateCurrentPathState() {
    // We are not at root path
    if (path !== '/') {
      /**
       * Remove leading / and split to avoid empty string 
       * Without the substring this will produce ['', 'pathLoc'] for /pathLoc
       */
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
    //If click is not on search panel
    if (!(key as string).includes('-search')){
      updateHandler(key as string);
    }
  }

  function handleSearch(value: string) {
    /**
     * The following will generate  path like //vol1/buck1/dir1/key...
     * since the first element of the currPos is ['/']
     * we are joining that with / as well causing //
     * Hence we substring from 1st index to remove one / from path
     */
    updateHandler([...currPath, value].join('/').substring(1));
  }

  function handleBreadcrumbClick(idx: number, lastPath: string) {
    /**
     * The following will generate  path like //vol1/buck1/dir1/key...
     * since the first element of the currPos is ['/']
     * we are joining that with / as well causing //
     */
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
      // Do not add any menu item for keys i.e keys cannot be drilled down
      // further
      if (!subpath.isKey) {
        const splitSubpath = subpath.path.split('/');
        return (
          <Menu.Item key={subpath.path}>
            {splitSubpath[splitSubpath.length - 1]}
          </Menu.Item>
        );
      }
      
    });
    //Push a new input to allow passing a path
    menuItems.push(
      <Menu.Item
        key={`${lastPath}-search`}
        style={{ width: '100%'}}>
        <Input.Search
          placeholder='Enter Path'
          onSearch={handleSearch}
          onClick={(e) => {
            //Prevent menu close on click
            e.stopPropagation();
          }} />
      </Menu.Item>
    )
    return (
      <Breadcrumb.Item key={lastPath}
        overlay={
          <Menu
            onClick={handleMenuClick}
            mode='inline'
            expandIcon={<CaretDownOutlined/>}>
            {menuItems}
          </Menu>
        }
        dropdownProps={{
          trigger: ['click'] 
        }}>
        {(lastPath === '/') ? <HomeFilled style={{fontSize: '16px'}}/> : lastPath}
      </Breadcrumb.Item>
    )
  }

  React.useEffect(() => {
    generateCurrentPathState()
  }, [path]); //Anytime the path changes we need to generate the state

  function generateBreadCrumbs(){
    let breadCrumbs = [];
    currPath.forEach((location, idx) => {
      breadCrumbs.push(
        <Breadcrumb.Item
          key={location}>
          {(location === '/')
            ? <HomeFilled
                onClick={() => {handleBreadcrumbClick(idx, location)}}
                style={{color: '#1aa57a'}} />
            : (<button
                className='breadcrumb-nav-item'
                onClick={() => {handleBreadcrumbClick(idx, location)}}>
                  {location}
              </button>)}
        </Breadcrumb.Item>
      );
    });
    breadCrumbs[breadCrumbs.length - 1] = generateSubMenu(currPath[currPath.length - 1]);
    return breadCrumbs;
  }

  return (
    <div id='breadcrumb-container'>
      <Breadcrumb
        separator={'/'}
        className='breadcrumb-nav'>
          {generateBreadCrumbs()}
      </Breadcrumb>
    </div>
  )
}

export default DUBreadcrumbNav;