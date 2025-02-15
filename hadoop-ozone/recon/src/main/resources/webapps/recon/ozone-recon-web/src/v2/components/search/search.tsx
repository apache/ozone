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
import { Input, Select } from 'antd';

import { Option } from '@/v2/components/select/singleSelect';
import { DownOutlined } from '@ant-design/icons';

// ------------- Types -------------- //
type SearchProps = {
  disabled?: boolean;
  searchColumn?: string;
  searchInput: string;
  searchOptions?: Option[];
  onSearchChange: (
    arg0: React.ChangeEvent<HTMLInputElement>
  ) => void;
  onChange: (
    value: string,
    //OptionType, OptionGroupData and OptionData are not
    //currently exported by AntD hence set to any
    option: any
  ) => void;
}

// ------------- Component -------------- //
const Search: React.FC<SearchProps> = ({
  disabled = false,
  searchColumn,
  searchInput = '',
  searchOptions = [],
  onSearchChange = () => {},
  onChange = () => {}   // Assign default value as a void function
}) => {

  const selectFilter = searchColumn
    ? (<Select
      disabled={disabled}
      suffixIcon={(searchOptions.length > 1) ? <DownOutlined/> : null}
      defaultValue={searchColumn}
      options={searchOptions}
      onChange={onChange}
      data-testid='search-dropdown'/>)
    : null

  return (
    <Input
      disabled={disabled}
      placeholder='Enter Search text'
      allowClear={true}
      value={searchInput}
      addonBefore={selectFilter}
      onChange={onSearchChange}
      size='middle'
      style={{
        maxWidth: 400
      }}
      data-testid='search-input'/>
  )
}

export default Search;
