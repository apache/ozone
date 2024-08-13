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

type SearchProps = {
  searchColumn?: string;
  searchOptions?: Option[];
  onSearch: (
    arg0: string,
    arg1: React.ChangeEvent<HTMLInputElement>
    | React.MouseEvent<HTMLElement, MouseEvent>
    | React.KeyboardEvent<HTMLInputElement>
    | undefined
  ) => void;
  onChange: (
    value: string,
    //OptionType, OptionGroupData and OptionData are not
    //currently exported by AntD hence set to any
    option: any
  ) => void;
}

const Search: React.FC<SearchProps> = ({
  searchColumn,
  searchOptions = [],
  onSearch = () => {},  // Assign default value as a void function
  onChange = () => {}   // Assign default value as a void function
}) => {

  const selectFilter = searchColumn
    ? (<Select
      defaultValue={searchColumn}
      options={searchOptions}
      onChange={onChange} />)
    : null

  return (
    <Input.Search
      placeholder='Enter Search text'
      allowClear={true}
      addonBefore={selectFilter}
      onSearch={onSearch}
      size='middle'
      style={{
        maxWidth: 400
      }}/>
  )
}

export default Search;
