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

import React from "react";
import { Tag } from "antd";
import { createPortal } from "react-dom";


// ------------- Types -------------- //
/**
 *  Due to design decisions we are currently not using the Tags
 * Until we reach a concensus on a better way to display the filter
 * Keeping the code in case we require it in the future
 */
export type TagProps = {
  label: string;
  closable: boolean;
  tagRef: React.RefObject<HTMLDivElement>;
  onClose: (arg0: string) => void;
}

// ------------- Component -------------- //
const ColumnTag: React.FC<TagProps> = ({
  label = '',
  closable = true,
  tagRef = null,
  onClose = () => {} // Assign default value as void funciton
}) => {
  const onPreventMouseDown = (event: React.MouseEvent<HTMLSpanElement>) => {
    // By default when clickin on the tags the text will get selected
    // which might interfere with user experience as people would want to close tags
    // but accidentally select tag text. Hence we prevent this behaviour.
    event.preventDefault();
    event.stopPropagation();
  };

  if (!tagRef?.current) return null;

  return createPortal(
    <Tag
      key={label}
      onMouseDown={onPreventMouseDown}
      closable={closable}
      onClose={() => (onClose(label))}
      style={{marginRight: 3}}>
        {label}
    </Tag>,
    tagRef.current
  );
}

export default ColumnTag;
