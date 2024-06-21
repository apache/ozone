/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at

* http://www.apache.org/licenses/LICENSE-2.0

* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

/**
* AntD has a @ant-design/react-slick component with throttle-debounce
* We should remove this dependency as we are not using react-slick in our code
* and throttle-debounce is having MIT/GPL 2.0 license due to it being derivative
* of jquery-throttle-debounce
*/

function readAndOmitPkg(pkg, _) {
    if (pkg.name === 'throttle-debounce'){
        delete pkg.dependencies['throttle-debounce']
    }
    return pkg
}

module.exports = {
    hooks: {
        readAndOmitPkg
    }
}
