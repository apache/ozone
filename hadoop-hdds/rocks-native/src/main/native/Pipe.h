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

#ifndef ROCKS_NATIVE_PIPE_H
#define ROCKS_NATIVE_PIPE_H

#include <stdio.h>

class Pipe {
    public:
        static const int READ_FILE_DESCRIPTOR_IDX;
        static const int WRITE_FILE_DESCRIPTOR_IDX;
        Pipe();
        ~Pipe();
        void close();
        int getReadFd() {
            return getPipeFileDescriptorIndex(READ_FILE_DESCRIPTOR_IDX);
        }

        int getWriteFd() {
            return getPipeFileDescriptorIndex(WRITE_FILE_DESCRIPTOR_IDX);
        }

        int getPipeFileDescriptorIndex(int idx) {
            return p[idx];
        }

        bool isOpen() {
            return open;
        }


    private:
        int p[2];
        FILE* wr;
        bool open;

};

#endif //ROCKS_NATIVE_PIPE_H
