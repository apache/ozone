/**                                                                             
 * Licensed to the Apache Software Foundation (ASF) under one or more           
 * contributor license agreements.  See the NOTICE file distributed with this   
 * work for additional information regarding copyright ownership.  The ASF      
 * licenses this file to you under the Apache License, Version 2.0 (the         
 * "License"); you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at                                      
 * <p>                                                                          
 * http://www.apache.org/licenses/LICENSE-2.0                                   
 * <p>                                                                          
 * Unless required by applicable law or agreed to in writing, software          
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT     
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the     
 * License for the specific language governing permissions and limitations under
 * the License.                                                                 
 */                  

#ifndef __FAILURE_INJECTOR_H__
#define __FAILURE_INJECTOR_H__

#include<string>
#include<map>
#include<vector>

#define CORRUPT_DATA_ERROR_CODE (99999)
#define MICROSECONDS_IN_A_SECOND (1000 * 1000)

#define InjectedOperations(GEN_CODE)    \
    GEN_CODE(GETATTR),                  \
    GEN_CODE(READLINK),                 \
    GEN_CODE(MKNOD),                    \
    GEN_CODE(MKDIR),                    \
    GEN_CODE(UNLINK),                   \
    GEN_CODE(RMDIR),                    \
    GEN_CODE(SYMLINK),                  \
    GEN_CODE(RENAME),                   \
    GEN_CODE(LINK),                     \
    GEN_CODE(CHMOD),                    \
    GEN_CODE(CHOWN),                    \
    GEN_CODE(TRUNCATE),                 \
    GEN_CODE(OPEN),                     \
    GEN_CODE(READ),                     \
    GEN_CODE(WRITE),                    \
    GEN_CODE(STATFS),                   \
    GEN_CODE(FLUSH),                    \
    GEN_CODE(RELEASE),                  \
    GEN_CODE(FSYNC),                    \
    GEN_CODE(SETXATTR),                 \
    GEN_CODE(GETXATTR),                 \
    GEN_CODE(LISTXATTR),                \
    GEN_CODE(REMOVEXATTR),              \
    GEN_CODE(OPENDIR),                  \
    GEN_CODE(READDIR),                  \
    GEN_CODE(RELEASEDIR),               \
    GEN_CODE(FSYNCDIR),                 \
    GEN_CODE(INIT),                     \
    GEN_CODE(DESTROY),                  \
    GEN_CODE(ACCESS),                   \
    GEN_CODE(CREATE),                   \
    GEN_CODE(LOCK),                     \
    GEN_CODE(UTIMENS),                  \
    GEN_CODE(BMAP),                     \
    GEN_CODE(IOCTL),                    \
    GEN_CODE(POLL),                     \
    GEN_CODE(WRITE_BUF),                \
    GEN_CODE(READ_BUF),                 \
    GEN_CODE(FLOCK),                    \
    GEN_CODE(FALLOCATE),                \
    GEN_CODE(COPY_FILE_RANGE),          \
    GEN_CODE(LSEEK),                    \
    GEN_CODE(NUM_OP_CODES)

#define GEN_CODE_ENUM(x)  x

namespace NoiseInjector {

enum InjectedOpCode {NO_OP = 0, InjectedOperations(GEN_CODE_ENUM)};

struct InjectedAction {
    enum ActionCode {DELAY = 0, FAIL, CORRUPT} code;
    int delay; /* In seconds */
    int error_code;

    InjectedAction(int c, int d, int e) : delay(d), error_code(e) {
        switch (c) {
            case DELAY:
                code = DELAY;
                break; 
            case FAIL:
                code = FAIL;
                break; 
            case CORRUPT:
                code = CORRUPT;
                break; 
        }
    };
};

class FilePathFailures {
public:
    std::vector<InjectedAction>* GetFailures(InjectedOpCode op);
    void addFailure(InjectedOpCode op, InjectedAction action);
    void resetFailure(InjectedOpCode op);
    void resetFailure();
    void dumpFailures();

private:
    /* 
     * Same file path could have multiple failures injected for
     * different operations. For each {path, operation}, it 
     * is possible to have more than on action. For example,
     * path {/opt/ABC/, read} injected failures could be
     *  1. Delay the operation for 50 miliseconds, followed by
     *  2. Fail the operation with EIO
     * simulating a malfunction disk.
     */
     
    std::map<InjectedOpCode, std::vector<InjectedAction> > mInjectedOperations;
};

class FailureInjector {
public:
    std::vector<InjectedAction>* GetFailures(std::string path,
                                             std::string op_name);
    void InjectFailure(std::string path, std::string op_name,
                       InjectedAction action);
    void InjectFailure(std::string path, InjectedOpCode op,
                       InjectedAction action);
    void ResetFailure(std::string path, std::string op_name);
    void ResetFailure(std::string path, InjectedOpCode op);
    void ResetFailure(std::string path);
    void ResetFailure();
    void dumpFailures(std::string path);
    void dumpFailures();
    InjectedOpCode  getOpCode(std::string op_name);

private:
    std::map<std::string, FilePathFailures *> mAllInjectedFailures_;

friend class TestFailureInjector;
};

} /* namespace NoiseInjector */

#endif  /*  __FAILURE_INJECTOR_H */
