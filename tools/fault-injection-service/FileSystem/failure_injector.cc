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

#include<string>
#include<map>
#include<vector>
#include<iostream>
#include"failure_injector.h"

using namespace std;
using namespace NoiseInjector;

#define GEN_CODE_NAMES(x)  #x

string InjectedOpNames[] = {"NO_OP", InjectedOperations(GEN_CODE_NAMES)};

vector<InjectedAction> *FilePathFailures::GetFailures(InjectedOpCode op)
{
    auto iter = mInjectedOperations.find(op);
    if (iter == mInjectedOperations.end()) {
        return NULL;
    }
    return (vector<InjectedAction> *)&iter->second;
}

void FilePathFailures::addFailure(
    InjectedOpCode op,
    InjectedAction action) 
{
    auto iter = mInjectedOperations.find(op);
    if (iter == mInjectedOperations.end()) {
        mInjectedOperations[op].push_back(action);
    } else {
        vector<InjectedAction> &action_vector = iter->second;
        for (auto &existing_action : action_vector) {
            if (action.code == existing_action.code) {
                return;
            }
        }
        action_vector.push_back(action);
    }
}

void FilePathFailures::resetFailure(InjectedOpCode op)
{
    auto itr = mInjectedOperations.find(op);
    if (itr != mInjectedOperations.end()) {
        mInjectedOperations[op].clear();
        mInjectedOperations.erase(op);
    }
}

void FilePathFailures::resetFailure()
{
    for (auto iter:mInjectedOperations) {
        InjectedOpCode op = iter.first;
        mInjectedOperations[op].clear();
        mInjectedOperations.erase(op);
    }
}

void FilePathFailures::dumpFailures()
{
    for (auto iter:mInjectedOperations) {
        InjectedOpCode op = iter.first;
        for (const auto action : mInjectedOperations[op]) {
            cout << "\t" << InjectedOpNames[op] << ": " << action.code << "\n";
        }
    }
}

InjectedOpCode  FailureInjector::getOpCode(std::string op_name)
{
    int i = 0;
    for (auto name : InjectedOpNames) {
       if (!name.compare(op_name)) {
            return (InjectedOpCode)i;
       }
        ++i; 
    }
    return  NO_OP;
}

std::vector<InjectedAction>* FailureInjector::GetFailures(
    std::string path,
    std::string op_name)
{
    auto op =getOpCode(op_name);
    if (op >= NUM_OP_CODES) {
        return NULL;
    }

    auto iter = mAllInjectedFailures_.find(path);
    if (iter != mAllInjectedFailures_.end()) {
        FilePathFailures* failure = iter->second;
        return failure->GetFailures(op);
    }
    return NULL;
}

void FailureInjector::InjectFailure(
    std::string path,
    std::string op_name,
    InjectedAction action)
{
    InjectFailure(path, getOpCode(op_name), action);
}

void FailureInjector::InjectFailure(
    std::string path,
    InjectedOpCode op,
    InjectedAction action) 
{
    if (op >= NUM_OP_CODES) {
        return;
    }
    auto iter = mAllInjectedFailures_.find(path);
    if (iter == mAllInjectedFailures_.end()) {
        FilePathFailures* failure = new FilePathFailures();
        failure->addFailure(op, action);
        mAllInjectedFailures_[path] = failure;
    } else {
        FilePathFailures* failure = iter->second;
        failure->addFailure(op, action);
    }

}

void FailureInjector::ResetFailure(
    std::string path,
    std::string op_name) 
{
    auto op =getOpCode(op_name);
    if (op >= NUM_OP_CODES) {
        return NULL;
    }
    ResetFailure(path, op);
}

void FailureInjector::ResetFailure(
    std::string path,
    InjectedOpCode op) 
{
    auto iter = mAllInjectedFailures_.find(path);
    if (iter != mAllInjectedFailures_.end()) {
        FilePathFailures* failure = iter->second;
        failure->resetFailure(op);
    }
}

void FailureInjector::ResetFailure(
    std::string path) 
{
    auto iter = mAllInjectedFailures_.find(path);
    if (iter != mAllInjectedFailures_.end()) {
        FilePathFailures* failure = iter->second;
        failure->resetFailure();
        mAllInjectedFailures_.erase(path);
    }
}

void FailureInjector::ResetFailure() 
{
    for (auto iter : mAllInjectedFailures_) {
        FilePathFailures* failure = iter.second;
        failure->resetFailure();
    }
    mAllInjectedFailures_.clear();
}

void FailureInjector::dumpFailures(std::string path) 
{
    auto iter = mAllInjectedFailures_.find(path);
    if (iter != mAllInjectedFailures_.end()) {
        FilePathFailures* failure = iter->second;
        failure->dumpFailures();
    }
}

void FailureInjector::dumpFailures() 
{
    for (auto iter : mAllInjectedFailures_) {
        FilePathFailures* failure = iter.second;
        cout << "Path: " << iter.first << "\n";
        failure->dumpFailures();
    }
}
