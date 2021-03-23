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

#include <iostream>
#include <memory>
#include <string>
#include <iostream>

#include <grpcpp/grpcpp.h>

#include "failure_injection_service.grpc.pb.h"
#include "failure_injector.h"
#include "failure_injector_svc_server.h"
#include "run_grpc_service.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using namespace NoiseInjector;
using namespace std;

Status FailureInjectorSvcServiceImpl::GetStatus(
    ServerContext* context,
    const GetStatusRequest* request,
    GetStatusReply* reply)
{

    string prefix("GetStatus ");
    cout << "Received request " << request->name() << "\n";
    reply->set_message(prefix + request->name());
    return Status::OK;
}

Status FailureInjectorSvcServiceImpl::InjectFailure(
    ServerContext* context,
    const InjectFailureRequest* request,
    InjectFailureReply* reply)
{
  cout << "Received request " << request->path() << ":" 
            << request->op_name() << "\n";
  mFailureInjector->InjectFailure(request->path(), request->op_name(),
                                InjectedAction((int)request->action_code(),
                                               (int)request->action_param(),
                                               (int)request->action_param()));
  return Status::OK;
}

Status FailureInjectorSvcServiceImpl::ResetFailure(
    ServerContext* context,
    const ResetFailureRequest* request,
    ResetFailureReply* reply)
{
    cout << "Received request ";
    if (request->has_path()) { 
            cout << request->path() << "\n";
        if (request->has_op_name()) {
            cout << request->op_name() << "\n";
        }
    }

    if (request->has_path() && !request->path().empty()) {
        if (request->has_op_name() && !request->op_name().empty()) {
            mFailureInjector->ResetFailure(request->path(), request->op_name());
        } else {
            mFailureInjector->ResetFailure(request->path());
        }
    } else {
        mFailureInjector->ResetFailure();
    }
    return Status::OK;
}

FailureInjectorSvcServiceImpl::FailureInjectorSvcServiceImpl(
    FailureInjector* injector)
{
    mFailureInjector = injector;
}
