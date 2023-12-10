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

#ifndef __FAILURE_INJECTOR_SVC_SERVER_H
#define __FAILURE_INJECTOR_SVC_SERVER_H

#include <iostream>
#include <memory>
#include <string>
#include <iostream>

#include <grpcpp/grpcpp.h>

#include "failure_injection_service.grpc.pb.h"
#include "failure_injector.h"
#include "run_grpc_service.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

namespace NoiseInjector {

class FailureInjectorSvcServiceImpl final : 
            public FailureInjectorSvc::Service {

public:
    Status GetStatus(ServerContext* context,
                     const GetStatusRequest* request,
                     GetStatusReply* reply) override;

    Status InjectFailure(ServerContext* context,
                         const InjectFailureRequest* request,
                         InjectFailureReply* reply) override;

    Status ResetFailure(ServerContext* context,
                        const ResetFailureRequest* request,
                        ResetFailureReply* reply);

    FailureInjectorSvcServiceImpl(FailureInjector* injector);

private :
  FailureInjector *mFailureInjector;
};

}

#endif /* __FAILURE_INJECTOR_SVC_SERVER_H */
