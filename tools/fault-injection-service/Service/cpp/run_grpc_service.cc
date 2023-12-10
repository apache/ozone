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
#include "failure_injector_svc_server.h"
#include "run_grpc_service.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;

using namespace NoiseInjector;
using namespace std;

void RunGrpcService::RunServer() 
{
  string server_address("0.0.0.0:50051");
  FailureInjectorSvcServiceImpl service(mFailureInjector);

  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  unique_ptr<Server> server(builder.BuildAndStart());
  cout << "Server listening on " << server_address << endl;

  // Wait for the server to shutdown.
  server->Wait();
}

RunGrpcService::RunGrpcService(FailureInjector *injector)
{
    mFailureInjector = injector;
}
