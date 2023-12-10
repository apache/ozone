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

#ifndef __FAILURE_INJECTOR_SVC_CLIENT_H
#define __FAILURE_INJECTOR_SVC_CLIENT_H

#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "failure_injection_service.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

namespace NoiseInjector {

class FailureInjectorSvcClient {
 public:
  FailureInjectorSvcClient(std::shared_ptr<Channel> channel);

  std::string GetStatus(const std::string& user); 

  void InjectFailure(std::string path, std::string op, int action, int param);

  void ResetFailure(std::string path, std::string op);

 private:
  std::unique_ptr<FailureInjectorSvc::Stub> stub_;
};

}

#endif /* __FAILURE_INJECTOR_SVC_CLIENT_H */
