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
#include <unistd.h>

#include <grpcpp/grpcpp.h>

#include "failure_injection_service.grpc.pb.h"
#include "failure_injector_svc_client.h"
#include "failure_injector.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using namespace NoiseInjector;
using namespace std;

FailureInjectorSvcClient::FailureInjectorSvcClient(
    shared_ptr<Channel> channel)
      : stub_(FailureInjectorSvc::NewStub(channel)) {}

string FailureInjectorSvcClient::GetStatus(const string& user)
{
    GetStatusRequest request;
    request.set_name(user);

    GetStatusReply reply;

    ClientContext context;

    Status status = stub_->GetStatus(&context, request, &reply);

    if (status.ok()) {
        return reply.message();
    } else {
        cout << status.error_code() << ": " << status.error_message()
                << endl;
        return "RPC failed";
    }
}

void FailureInjectorSvcClient::InjectFailure(
    string path,
    string op,
    int action,
    int param)
    
{
    InjectFailureRequest request;

    switch (action) {
    case 0:
        request.set_action_code(InjectFailureRequest::DELAY);
        break;
    case 1:
        request.set_action_code(InjectFailureRequest::FAIL);
        break;
    case 2:
        request.set_action_code(InjectFailureRequest::CORRUPT);
        break;
    }

    request.set_path(path);
    request.set_op_name(op);
    request.set_action_param(param);


    InjectFailureReply reply;

    ClientContext context;

    Status status = stub_->InjectFailure(&context, request, &reply);

    if (status.ok()) {
        cout << "RPC Success\n";
    } else {
        cout << status.error_code() << ": " << status.error_message()
                << endl;
        cout << "RPC failed\n";
    }
}

void FailureInjectorSvcClient::ResetFailure(
    string path,
    string op)
{
    ResetFailureRequest request;

    request.set_path(path);
    request.set_op_name(op);

    ResetFailureReply reply;

    ClientContext context;

    Status status = stub_->ResetFailure(&context, request, &reply);

    if (status.ok()) {
      cout << "RPC Success\n";
    } else {
      cout << status.error_code() << ": " << status.error_message()
                << endl;
      cout << "RPC failed\n";
    }
}

void usage(char *prog)
{
    cout << prog << ": -irs -o operation -p path -a action -d delay ";
    cout << "-e error_code\n" ;
    cout << " -i: Inject failure\n";
    cout << " -r: Reset failure \n";
    cout << " -s: status of NoiseInjection service \n";
    cout << " -p path: path to inject failure \n";
    cout << " -o operation: operation to inject failure \n";
    cout << " -a action: injected action (0: DELAY/1: FAIL/2: CORRUPT) \n";
    cout << " -d delay: delay in seconds\n";
    cout << " -e error code to return\n";

    exit(1);
}

int main(int argc, char** argv) 
{
    // Instantiate the client.
    FailureInjectorSvcClient injector(grpc::CreateChannel(
        "localhost:50051", grpc::InsecureChannelCredentials()));
    string reply;
    int opt;
    int flag = 0;
    string path;
    string operation;
    int action = InjectedAction::FAIL;
#define INVALID_PARAM   (-9999)
    int param = INVALID_PARAM;

    while ((opt = getopt(argc, argv, "irsp:o:a:d:e:")) != -1) {
        switch(opt) {
        case 'i':
            flag = 'i';
            break;
        case 'r':
            flag = 'r';
            break;
        case 's':
            flag = 's';
            break;
        case 'p':
            path = optarg; 
            break;
        case 'o':
            operation = optarg; 
            break;
        case 'a':
            action =atoi(optarg); 
            break;
        case 'd':
        case 'e':
            if (param == INVALID_PARAM) {
                param =atoi(optarg);
            } else {
                usage(argv[0]);
            }
            break;
        default:
            usage(argv[0]);
        }
    }

    switch (flag) {
    case 'i':
        if (param == INVALID_PARAM || action > InjectedAction::CORRUPT ||
            path.empty() || operation.empty()) {
            usage(argv[0]);
        }
        injector.InjectFailure(path, operation, action, param);
        break;
    case 'r':
        injector.ResetFailure(path, operation);
        break;
    case 's':
        reply = injector.GetStatus("Some Status");
        cout << "FailureInjectorSvc received: ";
        cout << reply << endl;
        break;
    default:
        usage(argv[0]);
    }

    return 0;
}
