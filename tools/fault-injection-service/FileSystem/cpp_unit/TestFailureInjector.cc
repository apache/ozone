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
#include <string>
#include <list>
#include <cppunit/TestCase.h>
#include <cppunit/TestFixture.h>
#include <cppunit/ui/text/TextTestRunner.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestResultCollector.h>
#include <cppunit/TestRunner.h>
#include <cppunit/BriefTestProgressListener.h>
#include <cppunit/CompilerOutputter.h>
#include <cppunit/XmlOutputter.h>
#include <netinet/in.h>
#include "failure_injector.h"
 
using namespace CppUnit;
using namespace std;
using namespace NoiseInjector;

//-----------------------------------------------------------------------------
 
class TestFailureInjector : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(TestFailureInjector);
    CPPUNIT_TEST(testFailureInjector);
    CPPUNIT_TEST_SUITE_END();
 
public:
    void setUp(void);
    void tearDown(void);
 
protected:
    void testFailureInjector();
    void testGetOpCode();
 
private:
 
    FailureInjector *mTestObj;
};
 
//-----------------------------------------------------------------------------

void TestFailureInjector::testGetOpCode()
{
    CPPUNIT_ASSERT(mTestObj->getOpCode("NO_OP") == NO_OP); 
    CPPUNIT_ASSERT(mTestObj->getOpCode("GETATTR") == GETATTR); 
    CPPUNIT_ASSERT(mTestObj->getOpCode("READDIR") == READDIR); 
    CPPUNIT_ASSERT(mTestObj->getOpCode("LSEEK") == LSEEK); 
    CPPUNIT_ASSERT(mTestObj->getOpCode("NUM_OP_CODES") == NUM_OP_CODES); 
}
 
//-----------------------------------------------------------------------------

void TestFailureInjector::testFailureInjector()
{
    string path("/opt/test.xyz");
    string path2("/opt/test2.xyz");

    testGetOpCode();
    mTestObj->InjectFailure(path, "RENAME",
                        InjectedAction(InjectedAction::ActionCode::DELAY,
                                       1, 0));
    CPPUNIT_ASSERT(mTestObj->GetFailures(path, "RENAME")->size() == 1);
    CPPUNIT_ASSERT((*mTestObj->GetFailures(path, "RENAME"))[0].code == 
                                         InjectedAction::ActionCode::DELAY);

    // two actions for (path, RENAME)
    mTestObj->InjectFailure(path, "RENAME",
                        InjectedAction(InjectedAction::ActionCode::FAIL,
                                       1, 0));
    CPPUNIT_ASSERT(mTestObj->GetFailures(path, "RENAME")->size() == 2);
    CPPUNIT_ASSERT((*mTestObj->GetFailures(path, "RENAME"))[0].code == 
                                         InjectedAction::ActionCode::DELAY);
    CPPUNIT_ASSERT((*mTestObj->GetFailures(path, "RENAME"))[1].code == 
                                         InjectedAction::ActionCode::FAIL);

    // one actions for (path,READ)
    mTestObj->InjectFailure(path, "READ",
                        InjectedAction(InjectedAction::ActionCode::DELAY,
                                       1, 0));
    CPPUNIT_ASSERT(mTestObj->GetFailures(path, "READ")->size() == 1);
    CPPUNIT_ASSERT((*mTestObj->GetFailures(path, "READ"))[0].code == 
                                         InjectedAction::ActionCode::DELAY);

    // one actions for (path2,LOCK)
    mTestObj->InjectFailure(path2, "LOCK",
                        InjectedAction(InjectedAction::ActionCode::DELAY,
                                       1, 0));
    CPPUNIT_ASSERT(mTestObj->GetFailures(path2, "LOCK")->size() == 1);
    CPPUNIT_ASSERT((*mTestObj->GetFailures(path2, "LOCK"))[0].code == 
                                         InjectedAction::ActionCode::DELAY);

    mTestObj->dumpFailures();
    
    mTestObj->ResetFailure(path, READ);
    CPPUNIT_ASSERT(mTestObj->GetFailures(path, "READ") == NULL);
    CPPUNIT_ASSERT(mTestObj->GetFailures(path, "RENAME")->size() == 2);

    mTestObj->ResetFailure(path);
    CPPUNIT_ASSERT(mTestObj->GetFailures(path, "RENAME") == NULL);

    mTestObj->ResetFailure();
    CPPUNIT_ASSERT(mTestObj->GetFailures(path, "RENAME") == NULL);
    CPPUNIT_ASSERT(mTestObj->GetFailures(path, "READ") == NULL);
    CPPUNIT_ASSERT(mTestObj->GetFailures(path2, "LOCK") == NULL);

    mTestObj->dumpFailures();
}

//-----------------------------------------------------------------------------
 
void TestFailureInjector::setUp(void)
{
    mTestObj = new FailureInjector();
}
 
void TestFailureInjector::tearDown(void)
{
    delete mTestObj;
}
 
//-----------------------------------------------------------------------------
 
CPPUNIT_TEST_SUITE_REGISTRATION( TestFailureInjector );
 
int main(int argc, char* argv[])
{
    // informs test-listener about testresults
    CPPUNIT_NS::TestResult testresult;
 
    // register listener for collecting the test-results
    CPPUNIT_NS::TestResultCollector collectedresults;
    testresult.addListener (&collectedresults);
 
    // register listener for per-test progress output
    CPPUNIT_NS::BriefTestProgressListener progress;
    testresult.addListener (&progress);
 
    // insert test-suite at test-runner by registry
    CPPUNIT_NS::TestRunner testrunner;
    testrunner.addTest (CPPUNIT_NS::TestFactoryRegistry::getRegistry().makeTest ());
    testrunner.run(testresult);
 
    // output results in compiler-format
    CPPUNIT_NS::CompilerOutputter compileroutputter(&collectedresults, std::cerr);
    compileroutputter.write ();
 
    // Output XML for Jenkins CPPunit plugin
    ofstream xmlFileOut("cppTestFailureInjectorResults.xml");
    XmlOutputter xmlOut(&collectedresults, xmlFileOut);
    xmlOut.write();
 
    // return 0 if tests were successful
    return collectedresults.wasSuccessful() ? 0 : 1;
}

