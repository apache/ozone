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
 
class TestFilePathFailures : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(TestFilePathFailures);
    CPPUNIT_TEST(testFilePathFailures);
    CPPUNIT_TEST_SUITE_END();
 
public:
    void setUp(void);
    void tearDown(void);
 
protected:
    void testFilePathFailures();
 
private:
 
    FilePathFailures *mTestObj;
};
 
//-----------------------------------------------------------------------------

void TestFilePathFailures::testFilePathFailures()
{
    mTestObj->addFailure(READ,
                        InjectedAction(InjectedAction::ActionCode::DELAY,
                                       1, 0));
    CPPUNIT_ASSERT(mTestObj->GetFailures(READ)->size() == 1);

    mTestObj->addFailure(READ,
                        InjectedAction(InjectedAction::ActionCode::DELAY,
                                       1, 0));
    // Redundant actions should not be added
    CPPUNIT_ASSERT(mTestObj->GetFailures(READ)->size() == 1);
    CPPUNIT_ASSERT(mTestObj->GetFailures(READ)->size() == 1);
    CPPUNIT_ASSERT((*mTestObj->GetFailures(READ))[0].code == 
                                         InjectedAction::ActionCode::DELAY);
    mTestObj->addFailure(READ,
                        InjectedAction(InjectedAction::ActionCode::FAIL,
                                       1, 0));
    mTestObj->addFailure(READ,
                        InjectedAction(InjectedAction::ActionCode::CORRUPT,
                                       1, 0));
    CPPUNIT_ASSERT(mTestObj->GetFailures(READ)->size() == 3);
    mTestObj->addFailure(FLOCK,
                        InjectedAction(InjectedAction::ActionCode::DELAY,
                                       1, 0));
    CPPUNIT_ASSERT(mTestObj->GetFailures(FLOCK)->size() == 1);
    CPPUNIT_ASSERT((*mTestObj->GetFailures(FLOCK))[0].code == 
                                         InjectedAction::ActionCode::DELAY);
    mTestObj->dumpFailures();
}

//-----------------------------------------------------------------------------
 
void TestFilePathFailures::setUp(void)
{
    mTestObj = new FilePathFailures();
}
 
void TestFilePathFailures::tearDown(void)
{
    delete mTestObj;
}
 
//-----------------------------------------------------------------------------
 
CPPUNIT_TEST_SUITE_REGISTRATION( TestFilePathFailures );
 
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
    ofstream xmlFileOut("cppTestFilePathFailuresResults.xml");
    XmlOutputter xmlOut(&collectedresults, xmlFileOut);
    xmlOut.write();
 
    // return 0 if tests were successful
    return collectedresults.wasSuccessful() ? 0 : 1;
}

