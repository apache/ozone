#!/usr/bin/env bats
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

load find_test_class_project.sh

setup() {
  # Create a temporary directory for test files
  TEST_DIR=$(mktemp -d)
  mkdir -p "${TEST_DIR}/project1/src/test/java/org/apache/ozone/test"
  mkdir -p "${TEST_DIR}/project2/src/test/java/org/apache/ozone/test"  
  touch "${TEST_DIR}/project1/pom.xml"
  touch "${TEST_DIR}/project2/pom.xml"
  touch "${TEST_DIR}/project1/src/test/java/org/apache/ozone/test/TestClass1.java"
  touch "${TEST_DIR}/project2/src/test/java/org/apache/ozone/test/TestClass2.java"
  
  ORIG_DIR=$(pwd)
  cd "${TEST_DIR}"
}

teardown() {
  cd "${ORIG_DIR}"
  rm -rf "${TEST_DIR}"
}

# Test the find_project_paths_for_test_class function

@test "find project for simple class name" {
  result=$(find_project_paths_for_test_class "TestClass1" 2>/dev/null)

  [ "$result" = "./project1" ]
}

@test "find project for class with package" {
  result=$(find_project_paths_for_test_class "org.apache.ozone.test.TestClass2" 2>/dev/null)

  [ "$result" = "./project2" ]
}

@test "find project for wildcard class" {
  result=$(find_project_paths_for_test_class "TestClass*" 2>/dev/null)
  expected=$(echo -e "./project1\n./project2")

  [ "$result" = "$expected" ]
}

@test "no project for non-existent class" {
  result=$(find_project_paths_for_test_class "NonExistentClass" 2>/dev/null)

  [ -z "$result" ]
}

@test "skip abstract classes" {
  touch "${TEST_DIR}/project1/src/test/java/org/apache/ozone/test/AbstractTestClass.java"
  
  result=$(find_project_paths_for_test_class "AbstractTestClass" 2>/dev/null)
  
  [ -z "$result" ]
}

@test "empty class name returns nothing" {
  result=$(find_project_paths_for_test_class "" 2>/dev/null)
  
  [ -z "$result" ]
}

@test "multiple projects with same test class name" {
  touch "${TEST_DIR}/project1/src/test/java/org/apache/ozone/test/CommonTest.java"
  touch "${TEST_DIR}/project2/src/test/java/org/apache/ozone/test/CommonTest.java"
  
  result=$(find_project_paths_for_test_class "CommonTest" 2>/dev/null)
  
  expected=$(echo -e "./project1\n./project2")
  [ "$result" = "$expected" ]
}

@test "project without pom.xml is ignored" {
  mkdir -p "${TEST_DIR}/project3/src/test/java/org/apache/ozone/test"
  touch "${TEST_DIR}/project3/src/test/java/org/apache/ozone/test/TestClass3.java"
  
  result=$(find_project_paths_for_test_class "TestClass3" 2>/dev/null)
  
  [ -z "$result" ]
}

@test "partial package name search" {
  result=$(find_project_paths_for_test_class "ozone.test.TestClass2" 2>/dev/null)
  
  [ "$result" = "./project2" ]
}

@test "test class in non-standard test directory" {
  mkdir -p "${TEST_DIR}/project1/src/test/scala/org/apache/ozone/test"
  touch "${TEST_DIR}/project1/src/test/scala/org/apache/ozone/test/ScalaTest.java"
  
  result=$(find_project_paths_for_test_class "ScalaTest" 2>/dev/null)
  
  [ "$result" = "./project1" ]
}

@test "case sensitivity in class name" {
  touch "${TEST_DIR}/project1/src/test/java/org/apache/ozone/test/MixedCaseTest.java"
  
  result=$(find_project_paths_for_test_class "mixedcasetest" 2>/dev/null)

  [ -z "$result" ]
}

@test "nested project structure" {
  mkdir -p "${TEST_DIR}/parent/child/src/test/java/org/apache/ozone/test"
  touch "${TEST_DIR}/parent/child/pom.xml"
  touch "${TEST_DIR}/parent/child/src/test/java/org/apache/ozone/test/NestedTest.java"
  
  result=$(find_project_paths_for_test_class "NestedTest" 2>/dev/null)
  
  [ "$result" = "./parent/child" ]
}

@test "test class with numeric suffix" {
  touch "${TEST_DIR}/project1/src/test/java/org/apache/ozone/test/Test1.java"
  touch "${TEST_DIR}/project1/src/test/java/org/apache/ozone/test/Test2.java"
  touch "${TEST_DIR}/project2/src/test/java/org/apache/ozone/test/Test3.java"
  
  result=$(find_project_paths_for_test_class "Test[1-2]" 2>/dev/null)
  
  [ "$result" = "./project1" ]
}

@test "multiple test classes matching pattern" {
  touch "${TEST_DIR}/project1/src/test/java/org/apache/ozone/test/TestA.java"
  touch "${TEST_DIR}/project2/src/test/java/org/apache/ozone/test/TestB.java"
  touch "${TEST_DIR}/project2/src/test/java/org/apache/ozone/test/TestC.java"
  
  result=$(find_project_paths_for_test_class "Test[A-C]" 2>/dev/null)
  
  expected=$(echo -e "./project1\n./project2")
  [ "$result" = "$expected" ]
}

@test "test class in multiple package levels" {
  mkdir -p "${TEST_DIR}/project1/src/test/java/org/apache/ozone/test/deep/nested/pkg"
  touch "${TEST_DIR}/project1/src/test/java/org/apache/ozone/test/deep/nested/pkg/DeepTest.java"
  
  result=$(find_project_paths_for_test_class "org.apache.ozone.test.deep.nested.pkg.DeepTest" 2>/dev/null)
  
  [ "$result" = "./project1" ]
}

@test "test class with same name in different packages" {
  mkdir -p "${TEST_DIR}/project1/src/test/java/org/apache/ozone/test/pkg1"
  mkdir -p "${TEST_DIR}/project2/src/test/java/org/apache/ozone/test/pkg2"
  touch "${TEST_DIR}/project1/src/test/java/org/apache/ozone/test/pkg1/SameNameTest.java"
  touch "${TEST_DIR}/project2/src/test/java/org/apache/ozone/test/pkg2/SameNameTest.java"
  
  result=$(find_project_paths_for_test_class "SameNameTest" 2>/dev/null)
  
  expected=$(echo -e "./project1\n./project2")
  [ "$result" = "$expected" ]
}

@test "test class with package wildcard" {
  mkdir -p "${TEST_DIR}/project1/src/test/java/org/apache/ozone/test/pkg1"
  mkdir -p "${TEST_DIR}/project2/src/test/java/org/apache/ozone/test/pkg2"
  touch "${TEST_DIR}/project1/src/test/java/org/apache/ozone/test/pkg1/WildcardTest.java"
  touch "${TEST_DIR}/project2/src/test/java/org/apache/ozone/test/pkg2/WildcardTest.java"
  
  result=$(find_project_paths_for_test_class "org.apache.ozone.test.pkg*.WildcardTest" 2>/dev/null)
  
  expected=$(echo -e "./project1\n./project2")
  [ "$result" = "$expected" ]
}

@test "test class with exact package match" {
  mkdir -p "${TEST_DIR}/project1/src/test/java/org/apache/ozone/test/exact"
  mkdir -p "${TEST_DIR}/project1/src/test/java/org/apache/ozone/test/exactmatch"
  touch "${TEST_DIR}/project1/src/test/java/org/apache/ozone/test/exact/ExactTest.java"
  touch "${TEST_DIR}/project1/src/test/java/org/apache/ozone/test/exactmatch/ExactTest.java"
  result=$(find_project_paths_for_test_class "org.apache.ozone.test.exact.ExactTest" 2>/dev/null)
  
  [ "$result" = "./project1" ]
}

@test "test class with trailing whitespace" {
  result=$(find_project_paths_for_test_class "TestClass1 " 2>/dev/null)
  
  [ "$result" = "./project1" ]
}

@test "test class project with trailing whitespace" {
  result=$(find_project_paths_for_test_class "apache.ozone.test.TestClass1 " 2>/dev/null)

  [ "$result" = "./project1" ]
}

@test "test class with leading whitespace" {
  result=$(find_project_paths_for_test_class " TestClass1" 2>/dev/null)

  [ "$result" = "./project1" ]
}

@test "test class with partial package and wildcard" {
  result=$(find_project_paths_for_test_class "apache.*.TestClass*" 2>/dev/null)

  expected=$(echo -e "./project1\n./project2")
  [ "$result" = "$expected" ]
}

# Test the build_maven_project_list function

@test "build maven project list with empty project paths" {
  result=$(build_maven_project_list "")
  
  [ "$result" = "" ]
}

@test "build maven project list with one project path" {
  result=$(build_maven_project_list "./project1")

  [ "$result" = "-pl ./project1" ]
}

@test "build maven project list with multiple project paths" {
  local project_paths=$(echo -e "./project1\n./project2")
  result=$(build_maven_project_list "$project_paths")

  [ "$result" = "-pl ./project1,./project2" ]
}
