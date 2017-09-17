/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_4.semantics.functions

import org.neo4j.cypher.internal.apa.v3_4.symbols._

class AbsTest extends FunctionTestBase("abs") {

  test("shouldFailIfWrongArguments") {
    testInvalidApplication()("Insufficient parameters for function 'abs'")
    testInvalidApplication(CTFloat, CTFloat)("Too many parameters for function 'abs'")
  }

  test("shouldHandleAllSpecializations") {
    testValidTypes(CTInteger)(CTInteger)
    testValidTypes(CTFloat)(CTFloat)
  }

  test("shouldHandleCombinedSpecializations") {
    testValidTypes(CTFloat | CTInteger)(CTFloat | CTInteger)
  }

  test("shouldReturnErrorIfInvalidArgumentTypes") {
    testInvalidApplication(CTNode)("Type mismatch: expected Float or Integer but was Node")
  }
}
