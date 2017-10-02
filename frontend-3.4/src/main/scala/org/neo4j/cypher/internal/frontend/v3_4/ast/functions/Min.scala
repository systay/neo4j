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
package org.neo4j.cypher.internal.frontend.v3_4.ast.functions

import org.neo4j.cypher.internal.frontend.v3_4.{SemanticCheck, ast}
import org.neo4j.cypher.internal.frontend.v3_4.ast.AggregatingFunction
import org.neo4j.cypher.internal.frontend.v3_4.symbols._

case object Min extends AggregatingFunction {
  override def name = "min"

  override def semanticCheck(ctx: ast.Expression.SemanticContext,
                             invocation: ast.FunctionInvocation): SemanticCheck =
    checkArgs(invocation, 1) chain
      invocation.arguments.expectType(CTAny.covariant) chain
      invocation.specifyType(invocation.arguments.leastUpperBoundsOfTypes)
}