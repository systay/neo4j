/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v2_3.docgen

import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{AllNodesScan, IdName}
import org.neo4j.cypher.internal.frontend.v2_3.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.frontend.v2_3.perty.gen.DocHandlerTestSuite
import org.neo4j.cypher.internal.frontend.v2_3.perty.handler.SimpleDocHandler

class InternalDocGenTest extends DocHandlerTestSuite[Any] with AstConstructionTestSupport {
  val docGen =  plannerDocGen orElse AstDocHandler.docGen.lift[Any] orElse SimpleDocHandler.docGen

  case class Container(v: Any)

  test("should render logical plans inside containers") {
    val result = pprintToString(Container(AllNodesScan(IdName("a"), Set.empty)(null)))
    result should equal("Container(AllNodesScan(a, Set()))")
  }
}
