/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.ast

import org.neo4j.cypher.internal.commons.CypherFunSuite

import java.util.regex.Pattern.quote

class ConvertLikeToRegexTest extends CypherFunSuite {

  val tests = Seq(
    "%" -> ".*"
    ,
    "_" -> "."
    ,
    "A%" -> s"${quote("A")}.*"
    ,
    "A_B" -> s"${quote("A")}.${quote("B")}"
    ,
    ".*" -> quote(".*")
    ,
    "[Ab]" -> "[Ab]"
  )

  tests.foreach {
    case (like, regex) => test(s"$like -> $regex") {
      convertLikeToRegex(LikeParser(like)) should equal(regex)
    }

  }
}
