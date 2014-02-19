/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.pipes

import java.net.URL
import org.neo4j.cypher.internal.commons.CypherFunSuite

class ToStreamTest extends CypherFunSuite {
  test("should open a local file with an absolute path") {
    val path = "/tmp/file.csv"
    val url = new URL(s"file://$path")
    val c = ToStream(url)

    c shouldBe 'isFile
    c.file.getPath should equal (path)
  }

  test("should open a local file with a relative path") {
    val path = "./file.csv"
    val url = new URL(s"file://$path")
    val c = ToStream(url)

    c shouldBe 'isFile
    c.file.getPath should equal (path)
  }

  test("should open a local file with a relative path 2") {
    val path = ".././.././file.csv"
    val url = new URL(s"file://$path")
    val c = ToStream(url)

    c shouldBe 'isFile
    c.file.getPath should equal (path)
  }


  test("should open a remote url") {
    val url = new URL("http://www.google.com")
    val c = ToStream(url)

    c should not be 'isFile
    an [IllegalStateException] should be thrownBy c.file
  }
}
