/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.docgen

import org.junit.Test
import org.junit.Assert._

class SetTest extends DocumentingTestBase {
  def graphDescription = List("Andres KNOWS Peter")

  override val properties = Map(
    "Andres" -> Map("age" -> 36l),
    "Peter" -> Map("age" -> 34l)
  )

  def section = "Set"

  @Test def set_property_on_node() {
    testQuery(
      title = "Set a property",
      text = "To set a property on a node or relationship, use +SET+",
      queryText = "start n = node(%Andres%) set n.surname = 'Taylor' return n",
      returns = "The newly changes node is returned",
      assertions = (p) => assert(node("Andres").getProperty("surname") === "Taylor")
    )
  }

  @Test def set_property_to_null() {
    testQuery(
      title = "Remove a property",
      text = """Normally you remove a property by using delete, but it's sometimes handy to do
it using the +SET+ command. One example is if the property comes from a parameter.""",
      queryText = "start n = node(%Andres%) set n.name = null return n",
      returns = "The node is returned, and the name property is now missing.",
      assertions = (p) => assertFalse(node("Andres").hasProperty("name"))
    )
  }
}