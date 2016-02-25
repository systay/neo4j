/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package cypher.feature.parser

import java.util

import cucumber.api.DataTable

import scala.collection.JavaConverters._

object scalaResultsParser {

  private def parser = new ResultsParser
  private val listener = new CypherValuesCreator

  def apply(input: String): AnyRef = {
    parser.parse(input, listener)
  }
}

object parseFullTable extends (DataTable => util.List[util.Map[String, AnyRef]]) {

  override def apply(table: DataTable): util.List[util.Map[String, AnyRef]] = {
    val keys = table.topCells().asScala
    val cells = table.cells(1).asScala

    val output = new util.ArrayList[util.Map[String, AnyRef]]
    cells.zipWithIndex.foreach { case (list, index) =>
      val map = new util.HashMap[String, AnyRef]()
      list.asScala.foreach { value =>
        val parsed = scalaResultsParser(value)
        map.put(keys(index), parsed)
      }
      output.add(map)
    }
    output
  }
}
