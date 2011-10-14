package org.neo4j.cypher.parser

/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import org.neo4j.cypher.commands._
import scala.util.parsing.combinator._

trait StartClause extends JavaTokenParsers with Tokens {
  def start: Parser[Start] = ignoreCase("start") ~> rep1sep(startBit, ",") ^^ (x => Start(x: _*))

  def startBit = identity ~ "=" ~ lookup ^^ {
    case id ~ "=" ~ l => l(id)
  }

  def nodes = ignoreCase("node") ^^ (x => "node")

  def rels = (ignoreCase("relationship") | ignoreCase("rel")) ^^ (x => "rel")

  def lookup: Parser[(String) => StartItem] = (nodes | rels) ~ (parens(parameter) | ids | idxLookup | idxString) ^^ {
    case "node" ~ l => l match {
      case l: Value => (id: String) => NodeById(id, l)
      case x: (String, Value, Value) => (id: String) => NodeByIndex(id, x._1, x._2, x._3)
      case x: (String, Value) => (id: String) => NodeByIndexQuery(id, x._1, x._2)
    }

    case "rel" ~ l => l match {
      case l: Value => (id: String) => RelationshipById(id, l)
      case x: (String, Value, Value) => (id: String) => RelationshipByIndex(id, x._1, x._2, x._3)
      case x: (String, Value) => (id: String) => RelationshipByIndexQuery(id, x._1, x._2)
    }
  }

  def ids = parens(rep1sep(wholeNumber, ",")) ^^ (x => Literal(x.map(_.toLong)))

  def idxString: Parser[(String, Value)] = ":" ~> identity ~ parens(parameter|stringLit) ^^ {
    case id ~ valu => (id, valu)
  }

  def idxLookup: Parser[(String, Value, Value)] = ":" ~> identity ~ parens(idxQueries) ^^ {
    case a ~ b => (a, b._1, b._2)
  }

  def idxQueries: Parser[(Value, Value)] = idxQuery

  def idxQuery: Parser[(Value, Value)] = (id | parameter) ~ "=" ~ (parameter | stringLit) ^^ {
    case k ~ "=" ~ v => (k, v)
  }

  def id: Parser[Value] = identity ^^ (x => Literal(x))

  def stringLit: Parser[Value] = string ^^ (x => Literal(x))


  def andQuery: Parser[String] = idxQuery ~ ignoreCase("and") ~ idxQueries ^^ {
    case q ~ and ~ qs => q + " AND " + qs
  }

  def orQuery: Parser[String] = idxQuery ~ ignoreCase("or") ~ idxQueries ^^ {
    case q ~ or ~ qs => q + " OR " + qs
  }
}







