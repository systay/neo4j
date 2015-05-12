/*
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
package org.neo4j.cypher.internal.compiler.v2_3.codegen.experiment

import org.neo4j.cypher.internal.compiler.v2_3.codegen.experiment.Expression._
import org.neo4j.cypher.internal.compiler.v2_3.codegen.experiment.Statement._

case class Class(name: Name, constructor: Constructor, fields: Seq[Field], methods: Seq[Method])
case class Method(name: Name, priv: Boolean, arguments: Seq[Field], block: Statement, returnType: Type)
case class Constructor(priv: Boolean, arguments: Seq[Field], code: Statement)
case class Field(finale: Boolean, name: Name, typ: Type)
case class Catch(variable: Name, typ: Type, code: Block)

trait Statement
trait Expression extends Statement

object Statement {
  type Name = String
  type Type = String

  case class Block(statements: Seq[Statement]) extends Statement
  case class VariableDeclaration(name: Name, mutable: Boolean, typ: Type, init: Expression) extends Statement
  case object NOP extends Statement
  case class If(predicate: Expression, then: Statement, elze: Statement) extends Statement
  case class Throw(exceptionType: Type, arguments: Seq[Expression]) extends Statement
  case class Super(arguments: Seq[Expression]) extends Statement
  case class SetField(name: Name, value: Expression) extends Statement
  case class Try(code: Block, _catch: Catch, _finally: Block)
}

object Expression {
  case class Variable(name: Name) extends Expression
  case class Equals(l: Expression, r: Expression) extends Expression
  case class LiteralInt(v: Int) extends Expression
  case class LiteralString(v: String) extends Expression
  case class VariableAssignment(variableName: Name, exp: Expression) extends Expression
  case class InstanceMethodCall(instance: Expression, method: String, arguments: Seq[Expression]) extends Expression
  case class StaticMethodCall(typ: Type, method: String, arguments: Seq[Expression]) extends Expression
  case class Not(expression: Expression) extends Expression
  case class New(typ: Type, arguments: Seq[Expression]) extends Expression
}


//object Test {
//
//  private val RESULT_ROW = "org.neo4j.cypher.internal.compiler.v2_3.codegen.ResultRowImpl"
//
//  val constructor = Constructor(priv = false,
//    arguments = Seq(
//      Field(finale = false, "closer", "org.neo4j.cypher.internal.compiler.v2_3.TaskCloser"),
//      Field(finale = false, "statement", "org.neo4j.kernel.api.Statement"),
//      Field(finale = false, "db", "org.neo4j.graphdb.GraphDatabaseService"),
//      Field(finale = false, "executionMode", "org.neo4j.cypher.internal.compiler.v2_3.ExecutionMode"),
//      Field(finale = false, "description", "org.neo4j.function.Supplier<org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription>"),
//      Field(finale = false, "tracer", "org.neo4j.cypher.internal.compiler.v2_3.codegen.QueryExecutionTracer"),
//      Field(finale = false, "params", "java.util.Map<String, Object>")
//    ),
//    code = Block(Seq(
//      Super(Seq(Variable("closer"))),
//      SetField("ro", InstanceMethodCall(Variable("statement"), "readOperations", Seq.empty)),
//      SetField("db", Variable("db")),
//      SetField("tracer", Variable("tracer")),
//      SetField("params", Variable("params")),
//      SetField("v4", LiteralInt(-1)),
//      SetField("javaColumns", StaticMethodCall("java.util.Arrays", "asList", Seq(LiteralString("n1"), LiteralString("n2"))))
//    )))
//
//  val fields = Seq(
//    Field(finale = true, "ro", "org.neo4j.kernel.api.ReadOperations"),
//    Field(finale = true, "db", "org.neo4j.graphdb.GraphDatabaseService"),
//    Field(finale = true, "params", "java.util.Map<String, Object>"),
//    Field(finale = true, "tracer", "org.neo4j.cypher.internal.compiler.v2_3.codegen.QueryExecutionTracer"),
//    Field(finale = true, "javaColumns", "java.util.List<String>"),
//    Field(finale = false, "v4", "int")
//  )
//
//
//  val methodBlock = Block(Seq(
//    VariableDeclaration("row", false, RESULT_ROW, New(RESULT_ROW, Seq.empty)),
//    If(
//      Equals(Variable("v4"), LiteralInt(-1)),
//      InstanceMethodCall(Variable("ro"), "relationshipTypeGetForName", Seq(LiteralString("KNOWS"))),
//      NOP
//    ),
//  Try()
//  ))
//
//  val accept = Method(name = "accept", priv = false, arguments = Seq(Field(finale = true, "visitor", "ResultVisitor<E>")), block = NOP, returnType = "void")
//
//  Class(
//    name = "APA",
//    constructor = constructor,
//    fields = fields,
//    methods = Seq(accept)
//  )
//}
