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
package org.neo4j.cypher.internal.compiler.v2_3.codegen.ir

import org.neo4j.cypher.internal.compiler.v2_3.codegen.JavaUtils.JavaTypes.{DOUBLE, LIST, LONG, MAP, NUMBER, OBJECT, STRING}
import org.neo4j.cypher.internal.compiler.v2_3.codegen.JavaUtils.{JavaString, JavaSymbol}
import org.neo4j.cypher.internal.compiler.v2_3.codegen.experiment._
import org.neo4j.cypher.internal.compiler.v2_3.codegen.{CodeGenerator, KernelExceptionCodeGen, Namer}

sealed trait ProjectionInstruction extends Instruction {
  def projectedVariable: JavaSymbol
  def generateCode() = ""
  override protected def _importedClasses() = Set.empty
}

object ProjectionInstruction {
  def literal(value: Long): ProjectionInstruction = ProjectLiteral(JavaSymbol(value.toString + "L", LONG))
  def literal(value: Double): ProjectionInstruction = ProjectLiteral(JavaSymbol(value.toString, DOUBLE))
  def literal(value: String): ProjectionInstruction = ProjectLiteral(JavaSymbol( s""""$value"""", STRING))
  def literal(value: Boolean): ProjectionInstruction = ???
  def parameter(key: String): ProjectionInstruction = ProjectParameter(key)

  def add(lhs: ProjectionInstruction, rhs: ProjectionInstruction): ProjectionInstruction = ProjectAddition(lhs, rhs)
  def sub(lhs: ProjectionInstruction, rhs: ProjectionInstruction): ProjectionInstruction = ProjectSubtraction(lhs, rhs)
}

case class ProjectNodeProperty(id: String, token: Option[Int], propName: String, nodeIdVar: String, namer: Namer) extends ProjectionInstruction {
  private val propKeyVar = token.map(_.toString).getOrElse(namer.newVarName())
  private val methodName = namer.newMethodName()

  def generateInit() = if (token.isEmpty)
      s"""if ( $propKeyVar == -1 )
         |{
         |$propKeyVar = ro.propertyKeyGetForName( "$propName" );
         |}
       """.stripMargin
      else ""

  override def generateInitStatement(): Statement = if (token.isEmpty)
    If(
      Equals(Variable(propKeyVar), LiteralInt(-1)),
      VariableAssignment(propKeyVar, InstanceMethodCall(Variable("ro"), "propertyKeyGetForName", Seq(LiteralString(propName)))),
      NOP)
  else
    NOP

  override def _importedClasses() =
    Set("org.neo4j.kernel.api.properties.Property", "org.neo4j.kernel.api.exceptions.EntityNotFoundException")

  override def members() = if (token.isEmpty) s"private int $propKeyVar = -1;" else ""

  def projectedVariable = JavaSymbol(s"$methodName( $nodeIdVar )", OBJECT)

  override protected def _exceptions() = Set(KernelExceptionCodeGen)

  override def operatorId = Some(id)

  override protected def _method: Option[Method] = Some(new Method() {
    override def name: String = methodName

    override def generateCode: String = {
      val eventVar = "event_" + id
      s"""
        |private Object $methodName( long nodeId ) throws EntityNotFoundException
        |{
        |try ( QueryExecutionEvent $eventVar = tracer.executeOperator( $id ) )
        |{
        |$eventVar.dbHit();
        |$eventVar.row();
        |return ro.nodeGetProperty( nodeId, $propKeyVar ).value( null );
        |}
        |}
      """.stripMargin
    }
  })
}

case class ProjectRelProperty(token: Option[Int], propName: String, relIdVar: String, namer: Namer) extends ProjectionInstruction {
  private val propKeyVar = token.map(_.toString).getOrElse(namer.newVarName())

  def generateInit() =
    if (token.isEmpty)
      s"""if ( $propKeyVar == -1 )
         |{
         |$propKeyVar = ro.propertyKeyGetForName( "$propName" );
         |}""".stripMargin
    else ""

  override def generateInitStatement(): Statement = if (token.isEmpty)
    If(
      Equals(Variable(propKeyVar), LiteralInt(-1)),
      VariableAssignment(propKeyVar, InstanceMethodCall(Variable("ro"), "propertyKeyGetForName", Seq(LiteralString(propName)))),
      NOP)
  else
    NOP


  override def _importedClasses() =
    Set("org.neo4j.kernel.api.properties.Property")

  override def members() = if (token.isEmpty) s"private int $propKeyVar = -1;" else ""

  def projectedVariable = JavaSymbol(s"ro.relationshipGetProperty( $relIdVar, $propKeyVar ).value( null )", OBJECT)

  override protected def _exceptions() = Set(KernelExceptionCodeGen)
}

case class ProjectParameter(key: String) extends ProjectionInstruction {

  def generateInit() =
    s"""if( !params.containsKey( "${key.toJava}" ) )
      |{
      |throw new ParameterNotFoundException( "Expected a parameter named ${key.toJava}" );
      |}
    """.stripMargin


  override def generateInitStatement(): Statement =
    If(
      Not(InstanceMethodCall(Variable("params"), "containsKey", Seq(LiteralString(key.toJava)))),
      Throw("org.neo4j.cypher.internal.compiler.v2_3.ParameterNotFoundException", Seq(LiteralString(s"Expected a parameter named ${key.toJava}"))),
      NOP
    )

  def projectedVariable = JavaSymbol(s"""params.get( "${key.toJava}" )""", OBJECT)

  def members() = ""

  override def _importedClasses(): Set[String] = Set("org.neo4j.cypher.internal.compiler.v2_3.ParameterNotFoundException")
}

trait NeedsNoInit {
  self: Instruction =>
  override def generateInit() = ""

  override def members() = ""

  override def generateInitStatement(): Statement = NOP
}

case class ProjectLiteral(projectedVariable: JavaSymbol) extends ProjectionInstruction with NeedsNoInit

case class ProjectNode(nodeIdVar: JavaSymbol) extends ProjectionInstruction with NeedsNoInit {

  def projectedVariable = nodeIdVar.withProjectedSymbol(CodeGenerator.getNodeById(nodeIdVar.name))
}

case class ProjectRelationship(relId: JavaSymbol) extends ProjectionInstruction with NeedsNoInit {

  def projectedVariable = relId.withProjectedSymbol(CodeGenerator.getRelationshipById(relId.name))
}

case class ProjectAddition(lhs: ProjectionInstruction, rhs: ProjectionInstruction)
  extends ProjectionInstruction with NeedsNoInit {

  def projectedVariable: JavaSymbol = {
    val leftTerm = lhs.projectedVariable
    val rightTerm = rhs.projectedVariable
    (leftTerm.javaType, rightTerm.javaType) match {
      case (LONG, LONG) => JavaSymbol(s"${leftTerm.name} + ${rightTerm.name}", LONG)
      case (LONG, DOUBLE) => JavaSymbol(s"${leftTerm.name} + ${rightTerm.name}", DOUBLE)
      case (LONG, STRING) => JavaSymbol(s"${leftTerm.name} + ${rightTerm.name}", STRING)

      case (DOUBLE, DOUBLE) => JavaSymbol(s"${leftTerm.name} + ${rightTerm.name}", DOUBLE)
      case (DOUBLE, LONG) => JavaSymbol(s"${leftTerm.name} + ${rightTerm.name}", DOUBLE)
      case (DOUBLE, STRING) => JavaSymbol(s"${leftTerm.name} + ${rightTerm.name}", STRING)

      case (STRING, STRING) => JavaSymbol(s"${leftTerm.name} + ${rightTerm.name}", STRING)
      case (STRING, LONG) => JavaSymbol(s"${leftTerm.name} + ${rightTerm.name}", STRING)
      case (STRING, DOUBLE) => JavaSymbol(s"${leftTerm.name} + ${rightTerm.name}", STRING)

      case (_, _) => JavaSymbol(s"CompiledMathHelper.add( ${leftTerm.name}, ${rightTerm.name} )", OBJECT)
    }
  }

  override def children = Seq(lhs, rhs)

  override def _importedClasses(): Set[String] = Set("org.neo4j.cypher.internal.compiler.v2_3.codegen.CompiledMathHelper")
}

case class ProjectSubtraction(lhs: ProjectionInstruction, rhs: ProjectionInstruction)
  extends ProjectionInstruction with NeedsNoInit {

  def projectedVariable: JavaSymbol = {
    val leftTerm = lhs.projectedVariable
    val rightTerm = rhs.projectedVariable
    (leftTerm.javaType, rightTerm.javaType) match {
      case (LONG, LONG) => JavaSymbol(s"${leftTerm.name} - ${rightTerm.name}", LONG)
      case (LONG, DOUBLE) => JavaSymbol(s"${leftTerm.name} - ${rightTerm.name}", DOUBLE)

      case (DOUBLE, DOUBLE) => JavaSymbol(s"${leftTerm.name} - ${rightTerm.name}", DOUBLE)
      case (DOUBLE, LONG) => JavaSymbol(s"${leftTerm.name} - ${rightTerm.name}", DOUBLE)

      case (_, _) => JavaSymbol(s"CompiledMathHelper.subtract( ${leftTerm.name}, ${rightTerm.name} )", NUMBER)
    }
  }

  override def children = Seq(lhs, rhs)

  override def _importedClasses(): Set[String] = Set("org.neo4j.cypher.internal.compiler.v2_3.codegen.CompiledMathHelper")
}

case class ProjectCollection(instructions: Seq[ProjectionInstruction])
  extends ProjectionInstruction with NeedsNoInit {

  override def children: Seq[Instruction] = instructions

  def projectedVariable = asJavaList(_.projectedVariable.name)
    .withProjectedSymbol(asJavaList(_.projectedVariable.materialize.name))

  override def _importedClasses() = Set("java.util.Arrays")

  private def asJavaList(f: ProjectionInstruction => String) =
    JavaSymbol(instructions.map(f).mkString("Arrays.asList(", ",", ")"), LIST)
}

case class ProjectMap(instructions: Map[String, ProjectionInstruction]) extends ProjectionInstruction with NeedsNoInit {

  override def children: Seq[Instruction] = instructions.values.toSeq

  def projectedVariable = asJavaMap(_.projectedVariable.name)
    .withProjectedSymbol(asJavaMap(_.projectedVariable.materialize.name))

  private def asJavaMap(f: ProjectionInstruction => String) = JavaSymbol(
    instructions.map {
      case (key, value) => s""""$key", ${f(value)}"""
    }.mkString("org.neo4j.helpers.collection.MapUtil.map(", ",", ")"), MAP)
}

case class Project(projections: Seq[ProjectionInstruction], parent: Instruction) extends Instruction {
  override def generateCode() = generate(_.generateCode())

  override protected def children = projections :+ parent

  override def members() = generate(_.members())

  override def generateInit() = generate(_.generateInit())

  override def generateInitStatement(): Statement = ???

  private def generate(f: Instruction => String) = projections.
                                                   map(f).
                                                   mkString("", CodeGenerator.n, CodeGenerator.n) + f(parent)

  override protected def _importedClasses(): Set[String] = Set.empty
}
