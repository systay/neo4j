/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.spi.v3_2.codegen

import java.lang.reflect.Modifier
import java.util
import java.util.stream.{DoubleStream, IntStream, LongStream}

import org.neo4j.codegen.CodeGeneratorOption._
import org.neo4j.codegen.Expression.{constant, invoke, newInstance}
import org.neo4j.codegen.MethodReference.constructorReference
import org.neo4j.codegen.TypeReference._
import org.neo4j.codegen.source.{SourceCode, SourceVisitor}
import org.neo4j.codegen.{CodeGenerator, Parameter, _}
import org.neo4j.cypher.internal.codegen.{PrimitiveNodeStream, PrimitiveRelationshipStream}
import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen._
import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.ir.expressions._
import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.spi.{CodeStructure, CodeStructureResult, MethodStructure}
import org.neo4j.cypher.internal.compiled_runtime.v3_2.executionplan.{GeneratedQuery, GeneratedQueryExecution}
import org.neo4j.cypher.internal.compiler.v3_2.executionplan._
import org.neo4j.cypher.internal.compiler.v3_2.helpers._
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.{Id, InternalPlanDescription}
import org.neo4j.cypher.internal.compiler.v3_2.planner.CantCompileQueryException
import org.neo4j.cypher.internal.compiler.v3_2.spi.{InternalResultVisitor, QueryContext}
import org.neo4j.cypher.internal.compiler.v3_2.{ExecutionMode, ResultRowImpl, TaskCloser}
import org.neo4j.cypher.internal.frontend.v3_2.symbols
import org.neo4j.kernel.api.ReadOperations
import org.neo4j.kernel.impl.core.NodeManager

object GeneratedQueryStructure extends CodeStructure[GeneratedQuery] {


  case class GeneratedQueryStructureResult(query: GeneratedQuery, source: Option[(String, String)])
    extends CodeStructureResult[GeneratedQuery]

  private def createGenerator(conf: CodeGenConfiguration, source: (Option[(String, String)]) => Unit) = {
    val mode = conf.mode match {
      case SourceCodeMode => SourceCode.SOURCECODE
      case ByteCodeMode => SourceCode.BYTECODE
    }
    val option = if (conf.saveSource) new SourceVisitor {
      override protected def visitSource(reference: TypeReference,
                                         sourceCode: CharSequence) = source(Some(reference.name(), sourceCode.toString))
    } else {
      source(None)
      BLANK_OPTION
    }

    try {
      CodeGenerator.generateCode(classOf[CodeStructure[_]].getClassLoader, mode, option)
    } catch {
      case e: Exception => throw new CantCompileQueryException(e.getMessage, e)
    }
  }

  class SourceSaver extends ((Option[(String, String)]) => Unit) {

    private var _source: Option[(String, String)] = None

    override def apply(v1: Option[(String, String)]) =  _source = v1

    def source: Option[(String, String)] = _source
  }

  override def generateQuery(className: String,
                             columns: Seq[String],
                             operatorIds: Map[String, Id],
                             conf: CodeGenConfiguration)
                            (methodStructure: MethodStructure[_] => Unit)
                            (implicit codeGenContext: CodeGenContext): GeneratedQueryStructureResult = {

    val sourceSaver = new SourceSaver
    val generator = createGenerator(conf, sourceSaver)
    val execution: ClassHandle = using(
      generator.generateClass(conf.packageName, className + "Execution", typeRef[GeneratedQueryExecution])) { clazz =>

      val fields: Fields = createFields(columns, clazz)
      setOperatorIds(clazz, operatorIds)
      addSimpleMethods(clazz, fields)
      addAccept(methodStructure, generator, clazz, fields, conf)
      clazz.handle()
    }
    val query = using(generator.generateClass(conf.packageName, className, typeRef[GeneratedQuery])) { clazz =>
      using(clazz.generateMethod(typeRef[GeneratedQueryExecution], "execute",
        param[TaskCloser]("closer"),
        param[QueryContext]("queryContext"),
        param[ExecutionMode]("executionMode"),
        param[Provider[InternalPlanDescription]]("description"),
        param[QueryExecutionTracer]("tracer"),
        param[util.Map[String, Object]]("params"))) { execute =>
        execute.returns(
          invoke(
            newInstance(execution),
            constructorReference(execution,
              typeRef[TaskCloser],
              typeRef[QueryContext],
              typeRef[ExecutionMode],
              typeRef[Provider[InternalPlanDescription]],
              typeRef[QueryExecutionTracer],
              typeRef[util.Map[String, Object]]),
            execute.load("closer"),
            execute.load("queryContext"),
            execute.load("executionMode"),
            execute.load("description"),
            execute.load("tracer"),
            execute.load("params")))
      }
      clazz.handle()
    }.newInstance().asInstanceOf[GeneratedQuery]
    val clazz: Class[_] = execution.loadClass()
    operatorIds.foreach {
      case (key, id) => setStaticField(clazz, key, id)
    }

    println(sourceSaver.source)
    GeneratedQueryStructureResult(query, sourceSaver.source)
  }

  private def addAccept(methodStructure: (MethodStructure[_]) => Unit,
                        generator: CodeGenerator,
                        clazz: ClassGenerator,
                        fields: Fields,
                        conf: CodeGenConfiguration)(implicit codeGenContext: CodeGenContext) = {
    using(clazz.generate(MethodDeclaration.method(typeRef[Unit], "accept",
      Parameter.param(parameterizedType(classOf[InternalResultVisitor[_]],
        typeParameter("E")), "visitor")).
      parameterizedWith("E", extending(typeRef[Exception])).
      throwsException(typeParameter("E")))) { (codeBlock: CodeBlock) =>
      val structure = new GeneratedMethodStructure(fields, codeBlock, new AuxGenerator(conf.packageName, generator), onClose =
        Seq((success: Boolean) => (block: CodeBlock) => {
          val target = Expression.get(block.self(), fields.closeable)
          val reference = method[Completable, Void]("completed")
          val constant1 = Expression.constant(success)
          val invoke1 = Expression.invoke(target, reference, constant1)
          block.expression(invoke1)
        }))
      codeBlock.assign(typeRef[ResultRowImpl], "row", Templates.newResultRow)
      methodStructure(structure)
      structure.finalizers.foreach(_ (true)(codeBlock))
    }
  }

  private def addSimpleMethods(clazz: ClassGenerator, fields: Fields) = {
    clazz.generate(Templates.constructor(clazz.handle()))
    Templates.getOrLoadReadOperations(clazz, fields)
    clazz.generate(Templates.setSuccessfulCloseable(clazz.handle()))
    clazz.generate(Templates.executionMode(clazz.handle()))
    clazz.generate(Templates.executionPlanDescription(clazz.handle()))
    clazz.generate(Templates.JAVA_COLUMNS)
  }

  private def setOperatorIds(clazz: ClassGenerator, operatorIds: Map[String, Id]) = {
    operatorIds.keys.foreach { opId =>
      clazz.staticField(typeRef[Id], opId)
    }
  }

  private def createFields(columns: Seq[String], clazz: ClassGenerator) = {
    clazz.staticField(typeRef[util.List[String]], "COLUMNS", Templates.asList[String](
      columns.map(key => constant(key))))

    Fields(
      closer = clazz.field(typeRef[TaskCloser], "closer"),
      ro = clazz.field(typeRef[ReadOperations], "ro"),
      entityAccessor = clazz.field(typeRef[NodeManager], "nodeManager"),
      executionMode = clazz.field(typeRef[ExecutionMode], "executionMode"),
      description = clazz.field(typeRef[Provider[InternalPlanDescription]], "description"),
      tracer = clazz.field(typeRef[QueryExecutionTracer], "tracer"),
      params = clazz.field(typeRef[util.Map[String, Object]], "params"),
      closeable = clazz.field(typeRef[Completable], "closeable"),
      queryContext = clazz.field(typeRef[QueryContext], "queryContext"))
  }

  def method[O <: AnyRef, R](name: String, params: TypeReference*)
                            (implicit owner: Manifest[O], returns: Manifest[R]): MethodReference =
    MethodReference.methodReference(typeReference(owner), typeReference(returns), name, Modifier.PUBLIC, params: _*)

  def staticField[O <: AnyRef, R](name: String)(implicit owner: Manifest[O], fieldType: Manifest[R]): FieldReference =
    FieldReference.staticField(typeReference(owner), typeReference(fieldType), name)

  def param[T <: AnyRef](name: String)(implicit manifest: Manifest[T]): Parameter =
    Parameter.param(typeReference(manifest), name)

  def typeRef[T](implicit manifest: Manifest[T]): TypeReference = typeReference(manifest)

  def typeReference(manifest: Manifest[_]): TypeReference = {
    val arguments = manifest.typeArguments
    val base = TypeReference.typeReference(manifest.runtimeClass)
    if (arguments.nonEmpty) {
      TypeReference.parameterizedType(base, arguments.map(typeReference): _*)
    } else {
      base
    }
  }

  def lowerType(cType: CodeGenType): TypeReference = cType match {
    case CodeGenType(symbols.CTNode, IntType) => typeRef[Long]
    case CodeGenType(symbols.CTRelationship, IntType) => typeRef[Long]
    case CodeGenType(symbols.CTInteger, IntType) => typeRef[Long]
    case CodeGenType(symbols.CTFloat, FloatType) => typeRef[Double]
    case CodeGenType(symbols.CTBoolean, BoolType) => typeRef[Boolean]
    //case CodeGenType(symbols.CTString, ReferenceType) => typeRef[String] // We do not (yet) care about non-primitive types
    case CodeGenType(symbols.ListType(symbols.CTNode), ListReferenceType(IntType)) => typeRef[PrimitiveNodeStream]
    case CodeGenType(symbols.ListType(symbols.CTRelationship), ListReferenceType(IntType)) => typeRef[PrimitiveRelationshipStream]
    case CodeGenType(symbols.ListType(_), ListReferenceType(IntType)) => typeRef[LongStream]
    case CodeGenType(symbols.ListType(_), ListReferenceType(FloatType)) => typeRef[DoubleStream]
    case CodeGenType(symbols.ListType(_), ListReferenceType(BoolType)) => typeRef[IntStream]
    //case CodeGenType(symbols.ListType(_), _) => typeRef[java.lang.Iterable[Object]] // We do not (yet) have a shared base class for List types
    case _ => typeRef[Object]
  }

  def nullValue(cType: CodeGenType) = cType match {
    case CodeGenType(symbols.CTNode, IntType) => constant(-1L)
    case CodeGenType(symbols.CTRelationship, IntType) => constant(-1L)
    case _ => constant(null)
  }
}