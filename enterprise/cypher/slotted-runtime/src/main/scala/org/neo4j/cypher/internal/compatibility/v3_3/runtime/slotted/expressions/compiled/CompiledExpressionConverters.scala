package org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.expressions.compiled

import org.neo4j.codegen
import org.neo4j.codegen._
import org.neo4j.codegen.source.{SourceCode, SourceVisitor}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert.ExpressionConverter
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.ProjectedPath
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.predicates.Predicate
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.{expressions => commands}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{QueryState, SeekArgs}
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.SeekableArgs
import org.neo4j.cypher.internal.frontend.v3_3.ast
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{Value, Values}

object CompiledExpressionConverters extends ExpressionConverter {

  import org.neo4j.cypher.internal.frontend.v3_3.helpers._

  override def toCommandExpression(expression: ast.Expression): commands.Expression = {
    val (generatorGen, source: SourceSaver) = createGenerator()

    val handle =
      using(generatorGen.generateClass("org.neo4j.codegen.test", "TestarLost", classOf[CompiledExpression])) { clazz =>
        val returnType = classOf[AnyValue]
        val p1 = Parameter.param(classOf[ExecutionContext], "row")
        val p2 = Parameter.param(classOf[QueryState], "state")

        using(clazz.generateMethod(returnType, "execute", p1, p2)) { (method: CodeBlock) =>
          method.returns(createExpression(expression, method))
        }

        clazz.handle()
      }

    val expressionClass = handle.loadClass()
    val inner = expressionClass.newInstance().asInstanceOf[CompiledExpression]
    println(source._source)
    CompiledWrapper(inner)
  }

  private def createExpression(expression: ast.Expression, method: CodeBlock): Expression = {
    val returnExpression = expression match {
      case ast.Null() =>
        nullValueE

      case ast.Parameter(key, typ) =>
        val stateE = method.load("state")
        val getParam = MethodReference.methodReference(
          classOf[QueryState],
          classOf[AnyValue],
          "getParam",
          classOf[String])

        codegen.Expression.invoke(stateE, getParam, Expression.constant(key))

      case ast.Equals(l, r) =>
        def loadIfNotNull(e: ast.Expression, name: String) = {
          val expression = createExpression(e, method)
          val variable = method.assign(TypeReference.typeReference(classOf[AnyValue]), name, expression)
          val leftIsNull = Expression.equal(Expression.load(variable), nullValueE)
          using(method.ifStatement(leftIsNull)) { ifBlock =>
            ifBlock.returns(nullValueE)
          }
          variable
        }

        val varL = loadIfNotNull(l, "l")
        val varR = loadIfNotNull(r, "r")
        val equals = MethodReference.methodReference(
          classOf[AnyValue],
          classOf[Boolean],
          "equals",
          classOf[Object])

        val equalsE = Expression.invoke(Expression.load(varL), equals, Expression.load(varR))
        val turnBooleanToValue = MethodReference.methodReference( //
          classOf[Values],
          classOf[Boolean],
          "booleanValue",
          classOf[Boolean])

        Expression.invoke(turnBooleanToValue, equalsE)
    }
    returnExpression
  }

  private def nullValueE = {
    val owner = TypeReference.typeReference(classOf[Values])
    val fieldType = TypeReference.typeReference(classOf[Value])
    val field = FieldReference.staticField(owner, fieldType, "NO_VALUE")
    codegen.Expression.getStatic(field)
  }

  private def createGenerator() = {
    val sourceSaver = new SourceSaver
    (CodeGenerator.generateCode(SourceCode.SOURCECODE, sourceSaver),sourceSaver)
  }

  class SourceSaver extends SourceVisitor {
    var _source: Seq[(String, String)] = Seq.empty

    override protected def visitSource(reference: TypeReference, sourceCode: CharSequence): Unit =
      _source = _source :+ (reference.name(), sourceCode.toString)

  }

  override def toCommandPredicate(in: ast.Expression): Predicate = ???

  override def toCommandPredicate(e: Option[ast.Expression]): Predicate = ???

  override def toCommandSeekArgs(seek: SeekableArgs): SeekArgs = ???

  override def toCommandProjectedPath(e: ast.PathExpression): ProjectedPath = ???
}

case class CompiledWrapper(inner: CompiledExpression) extends commands.Expression {

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue =
    inner.execute(ctx, state)

  override def rewrite(f: (commands.Expression) => commands.Expression) = this

  override def arguments = Seq.empty

  override def symbolTableDependencies = Set.empty
}