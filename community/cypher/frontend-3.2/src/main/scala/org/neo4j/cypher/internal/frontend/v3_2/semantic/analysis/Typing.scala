package org.neo4j.cypher.internal.frontend.v3_2.semantic.analysis

import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.symbols.{TypeSpec, _}

import scala.collection.mutable

//noinspection TypeCheckCanBeMatch
object Typing extends Phase[Unit] {
  override def initialValue: Unit = {}

  override protected def before(node: ASTNode, environment: Unit): Unit = {}

  override protected def after(node: ASTNode, environment: Unit): Unit = {

    if (node.isInstanceOf[Expression]) {
      node.asInstanceOf[Expression] match {
        case e if e.myType.hasValue =>
        // This expression is already typed. No need to do any more work

        case e: SignedDecimalIntegerLiteral =>
          e.myType.value = CTInteger.covariant

        case e: IKnowMyType =>
          e.myType.value = e.knownType

        case e@Add(lhs, rhs) =>
          val myType = (lhs.myType.value, rhs.myType.value) match {
            case (CTInteger.covariant, CTInteger.covariant) => CTInteger.covariant
          }
          e.myType.value = myType

        case v: Variable => ???
//          v.myType.value = v.myScope.value.getTypeOf(v)
      }
    }
    environment
  }
}

class VariableTypes {
  private val types = mutable.HashMap[String, TypeSpec]()

  def add(v: Variable, t: TypeSpec) = types + (v.name -> t)

  def typeOf(v: Variable): TypeSpec = types(v.name)
}

trait IKnowMyType {
  self: Expression =>

  def knownType: TypeSpec
}