package org.neo4j.cypher.internal.compiler.v3_2.bork

import org.neo4j.cypher.internal.compiler.v3_2.bork
import org.neo4j.cypher.internal.frontend.v3_2.{SemanticTable, ast}

object ExpressionConverter {
  def transform(in: ast.Expression)(implicit state: SemanticTable, pipeLine: Pipeline): bork.Expression =
    in match {
      case ast.GreaterThan(l, r) =>
        val lhs = transform(l)
        val rhs = transform(r)
        new bork.expressions.GreaterThan(lhs, rhs)

      case ast.Property(ast.Variable(id), propertyKey) if state.isNode(id) => ???
//        val propKeyToken = propertyKey.id.get.id
//        val slot = pipeLine.slots(IdName(id)).asInstanceOf[LongSlot]
//        new NodeProperty(slot.getOffset, propKeyToken)

      case p =>
        throw new NotImplementedError(p.toString)
    }
}
