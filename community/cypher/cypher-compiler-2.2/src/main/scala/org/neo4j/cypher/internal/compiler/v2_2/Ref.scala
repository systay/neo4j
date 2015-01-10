package org.neo4j.cypher.internal.compiler.v2_2

object Ref {
  def apply[T <: AnyRef](v: T) = new Ref[T](v)
}

final class Ref[T <: AnyRef](val v: T) {
  if (v == null)
    throw new InternalException("Attempt to instantiate Ref(null)")

  override def toString = s"Ref($v)"

  override def hashCode = java.lang.System.identityHashCode(v)

  override def equals(that: Any) = that match {
    case other: Ref[_] => v eq other.v
    case _             => false
  }
}
