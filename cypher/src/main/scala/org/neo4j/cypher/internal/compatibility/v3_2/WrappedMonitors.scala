package org.neo4j.cypher.internal.compatibility.v3_2

import org.neo4j.cypher.internal.compiler.v3_2.Monitors
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}

import scala.reflect.ClassTag

case class WrappedMonitors(kernelMonitors: KernelMonitors) extends Monitors {
  def addMonitorListener[T](monitor: T, tags: String*) {
    kernelMonitors.addMonitorListener(monitor, tags: _*)
  }

  def newMonitor[T <: AnyRef : ClassTag](tags: String*): T = {
    val clazz = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    kernelMonitors.newMonitor(clazz, tags: _*)
  }
}
