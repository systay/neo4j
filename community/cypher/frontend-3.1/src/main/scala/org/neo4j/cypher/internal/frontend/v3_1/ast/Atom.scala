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
package org.neo4j.cypher.internal.frontend.v3_1.ast

/**
  * Inspired by Daniel Spiewaks presentation "From CFG to EXE"
  * https://www.youtube.com/watch?v=uVEBikEMuRQ
  *
  * This is a way of having a field that behaves as if it was immutable - once a value has been read, it is guaranteed
  * to never change. It's possible to set the value multiple times, but as soon as it has been read, it is fixed.
  *
  * Atoms can also know how to get populated, so when a Atom-value is read, if no value has been set from the outside,
  * it can go out and produce a value through it's "populate" method.
  */
trait Atom[A] {
  def apply(): A = {
    isForced = true
    if (isSet) {
      value
    } else {
      populate()

      if (!isSet) {
        sys.error("Value not set")
      }

      value
    }
  }

  def update(a: A): Unit = {
    if (!isSet || !isForced) {
      isSet = true
      value = a
    }
  }

  private var isForced = false
  private var isSet = false
  private var value: A = _

  protected def populate(): Unit = {
    sys.error("Don't know how to self populate!")
  }

  def copyTo(other: Atom[A]): Unit = if (isSet) {
    other.update(value)
  }

  def copyToIfNotSet(other: Atom[A]): Unit = if(isSet && !other.isSet) {
    other.update(value)
  }
}

object Atom {
  def atom[A](f: => Unit): Atom[A] = new Atom[A] {
    override protected def populate(): Unit = {
      f
    }
  }

  def atom[A]: Atom[A] = new Atom[A] {}
}