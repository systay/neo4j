/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_0.symbols

import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite

class TypeSpecTest extends CypherFunSuite {

  test("all types should contain all") {
    TypeSpec.all contains CTAny should equal(true)
    TypeSpec.all contains CTString should equal(true)
    TypeSpec.all contains CTNumber should equal(true)
    TypeSpec.all contains CTInteger should equal(true)
    TypeSpec.all contains CTFloat should equal(true)
    TypeSpec.all contains CTNode should equal(true)
    TypeSpec.all contains CTList(CTAny) should equal(true)
    TypeSpec.all contains CTList(CTFloat) should equal(true)
    TypeSpec.all contains CTList(CTList(CTFloat)) should equal(true)
  }

  test("should return true if contains") {
    CTNumber.covariant contains CTInteger should equal(true)
    CTNumber.covariant contains CTString should equal(false)

    val anyCollection = CTList(CTAny).covariant
    anyCollection contains CTList(CTString) should equal(true)
    anyCollection contains CTList(CTInteger) should equal(true)
    anyCollection contains CTList(CTAny) should equal(true)
    anyCollection contains CTList(CTList(CTInteger)) should equal(true)
    anyCollection contains CTBoolean should equal(false)
    anyCollection contains CTAny should equal(false)
  }

  test("should return true if contains CTAny") {
    TypeSpec.all containsAny TypeSpec.all should equal(true)
    TypeSpec.all containsAny CTNumber.covariant should equal(true)
    TypeSpec.all containsAny CTNode.invariant should equal(true)
    CTNumber.covariant containsAny TypeSpec.all should equal(true)

    CTNumber.covariant containsAny CTInteger should equal(true)
    CTInteger.covariant containsAny (CTInteger | CTString) should equal(true)
    CTInteger.covariant containsAny CTNumber.covariant should equal(true)
    CTInteger.covariant containsAny TypeSpec.all should equal(true)

    CTInteger.covariant containsAny CTString should equal(false)
    CTNumber.covariant containsAny CTString should equal(false)
  }

  test("should union") {
    CTNumber.covariant | CTString.covariant should equal(CTNumber | CTFloat | CTInteger | CTString)
    CTNumber.covariant | CTBoolean should equal(CTNumber | CTFloat | CTInteger | CTBoolean)

    CTNumber.covariant union CTList(CTString).covariant should equal(CTNumber | CTFloat | CTInteger | CTList(CTString))
    CTList(CTNumber) union CTList(CTString).covariant should equal(CTList(CTNumber) | CTList(CTString))
  }

  test("should intersect") {
    TypeSpec.all & CTInteger should equal(CTInteger.invariant)
    CTNumber.covariant & CTInteger should equal(CTInteger.invariant)
    CTNumber.covariant & CTString should equal(TypeSpec.none)

    (CTNumber | CTInteger) & (CTAny | CTNumber) should equal(CTNumber.invariant)
    CTNumber.contravariant & CTNumber.covariant should equal(CTNumber.invariant)
    (CTNumber | CTInteger) & (CTNumber | CTFloat) should equal(CTNumber.invariant)

    CTList(CTList(CTAny)).contravariant intersect CTList(CTAny).covariant should equal(CTList(CTAny) | CTList(CTList(CTAny)))

    (CTNumber.covariant | CTList(CTAny).covariant) intersect (CTNumber.covariant | CTString.covariant) should equal(CTNumber.covariant)
  }

  test("should constrain") {
    CTInteger.covariant should equal(CTInteger.invariant)
    CTNumber.covariant should equal(CTNumber | CTFloat | CTInteger)

    CTInteger constrain CTNumber should equal(CTInteger.invariant)
    CTNumber.covariant constrain CTInteger should equal(CTInteger.invariant)
    CTNumber constrain CTInteger should equal(TypeSpec.none)

    (CTInteger | CTString | CTMap) constrain CTNumber should equal(CTInteger.invariant)
    (CTInteger | CTList(CTString)) constrain CTList(CTAny) should equal(CTList(CTString).invariant)
    (CTInteger | CTList(CTMap)) constrain CTList(CTNode) should equal(TypeSpec.none)
    (CTInteger | CTList(CTString)) constrain CTAny should equal(CTInteger | CTList(CTString))
  }

  test("constrain to branch type within list contains") {
    TypeSpec.all constrain CTList(CTNumber) should equal(CTList(CTNumber) | CTList(CTInteger) | CTList(CTFloat))
  }

  test("constrain to sub type within list") {
    CTList(CTAny).covariant constrain CTList(CTString) should equal(CTList(CTString).invariant)
  }

  test("constrain to another branch") {
    CTNumber.covariant constrain CTString should equal(TypeSpec.none)

    CTString.covariant constrain CTNumber should equal(TypeSpec.none)
  }

  test("union two branches") {
    CTNumber.covariant | CTString.covariant should equal(CTNumber | CTInteger | CTFloat | CTString)
  }

  test("constrain to super type of some") {
    (CTInteger | CTString) constrain CTNumber should equal(CTInteger.invariant)
    CTInteger.contravariant constrain CTNumber should equal(CTNumber | CTInteger)
    CTNumber.contravariant constrain CTNumber should equal(CTNumber.invariant)
    CTNumber.contravariant constrain CTAny should equal(CTAny | CTNumber)
  }

  test("constrain to CTAny") {
    TypeSpec.all constrain CTAny should equal(TypeSpec.all)
    CTNumber.covariant constrain CTAny should equal(CTNumber.covariant)
  }

  test("constrain to sub type of some") {
    val constrainedToNumberOrCollectionT = CTNumber.covariant | CTList(CTAny).covariant
    constrainedToNumberOrCollectionT constrain CTList(CTNumber) should equal(CTList(CTNumber) | CTList(CTFloat) | CTList(CTInteger))
  }

  test("constrain to super type of none") {
    CTNumber.contravariant constrain CTInteger should equal(TypeSpec.none)
    CTNumber.contravariant constrain CTString should equal(TypeSpec.none)
  }

  test("should find leastUpperBounds of TypeSpecs") {
    (CTNode | CTNumber) leastUpperBounds (CTNode | CTNumber) should equal(CTNode | CTNumber | CTAny)

    (CTNode | CTNumber) leastUpperBounds (CTNode | CTNumber) should equal(CTNode | CTNumber | CTAny)
    (CTNode | CTNumber) leastUpperBounds CTNumber should equal(CTNumber | CTAny)
    (CTNode | CTNumber) leastUpperBounds (CTNode | CTNumber | CTRelationship) should equal(CTNode | CTNumber | CTMap | CTAny)
    (CTNode | CTNumber) leastUpperBounds CTAny should equal(CTAny.invariant)
    CTAny leastUpperBounds (CTNode | CTNumber) should equal(CTAny.invariant)

    CTRelationship.invariant leastUpperBounds CTNode.invariant should equal(CTMap.invariant)
    (CTRelationship | CTInteger) leastUpperBounds (CTNode | CTNumber) should equal(CTMap | CTNumber | CTAny)

    (CTInteger | CTList(CTString)) leastUpperBounds (CTNumber | CTList(CTInteger)) should equal(CTNumber | CTList(CTAny) | CTAny)
  }

  test("leastUpperBounds to root type") {
    TypeSpec.all leastUpperBounds CTAny should equal(CTAny.invariant)
    CTAny leastUpperBounds TypeSpec.all should equal(CTAny.invariant)
    CTList(CTAny).covariant leastUpperBounds CTAny should equal(CTAny.invariant)
    CTAny leastUpperBounds CTList(CTAny).covariant should equal(CTAny.invariant)
  }

  test("leastUpperBounds with leaf type") {
    TypeSpec.all leastUpperBounds CTInteger should equal(CTAny | CTNumber | CTInteger)
  }

  test("leastUpperBounds with list") {
    TypeSpec.all leastUpperBounds CTList(CTAny) should equal(CTAny | CTList(CTAny))
    TypeSpec.all leastUpperBounds CTList(CTString) should equal(CTAny | CTList(CTAny) | CTList(CTString))
  }

  test("leastUpperBounds with multiple types") {
    TypeSpec.all leastUpperBounds (CTInteger | CTString) should equal(CTAny | CTNumber | CTInteger | CTString)
    TypeSpec.all leastUpperBounds (CTList(CTString) | CTInteger) should equal(CTAny | CTNumber | CTInteger | CTList(CTAny) | CTList(CTString))
  }

  test("leastUpperBounds with some super types") {
    (CTList(CTString) | CTInteger) leastUpperBounds CTNumber should equal(CTAny | CTNumber)
    (CTList(CTString) | CTInteger) leastUpperBounds CTList(CTAny) should equal(CTAny | CTList(CTAny))

    CTInteger leastUpperBounds CTNumber.covariant should equal(CTNumber | CTInteger)

    val mergedSet = CTList(CTList(CTAny)).covariant leastUpperBounds TypeSpec.all
    mergedSet.contains(CTList(CTList(CTString))) should equal(true)
    mergedSet.contains(CTList(CTList(CTInteger))) should equal(true)
    mergedSet.contains(CTList(CTList(CTNumber))) should equal(true)
    mergedSet.contains(CTList(CTList(CTAny))) should equal(true)
    mergedSet.contains(CTList(CTString)) should equal(false)
    mergedSet.contains(CTList(CTNumber)) should equal(false)
    mergedSet.contains(CTList(CTAny)) should equal(true)
    mergedSet.contains(CTString) should equal(false)
    mergedSet.contains(CTNumber) should equal(false)
    mergedSet.contains(CTAny) should equal(true)

    CTNumber.contravariant leastUpperBounds CTInteger.contravariant should equal(CTAny | CTNumber)
  }

  test("leastUpperBounds with some sub types") {
    (CTList(CTString) | CTNumber) leastUpperBounds CTInteger should equal(CTAny | CTNumber)
    CTNumber.covariant leastUpperBounds CTInteger should equal(CTInteger | CTNumber)

    val numberOrCollectionT = CTNumber.covariant | CTList(CTAny).covariant
    numberOrCollectionT leastUpperBounds CTInteger should equal(CTAny | CTNumber | CTInteger)
    numberOrCollectionT leastUpperBounds CTList(CTInteger) should equal(CTAny | CTList(CTAny) | CTList(CTNumber) | CTList(CTInteger))

    val listOfListOfAny = CTList(CTList(CTAny)).covariant
    (TypeSpec.all leastUpperBounds listOfListOfAny) contains CTList(CTList(CTString)) should equal(true)
    (TypeSpec.all leastUpperBounds listOfListOfAny) contains CTList(CTList(CTInteger)) should equal(true)
    (TypeSpec.all leastUpperBounds listOfListOfAny) contains CTList(CTList(CTNumber)) should equal(true)
    (TypeSpec.all leastUpperBounds listOfListOfAny) contains CTList(CTList(CTAny)) should equal(true)
    (TypeSpec.all leastUpperBounds listOfListOfAny) contains CTList(CTString) should equal(false)
    (TypeSpec.all leastUpperBounds listOfListOfAny) contains CTList(CTNumber) should equal(false)
    (TypeSpec.all leastUpperBounds listOfListOfAny) contains CTList(CTAny) should equal(true)
    (TypeSpec.all leastUpperBounds listOfListOfAny) contains CTString should equal(false)
    (TypeSpec.all leastUpperBounds listOfListOfAny) contains CTNumber should equal(false)
    (TypeSpec.all leastUpperBounds listOfListOfAny) contains CTAny should equal(true)
  }

  test("leastUpperBounds of branch with sub type") {
    CTNumber.covariant leastUpperBounds CTInteger should equal(CTNumber | CTInteger)
  }

  test("leastUpperBounds of branch to branch root") {
    CTNumber.covariant leastUpperBounds CTNumber should equal(CTNumber.invariant)
  }

  test("leastUpperBounds of branch to super type") {
    CTInteger.covariant leastUpperBounds CTNumber should equal(CTNumber.invariant)
  }

  test("leastUpperBounds of branch to unrelated type") {
    CTNumber.covariant leastUpperBounds CTString should equal(CTAny.invariant)
    CTInteger.covariant leastUpperBounds CTString should equal(CTAny.invariant)
  }

  test("leastUpperBounds with equivalent") {
    CTNumber.covariant leastUpperBounds (CTNumber | CTInteger | CTFloat) should equal(CTNumber.covariant)
  }

  test("should wrap in list") {
    (CTString | CTList(CTNumber)).wrapInList should equal(CTList(CTString) | CTList(CTList(CTNumber)))
    TypeSpec.all.wrapInList should equal(CTList(CTAny).covariant)
  }

  test("should identify coercions") {
    CTFloat.covariant.coercions should equal(TypeSpec.none)
    CTInteger.covariant.coercions should equal(CTFloat.invariant)
    (CTFloat | CTInteger).coercions should equal(CTFloat.invariant)
    CTList(CTAny).covariant.coercions should equal(CTBoolean.invariant)
    TypeSpec.exact(CTList(CTPath)).coercions should equal(CTBoolean.invariant)
    TypeSpec.all.coercions should equal(CTBoolean | CTFloat)
    CTList(CTAny).covariant.coercions should equal(CTBoolean.invariant)
  }

  test("should intersect with coercions") {
    TypeSpec.all intersectOrCoerce CTInteger should equal(CTInteger.invariant)
    CTInteger intersectOrCoerce CTFloat should equal(CTFloat.invariant)
    CTNumber intersectOrCoerce CTFloat should equal(TypeSpec.none)
    CTList(CTAny).covariant intersectOrCoerce CTBoolean should equal(CTBoolean.invariant)
    CTNumber.covariant intersectOrCoerce CTBoolean should equal(TypeSpec.none)
    CTList(CTAny).covariant intersectOrCoerce CTBoolean should equal(CTBoolean.invariant)
    CTInteger intersectOrCoerce CTString should equal(TypeSpec.none)
  }

  test("should constrain with coercions") {
    TypeSpec.all constrainOrCoerce CTInteger should equal(CTInteger.invariant)
    CTInteger constrainOrCoerce CTFloat should equal(CTFloat.invariant)
    CTNumber constrainOrCoerce CTFloat should equal(TypeSpec.none)
    CTList(CTAny).covariant constrainOrCoerce CTBoolean should equal(CTBoolean.invariant)
    CTNumber.covariant constrainOrCoerce CTBoolean should equal(TypeSpec.none)
    CTList(CTAny).covariant constrainOrCoerce CTBoolean should equal(CTBoolean.invariant)
    CTInteger constrainOrCoerce CTString should equal(TypeSpec.none)
  }

  test("equal TypeSpecs should equal") {
    CTString.invariant should equal(CTString.invariant)

    CTString.invariant should equal(CTString.covariant)
    CTString.covariant should equal(CTString.invariant)

    CTFloat | CTInteger | CTNumber should equal(CTNumber | CTInteger | CTFloat)
    CTNumber.covariant should equal(CTNumber | CTInteger | CTFloat)
    CTNumber | CTInteger | CTFloat should equal(CTNumber.covariant)

    CTNumber.covariant | CTInteger.covariant should equal(CTNumber.covariant)
    CTNumber.covariant | CTString.covariant should not equal(CTNumber.covariant)

    TypeSpec.all should equal(TypeSpec.all)
    CTAny.covariant should equal(TypeSpec.all)
    CTList(CTAny).covariant should equal(CTList(CTAny).covariant)
    CTList(CTAny).covariant should equal(CTList(CTAny).covariant)

    TypeSpec.all | CTString.covariant should equal(TypeSpec.all)
    TypeSpec.all | TypeSpec.all should equal(TypeSpec.all)
  }

  test("different TypeSpecs should not equal") {
    TypeSpec.all should not equal(CTNumber.covariant)
    CTNumber.covariant should not equal(TypeSpec.all)

    CTList(CTAny).covariant should not equal(CTNumber.covariant)
    CTNumber.covariant should not equal(CTList(CTAny).covariant)

    CTNumber.invariant should not equal(TypeSpec.all)
    TypeSpec.all should not equal(CTNumber.invariant)
  }

  test("should have indefinite size when allowing unconstrained any at any depth") {
    TypeSpec.all.hasDefiniteSize should equal(false)
    CTList(CTAny).covariant.hasDefiniteSize should equal(false)

    (CTString | CTNumber).hasDefiniteSize should equal(true)
    CTList(CTString).covariant.hasDefiniteSize should equal(true)

    CTList(CTList(CTAny)).covariant.hasDefiniteSize should equal(false)
    CTList(CTList(CTString)).covariant.hasDefiniteSize should equal(true)

    CTAny.contravariant.hasDefiniteSize should equal(true)
    (CTList(CTAny).covariant leastUpperBounds CTList(CTAny)).hasDefiniteSize should equal(true)
  }

  test("should be empty when no possibilities remain") {
    TypeSpec.all.isEmpty should equal(false)
    TypeSpec.none.isEmpty should equal(true)
    (CTNumber.contravariant intersect CTInteger).isEmpty should equal(true)
  }

  test("should format none") {
    TypeSpec.none.mkString("(", ", ", " or ", ")") should equal("()")
  }

  test("should format single type") {
    CTAny.invariant.mkString("(", ", ", " or ", ")") should equal("(Any)")
    CTNode.invariant.mkString("<", ", ", " and ", ">") should equal("<Node>")
  }

  test("should format two types") {
    (CTAny | CTNode).mkString("", ", ", " or ", "") should equal("Any or Node")
    (CTRelationship | CTNode).mkString("-", ", ", " or ", "-") should equal("-Node or Relationship-")
  }

  test("should format three types") {
    (CTRelationship | CTInteger | CTNode).mkString(", ") should equal("Integer, Node, Relationship")
    (CTRelationship | CTInteger | CTNode).mkString("(", ", ", ")") should equal("(Integer, Node, Relationship)")
    (CTRelationship | CTAny | CTNode).mkString("(", ", ", " or ", ")") should equal("(Any, Node or Relationship)")
    (CTRelationship | CTInteger | CTNode).mkString("[", ", ", " and ", "]") should equal("[Integer, Node and Relationship]")
  }

  test("should format to string for indefinite sized set") {
    TypeSpec.all.mkString(", ") should equal("T")
    CTList(CTAny).covariant.mkString(", ") should equal("Collection<T>")
    (CTList(CTAny).covariant | CTBoolean).mkString(", ") should equal("Boolean, Collection<T>")
    (CTList(CTList(CTAny)).covariant | CTBoolean | CTList(CTString)).mkString(", ") should equal("Boolean, Collection<String>, Collection<Collection<T>>")
  }

  test("should format to string for definite sized set") {
    CTAny.invariant.mkString(", ") should equal("Any")
    CTString.invariant.mkString(", ") should equal("String")
    CTNumber.covariant.mkString(", ") should equal("Float, Integer, Number")
    (CTNumber.covariant | CTBoolean.covariant).mkString(", ") should equal("Boolean, Float, Integer, Number")
    CTNumber.contravariant.mkString(", ") should equal("Any, Number")
    (CTBoolean.covariant | CTString.covariant | CTList(CTBoolean).covariant | CTList(CTString).covariant).mkString(", ") should equal("Boolean, String, Collection<Boolean>, Collection<String>")
    (CTBoolean.covariant | CTString.covariant | CTList(CTBoolean).covariant | CTList(CTList(CTString)).covariant).mkString(", ") should equal("Boolean, String, Collection<Boolean>, Collection<Collection<String>>")
    CTList(CTAny).contravariant.mkString(", ") should equal("Any, Collection<Any>")
  }

  test("should iterate over definite sized set") {
    CTString.invariant.iterator.toSeq should equal(Seq(CTString))
    CTNumber.covariant.iterator.toSeq should equal(Seq(CTFloat, CTInteger, CTNumber))
    (CTNumber.covariant | CTBoolean.covariant).iterator.toSeq should equal(Seq(CTBoolean, CTFloat, CTInteger, CTNumber))
    CTNumber.contravariant.iterator.toSeq should equal(Seq(CTAny, CTNumber))
    (CTBoolean | CTString | CTList(CTBoolean) | CTList(CTString)).iterator.toSeq should equal(Seq(CTBoolean, CTString, CTList(CTBoolean), CTList(CTString)))
    (CTBoolean | CTString | CTList(CTBoolean) | CTList(CTList(CTString))).iterator.toSeq should equal(Seq(CTBoolean, CTString, CTList(CTBoolean), CTList(CTList(CTString))))
  }
}
