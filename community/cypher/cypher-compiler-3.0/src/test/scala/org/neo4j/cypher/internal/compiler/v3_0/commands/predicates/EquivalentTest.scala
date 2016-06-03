package org.neo4j.cypher.internal.compiler.v3_0.commands.predicates

import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite
import org.scalacheck.{Arbitrary, Prop, Test, _}
import org.scalacheck.Gen._
import org.scalacheck.Prop._
import org.scalacheck.util.Buildable
import org.scalatest.prop.PropertyChecks

class EquivalentTest extends CypherFunSuite with PropertyChecks {

  implicit override val generatorDrivenConfig =
    PropertyCheckConfig(minSize = 100000, maxSize = 200000)

  val charGenerator = Gen.frequency(
    (0xD800 - Char.MinValue, Gen.choose[Char](Char.MinValue, 0xD800 - 1)),
    (Char.MaxValue - 0xDFFF, Gen.choose[Char](0xDFFF + 1, Char.MaxValue))
  )

  val stringGen = listOf(charGenerator) map (_.mkString)

  val values: Gen[Any] = frequency(
    1 -> (Gen.chooseNum(Byte.MinValue, Byte.MaxValue) map (_.asInstanceOf[Any])),
    1 -> (Gen.chooseNum(Double.MinValue, Double.MaxValue) map (_.asInstanceOf[Any])),
    1 -> (Gen.chooseNum(Float.MinValue, Float.MaxValue) map (_.asInstanceOf[Any])),
    1 -> (Gen.chooseNum(Int.MinValue, Int.MaxValue) map (_.asInstanceOf[Any])),
    1 -> (Gen.chooseNum(Long.MinValue, Long.MaxValue) map (_.asInstanceOf[Any])),
    1 -> (charGenerator map (_.asInstanceOf[Any])),
    1 -> (identifier map (_.asInstanceOf[Any]))
  )

  val twoRandomValues: Gen[(Any, Any)] = for {
    a <- values
    b <- values
  } yield (a, b)

  val twoEqualNumbers: Gen[(Any, Any)] = values.map(x => (x, x))

  val pairs: Gen[(Any, Any)] = frequency(
    1 -> twoEqualNumbers,
    10 -> twoRandomValues
  )

  def checkValue(x: (Any, Any)) = withClue(s"${x._1.getClass.getSimpleName}: ${x._1}\n${x._2.getClass.getSimpleName}: ${x._2}\n") {
    x match {
      case (n1: Number, n2: Number) =>
        withClue(s"${n1.getClass.toString} == ${n2.getClass.toString}") {
          val eq1 = Equivalent.tryBuild(n1).getOrElse(fail(s"failed to handle $n1"))
          val equals1 = n1.doubleValue().equals(n2.doubleValue())
          eq1.equals(n2) should equal(equals1)

          val eq2 = Equivalent.tryBuild(n2).getOrElse(fail(s"failed to handle $n2"))
          eq2.equals(n1) should equal(equals1)
        }
      case (c1: Char, c2: Char) =>
        val eq1 = Equivalent.tryBuild(c1).getOrElse(fail(s"failed to handle $c1"))
        val equals1 = c1 == c2
        eq1.equals(c2) should equal(equals1)
      case (a, b) =>
        val eq1 = Equivalent.tryBuild(a).getOrElse(fail(s"failed to handle $a"))
        eq1.equals(b) should equal(false)
    }
  }

  forAll(pairs)(checkValue)

  //  forAll { (n1: Number, n2: Number) => {
  //    val eq1 = Equivalent.tryBuild(n1).getOrElse(fail(s"failed to handle $n1"))
  //    withClue(s"${n1.getClass.toString} == ${n2.getClass.toString} ${eq1.equals(n2)}") {
  //      eq1.equals(n2) should equal(n1.doubleValue().equals(n2.doubleValue()))
  //    }
  //  }}

  //  forAll { (n1: String, n2: Char) => {
  //    val eq1 = Equivalent.tryBuild(n1).getOrElse(fail(s"failed to handle $n1"))
  //    withClue(s"${n1.getClass.toString} == ${n2.getClass.toString} ${eq1.equals(n2)}") {
  //      (eq1 == n2) should equal(n1 == n2.toString)
  //    }
  //  }}

  //  val apa = for(n <- Gen.)
  /*-------------------------------------------------------------------------*\
  **  ScalaCheck                                                             **
  **  Copyright (c) 2007-2015 Rickard Nilsson. All rights reserved.          **
  **  http://www.scalacheck.org                                              **
  **                                                                         **
  **  This software is released under the terms of the Revised BSD License.  **
  **  There is NO WARRANTY. See the file LICENSE for the full text.          **
  \*------------------------------------------------------------------------ */

  //  package org.scalacheck
  //
  //  import language.higherKinds
  //  import concurrent.Future
  //
  //  import util.{FreqMap, Buildable}
  //
  //
  //  sealed abstract class Arbitrary[T] {
  //    val arbitrary: Gen[T]
  //  }
  //
  //  /** Defines implicit [[org.scalacheck.Arbitrary]] instances for common types.
  //    *  <p>
  //    *  ScalaCheck
  //    *  uses implicit [[org.scalacheck.Arbitrary]] instances when creating properties
  //    *  out of functions with the `Prop.property` method, and when
  //    *  the `Arbitrary.arbitrary` method is used. For example, the
  //    *  following code requires that there exists an implicit
  //    *  `Arbitrary[MyClass]` instance:
  //    *  </p>
  //    *
  //    *  {{{
  //    *    val myProp = Prop.forAll { myClass: MyClass =>
  //    *      ...
  //    *    }
  //    *
  //    *    val myGen = Arbitrary.arbitrary[MyClass]
  //    *  }}}
  //    *
  //    *  <p>
  //    *  The required implicit definition could look like this:
  //    *  </p>
  //    *
  //    *  {{{
  //    *    implicit val arbMyClass: Arbitrary[MyClass] = Arbitrary(...)
  //    *  }}}
  //    *
  //    *  <p>
  //    *  The factory method `Arbitrary(...)` takes a generator of type
  //    *  `Gen[T]` and returns an instance of `Arbitrary[T]`.
  //    *  </p>
  //    *
  //    *  <p>
  //    *  The `Arbitrary` module defines implicit [[org.scalacheck.Arbitrary]]
  //    *  instances for common types, for convenient use in your properties and
  //    *  generators.
  //    *  </p>
  //    */
  //  object Arbitrary extends ArbitraryLowPriority with ArbitraryArities
  //
  //  /** separate trait to have same priority as ArbitraryArities */
  //  private[scalacheck] sealed trait ArbitraryLowPriority {
  //
  //    import Gen.{const, choose, sized, frequency, oneOf, buildableOf, resize}
  //    import collection.{immutable, mutable}
  //    import java.util.Date
  //
  //    /** Creates an Arbitrary instance */
  //    def apply[T](g: => Gen[T]): Arbitrary[T] = new Arbitrary[T] {
  //      lazy val arbitrary = g
  //    }
  //
  //    /** Returns an arbitrary generator for the type T. */
  //    def arbitrary[T](implicit a: Arbitrary[T]): Gen[T] = a.arbitrary
  //
  //    /** ** Arbitrary instances for each AnyVal ****/
  //
  //    /** Arbitrary AnyVal */
  //    implicit lazy val arbAnyVal: Arbitrary[AnyVal] = Arbitrary(oneOf(
  //      arbitrary[Unit], arbitrary[Boolean], arbitrary[Char], arbitrary[Byte],
  //      arbitrary[Short], arbitrary[Int], arbitrary[Long], arbitrary[Float],
  //      arbitrary[Double]
  //    ))
  //
  //    /** Arbitrary instance of Boolean */
  //    implicit lazy val arbBool: Arbitrary[Boolean] =
  //      Arbitrary(oneOf(true, false))
  //
  //    /** Arbitrary instance of Int */
  //    implicit lazy val arbInt: Arbitrary[Int] = Arbitrary(
  //      Gen.chooseNum(Int.MinValue, Int.MaxValue)
  //    )
  //
  //    /** Arbitrary instance of Long */
  //    implicit lazy val arbLong: Arbitrary[Long] = Arbitrary(
  //      Gen.chooseNum(Long.MinValue, Long.MaxValue)
  //    )
  //
  //    /** Arbitrary instance of Float */
  //    implicit lazy val arbFloat: Arbitrary[Float] = Arbitrary(
  //      for {
  //        s <- choose(0, 1)
  //        e <- choose(0, 0xfe)
  //        m <- choose(0, 0x7fffff)
  //      } yield java.lang.Float.intBitsToFloat((s << 31) | (e << 23) | m)
  //    )
  //
  //    /** Arbitrary instance of Double */
  //    implicit lazy val arbDouble: Arbitrary[Double] = Arbitrary(
  //      for {
  //        s <- choose(0L, 1L)
  //        e <- choose(0L, 0x7feL)
  //        m <- choose(0L, 0xfffffffffffffL)
  //      } yield java.lang.Double.longBitsToDouble((s << 63) | (e << 52) | m)
  //    )
  //
  //    /** Arbitrary instance of Char */
  //    implicit lazy val arbChar: Arbitrary[Char] = Arbitrary(
  //      Gen.frequency(
  //        (0xD800 - Char.MinValue, Gen.choose[Char](Char.MinValue, 0xD800 - 1)),
  //        (Char.MaxValue - 0xDFFF, Gen.choose[Char](0xDFFF + 1, Char.MaxValue))
  //      )
  //    )
  //
  //    /** Arbitrary instance of Byte */
  //    implicit lazy val arbByte: Arbitrary[Byte] = Arbitrary(
  //      Gen.chooseNum(Byte.MinValue, Byte.MaxValue)
  //    )
  //
  //    /** Arbitrary instance of Short */
  //    implicit lazy val arbShort: Arbitrary[Short] = Arbitrary(
  //      Gen.chooseNum(Short.MinValue, Short.MaxValue)
  //    )
  //
  //    /** Absolutely, totally, 100% arbitrarily chosen Unit. */
  //    implicit lazy val arbUnit: Arbitrary[Unit] = Arbitrary(const(()))
  //
  //    /** ** Arbitrary instances of other common types ****/
  //
  //    /** Arbitrary instance of String */
  //    implicit lazy val arbString: Arbitrary[String] =
  //      Arbitrary(arbitrary[List[Char]] map (_.mkString))
  //
  //    /** Arbitrary instance of Date */
  //    implicit lazy val arbDate: Arbitrary[Date] = Arbitrary(for {
  //      l <- arbitrary[Long]
  //      d = new Date
  //    } yield new Date(d.getTime + l))
  //
  //    /** Arbitrary instance of Throwable */
  //    implicit lazy val arbThrowable: Arbitrary[Throwable] =
  //      Arbitrary(oneOf(const(new Exception), const(new Error)))
  //
  //    /** Arbitrary instance of Exception */
  //    implicit lazy val arbException: Arbitrary[Exception] =
  //      Arbitrary(const(new Exception))
  //
  //    /** Arbitrary instance of Error */
  //    implicit lazy val arbError: Arbitrary[Error] =
  //      Arbitrary(const(new Error))
  //
  //    /** Arbitrary BigInt */
  //    implicit lazy val arbBigInt: Arbitrary[BigInt] = {
  //      def chooseBigInt: Gen[BigInt] =
  //        sized((s: Int) => choose(-s, s)) map (x => BigInt(x))
  //
  //      def chooseReallyBigInt: Gen[BigInt] = for {
  //        bi <- chooseBigInt
  //        n <- choose(32, 128)
  //      } yield bi << n
  //
  //      Arbitrary(
  //        frequency(
  //          (5, chooseBigInt),
  //          (10, chooseReallyBigInt),
  //          (1, BigInt(0)),
  //          (1, BigInt(1)),
  //          (1, BigInt(-1)),
  //          (1, BigInt(Int.MaxValue) + 1),
  //          (1, BigInt(Int.MinValue) - 1),
  //          (1, BigInt(Long.MaxValue)),
  //          (1, BigInt(Long.MinValue)),
  //          (1, BigInt(Long.MaxValue) + 1),
  //          (1, BigInt(Long.MinValue) - 1)
  //        )
  //      )
  //    }
  //
  //    /** Arbitrary BigDecimal */
  //    implicit lazy val arbBigDecimal: Arbitrary[BigDecimal] = {
  //      import java.math.MathContext._
  //      val mcGen = oneOf(UNLIMITED, DECIMAL32, DECIMAL64, DECIMAL128)
  //      val bdGen = for {
  //        x <- arbBigInt.arbitrary
  //        mc <- mcGen
  //        limit <- const(if (mc == UNLIMITED) 0 else math.max(x.abs.toString.length - mc.getPrecision, 0))
  //        scale <- Gen.chooseNum(Int.MinValue + limit, Int.MaxValue)
  //      } yield {
  //        try {
  //          BigDecimal(x, scale, mc)
  //        } catch {
  //          case ae: java.lang.ArithmeticException => BigDecimal(x, scale, UNLIMITED) // Handle the case where scale/precision conflict
  //        }
  //      }
  //      Arbitrary(bdGen)
  //    }
  //
  //    /** Arbitrary java.lang.Number */
  //    implicit lazy val arbNumber: Arbitrary[Number] = {
  //      val gen = Gen.oneOf(
  //        arbitrary[Byte], arbitrary[Short], arbitrary[Int], arbitrary[Long],
  //        arbitrary[Float], arbitrary[Double]
  //      )
  //      Arbitrary(gen map (_.asInstanceOf[Number]))
  //      // XXX TODO - restore BigInt and BigDecimal
  //      // Arbitrary(oneOf(arbBigInt.arbitrary :: (arbs map (_.arbitrary) map toNumber) : _*))
  //    }

}

