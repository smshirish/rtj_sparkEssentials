package part1recap

import scala.math.Ordering.BooleanOrdering

object ScalaRecap extends App {
/**
Values and variables
 */
  val aBoolean : Boolean = true  //value defined only once and evavualted in definition ,not again
  var aBooleanVar :Boolean = false // evaluated on each call but not on def.

  /**
    * Expressions -> Expression is evaluated to a value
    */
  val anIfExpression = if(10>5) "bigger" else "smaller"

  /**
    * Instruction vs Expression
    *
    * Instructions (Imperative) are executed one by one
    * Expressions are evaluated to a value
    */
  val theUnit = println("Print something ") //returns Unit (void in java )

  /**
    * OOP
    */
  class Animal
  class Dog extends Animal
  trait Carnovire{
    def eat(animal:Animal):Unit
  }

  class Crocodile extends Animal with Carnovire{
    override def eat(animal: Animal): Unit = println("Crunch Crunch !!!")
  }


  /**
    * Functions
    * Scala functions implement traits Function1 to 22
    */

  val incrementer : Function1[Int, Int] = new Function[Int,Int] {
    override def apply(x: Int): Int = x + 1

  }
  // above trait is sugered with simple syntax as below
  val incrementer2 : Int => Int = new (Int => Int ) {
    override def apply(x: Int): Int = x + 1

  }

  val incrementer3 : Int => Int = x => x + 1

  /**
    * Scala pattern matching
    */

  //pattern matching to deconstruct values
  val unknown :Any = 45
  val ordinalDescription = unknown match {
    case 1 =>  "One"
    case 2 =>  "Two"
    case _ =>  "Unknown"
  }

  //pattern matching for exception handling
  try{
    throw new NullPointerException("XXX")
  }catch{
    case e: NullPointerException => "Got NullPointerException "
    case _: "Unknown Exception type"
  }

  /**
    * Partial functions
    */

  val aPartialFunctionExample = (x: Int ) => x match{
    case 1 => 43
    case 2 => 56
    case _ => 999
  }

  //short form partial function  , partial funtion taking int returning int  aPartialFunctionExample : PartialFunction[Int,Int]
  val aPartialFunctionExample:PartialFunction[Int,Int]  = {
    case 1 => 43
    case 2 => 56
    case _ => 999
  }

  /**
    * Implicits ->
    */

  //implicits- Auto injection by compiler
  def methodWithImplicitArgument( x: Int ): Int =  x + 43

  implicit val implicitVal: Int = 67

  methodWithImplicitArgument  //67 passed implicitely by compiler

  /**
    * How are implicit parameters serched
    * LocalScope
    * Imported scope
    * Companion objects of the types involved in method call
    */

  //implicit conversions - Implicit defs
  case class Person(name:String){
    def greet = {
      println(s"My name is $name")
    }
  }

  implicit def stringToPerson (name:String) = Person(name)

  "bob".greet  // compiler searches for String implicits and converts string to Person using the method def and greet method is available

  //implicits conversion - Implicit classes

  implicit class  Dog2(name:String){
    def bark = println("Bark ")
  }

  "bon".bark  //same as abobe implicit def , Iplicit classes are preferred to implicit defs.


}
