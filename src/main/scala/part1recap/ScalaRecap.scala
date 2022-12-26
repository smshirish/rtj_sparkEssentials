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


}
