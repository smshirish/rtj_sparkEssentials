case class Person(name:String){
  def greet = {
    println(s"My name is $name")
  }
}

implicit def stringToPerson (name:String) = Person(name)

"bob".greet