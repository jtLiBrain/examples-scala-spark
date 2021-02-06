import org.scalatest._

class ScalaExampleTest extends FlatSpec with Matchers {
  "ScalaExample" should "sum two numbers" in {
    ScalaExample.sum(1, 5) shouldBe 6
  }
}
