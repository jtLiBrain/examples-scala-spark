import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class ScalaExampleSuite extends AnyFlatSpec with should.Matchers {
  "ScalaExample" should "sum two numbers" in {
    ScalaExample.sum(1, 5) shouldBe 6
    System.currentTimeMillis()
  }
}
