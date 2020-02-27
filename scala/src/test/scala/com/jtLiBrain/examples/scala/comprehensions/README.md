1. `for (𝑝 <- 𝑒) yield 𝑒′` 被转换为`𝑒 .map { case 𝑝 => 𝑒′ }`
2. `for (𝑝 <- 𝑒) 𝑒′`被转换为`𝑒 .foreach { case 𝑝 => 𝑒′ }`
3. `for (𝑝 <- 𝑒; 𝑝′ <- 𝑒′;…) yield 𝑒″`被转换为`𝑒.flatMap { case 𝑝 => for (𝑝′ <- 𝑒′;…) yield 𝑒″ }`
4. `for (𝑝 <- 𝑒; 𝑝′ <- 𝑒′;…) 𝑒″`被转换为`𝑒.foreach { case 𝑝 => for (𝑝′ <- 𝑒′;…) 𝑒″ }`
5. `𝑝  <- 𝑒 g`被转换为`𝑝  <- 𝑒.withFilter((𝑥1,…,𝑥𝑛) => 𝑔)`
6. 

```scala
for  { i <- 1 until n
       j <- 1 until i
       if isPrime(i+j)
} yield (i, j)
```

```scala
(1 until n)
  .flatMap {
     case i => (1 until i)
       .withFilter { j => isPrime(i+j) }
       .map { case j => (i, j) } }
```