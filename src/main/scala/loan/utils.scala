package loan

import scala.util.Random

object utils {
  def randomCreditScore(): Int = {
    val rand = Random.nextDouble()
    if (rand < 0.16) {
      // 16% chance for 300-579
      300 + (Random.nextDouble() * 279).toInt
    } else if (rand < 0.33) {  // 0.16 + 0.17
      // 17% chance for 580-669
      580 + (Random.nextDouble() * 89).toInt
    } else if (rand < 0.54) {  // 0.33 + 0.21
      // 21% chance for 670-739
      670 + (Random.nextDouble() * 69).toInt
    } else if (rand < 0.80) {  // 0.54 + 0.26
      // 26% chance for 740-799
      740 + (Random.nextDouble() * 59).toInt
    } else {
      // 20% chance for 800-850
      800 + (Random.nextDouble() * 50).toInt
    }
  }

  def skewedRandom(): Double = {
    val baseRandom = Random.nextDouble()

    // Apply transformation to make 50% of values > 0.8
    val skewed = if (baseRandom < 0.5) {
      // For the first 50% of values, map them to range [0, 0.8)
      baseRandom * 0.8
    } else {
      // For the other 50%, map them to range [0.8, 1.0)
      0.8 + (baseRandom - 0.5) * 0.4
    }

    // Round to 2 decimal places
    math.floor(skewed * 100) / 100
  }
}
