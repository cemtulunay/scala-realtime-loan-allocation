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

  object IndustryMapping {

    // Simple map of industries and their credibility scores
    val industries = Map(
      "Technology" -> 0.95,
      "Healthcare" -> 0.90,
      "Government" -> 0.88,
      "Finance" -> 0.85,
      "Education" -> 0.75,
      "Manufacturing" -> 0.70,
      "Real Estate" -> 0.65,
      "Retail" -> 0.55,
      "Hospitality" -> 0.50,
      "Entertainment" -> 0.45
    )

    // Get random industry name
    def getRandomIndustry(): String = {
      val industryNames = industries.keys.toSeq
      industryNames(Random.nextInt(industryNames.size))
    }

    // Get credibility score for an industry
    def getScore(industry: String): Double = {
      industries.getOrElse(industry, 0.5)
    }

    // Get realistic debt-to-income ratio based on industry
    def getDebtToIncomeRatio(industry: String): Double = {
      val baseRatio = industry match {
        case "Technology" => 0.25 + (Random.nextDouble() * 0.15)      // 0.25-0.40 (lower debt, higher income)
        case "Healthcare" => 0.28 + (Random.nextDouble() * 0.17)      // 0.28-0.45 (stable income, moderate debt)
        case "Government" => 0.30 + (Random.nextDouble() * 0.15)      // 0.30-0.45 (stable but moderate income)
        case "Finance" => 0.27 + (Random.nextDouble() * 0.18)         // 0.27-0.45 (good income, lifestyle debt)
        case "Education" => 0.35 + (Random.nextDouble() * 0.20)       // 0.35-0.55 (lower income, student loans)
        case "Manufacturing" => 0.33 + (Random.nextDouble() * 0.22)   // 0.33-0.55 (moderate income, cyclical)
        case "Real Estate" => 0.30 + (Random.nextDouble() * 0.25)     // 0.30-0.55 (variable income)
        case "Retail" => 0.40 + (Random.nextDouble() * 0.20)          // 0.40-0.60 (lower wages)
        case "Hospitality" => 0.42 + (Random.nextDouble() * 0.23)     // 0.42-0.65 (lower wages, irregular income)
        case "Entertainment" => 0.35 + (Random.nextDouble() * 0.30)   // 0.35-0.65 (very variable income)
        case _ => 0.35 + (Random.nextDouble() * 0.20)                  // Default: 0.35-0.55
      }
      // Round to 2 decimal places
      Math.round(baseRatio * 100.0) / 100.0
    }

    // Get employment stability score based on industry (0 = unstable, 1 = very stable)
    def getEmploymentStability(industry: String): Double = {
      val stabilityScore = industry match {
        case "Government" => 0.85 + (Random.nextDouble() * 0.10)      // 0.85-0.95 (highest stability, civil service)
        case "Healthcare" => 0.80 + (Random.nextDouble() * 0.15)      // 0.80-0.95 (essential services, aging population)
        case "Education" => 0.75 + (Random.nextDouble() * 0.15)       // 0.75-0.90 (tenure system, consistent demand)
        case "Technology" => 0.70 + (Random.nextDouble() * 0.20)      // 0.70-0.90 (high demand but competitive)
        case "Finance" => 0.65 + (Random.nextDouble() * 0.20)         // 0.65-0.85 (market dependent, cyclical)
        case "Manufacturing" => 0.55 + (Random.nextDouble() * 0.25)   // 0.55-0.80 (automation risk, economic cycles)
        case "Real Estate" => 0.45 + (Random.nextDouble() * 0.25)     // 0.45-0.70 (market cycles, commission-based)
        case "Retail" => 0.40 + (Random.nextDouble() * 0.25)          // 0.40-0.65 (automation, e-commerce impact)
        case "Hospitality" => 0.30 + (Random.nextDouble() * 0.30)     // 0.30-0.60 (seasonal, economic sensitivity)
        case "Entertainment" => 0.25 + (Random.nextDouble() * 0.35)   // 0.25-0.60 (project-based, unpredictable)
        case _ => 0.55 + (Random.nextDouble() * 0.25)                  // Default: 0.55-0.80
      }
      // Round to 2 decimal places
      Math.round(stabilityScore * 100.0) / 100.0
    }


    // Make realistic loan decision based on multiple factors
    def makeLoanDecision(
                          predictedIncome: Double,
                          creditScore: Int,
                          predictedNpl: Double,
                          employmentIndustry: String,
                          debtToIncomeRatio: Double,
                          employmentStability: Double
                        ): String = {

      // Score each factor (0-100)
      val incomeScore = Math.min(100, (predictedIncome / 1000).toInt)  // $1000 = 1 point
      val creditScoreNormalized = ((creditScore - 300) / 5.5).toInt    // 300-850 -> 0-100
      val nplScore = Math.max(0, (100 - (predictedNpl * 100)).toInt)   // Lower NPL = higher score
      val industryScore = (getScore(employmentIndustry) * 100).toInt
      val debtScore = Math.max(0, (100 - (debtToIncomeRatio * 200)).toInt)  // Lower DTI = higher score
      val stabilityScore = (employmentStability * 100).toInt

      // Weighted total score (different factors have different importance)
      val totalScore = (
        creditScoreNormalized * 0.30 +      // 30% - Most important
          nplScore * 0.25 +                 // 25% - AI prediction
          incomeScore * 0.20 +              // 20% - Income capacity
          debtScore * 0.15 +                // 15% - Current debt burden
          stabilityScore * 0.05 +           // 5% - Job stability
          industryScore * 0.05              // 5% - Industry factor
        ).toInt

      // Decision thresholds with some randomness for edge cases
      val decision = totalScore match {
        case score if score >= 80 => "APPROVED"
        case score if score >= 70 => if (Random.nextDouble() < 0.8) "APPROVED" else "MANUAL_REVIEW"
        case score if score >= 60 => if (Random.nextDouble() < 0.5) "APPROVED" else "MANUAL_REVIEW"
        case score if score >= 45 => if (Random.nextDouble() < 0.7) "MANUAL_REVIEW" else "REJECTED"
        case score if score >= 30 => if (Random.nextDouble() < 0.3) "MANUAL_REVIEW" else "REJECTED"
        case _ => "REJECTED"
      }

      // Hard rejection criteria (overrides score)
      if (creditScore < 400 || predictedNpl > 0.8 || debtToIncomeRatio > 0.7) {
        "REJECTED"
      } else {
        decision
      }
    }

  }


}

