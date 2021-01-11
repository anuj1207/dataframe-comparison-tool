package utility.df.comparison

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{ col, date_trunc, format_number }
import org.apache.spark.sql.types.{ DataTypes, DoubleType, StructField }
import org.apache.spark.sql.{ Column, DataFrame }

object DataFrameComparisonTool extends Logging {

  /**
   * The following method is not schema sensitive //TODO: add this as a feature in following update
   *
   * @param generatedDF         Generated DataFrame from the logic
   * @param expectedDF          Expected resultant DataFrame
   * @param keyCols             Columns through which each row can be uniquely identified (will be used in showing the diff if any)
   * @param decimalPrecision    Decision precision for the comparison of Double values
   * @param ignoreCols          The columns which are to be ignored in this comparison
   * @return                    true if dataframes match with given inputs and false if not
   */
  def compare(generatedDF: DataFrame, expectedDF: DataFrame, keyCols: List[Column], decimalPrecision: Int, ignoreCols: List[String] = Nil): Boolean = {
    log.debug("1. Comparison: Row Count")
    if (compareRowCount(generatedDF, expectedDF)) {
      log.info("Row count matched successfully")
      log.info("2. Comparison: Data comparison for common columns")
      if (compareData(generatedDF, expectedDF, keyCols, decimalPrecision, ignoreCols)) {
        log.info("Data matched successfully")
        log.info("Bonus level comparison: Schema comparison")
        if (compareSchema(generatedDF, expectedDF)) {
          log.info("Schema matched successfully")
        } else {
          log.info("Schema mismatched! Sad but workable :)")
        }
        true
      } else {
        log.info("Data mismatched! Sad")
        false
      }
    } else {
      log.info("Row count mismatched! Sad")
      false
    }
  }

  private def compareRowCount(generatedDF: DataFrame, expectedDF: DataFrame): Boolean = {
    val first = generatedDF.count()
    val second = expectedDF.count()
    log.info(s"Generated row count: $first")
    log.info(s"Expected row count: $second")
    first == second
  }

  private def compareSchema(generated: DataFrame, expected: DataFrame): Boolean = {
    val expectedColSet = expected.schema.fields.map(_.name.toLowerCase).toSet
    val generatedColSet = generated.schema.fields.map(_.name.toLowerCase).toSet
    val extraExpectedCols = expectedColSet.diff(generatedColSet)
    val extraGeneratedCols = generatedColSet.diff(expectedColSet)
    val isEqual = extraGeneratedCols.isEmpty && extraExpectedCols.isEmpty
    if (!isEqual) {
      log.info("Columns were not equal")
      log.info(s"Expected dataset has more columns: $extraExpectedCols")
      log.info(s"Generate dataset has more columns: $extraGeneratedCols")
    }
    isEqual
  }

  private def compareData(generated: DataFrame, expected: DataFrame, keyColumns: List[Column], decimalPrecision: Int, ignoreCols: List[String]) = {
    val generatedColumns = generated.schema.map(_.name.toLowerCase)
    val expectedColumns = expected.schema.map(_.name.toLowerCase)

    val commonColumns = generatedColumns.intersect(expectedColumns)

    val commonCols: List[Column] = generated.schema.fields
      .filter(f => commonColumns.contains(f.name.toLowerCase) && !ignoreCols.map(_.toLowerCase).contains(f.name.toLowerCase))
      .map {
        case StructField(name, DataTypes.TimestampType, _, _) => date_trunc("minute", col(name.toLowerCase)).as(name) //TODO: add time level to be taken from user for test criteria
        case StructField(name, DoubleType, _, _) => format_number(col(name.toLowerCase), decimalPrecision).as(name)
        case StructField(name, _, _, _) => col(name.toLowerCase)
          //TODO: make this bit more generic to take custom rules as per use case requirements
      }.toList

    log.info(s"Key column comparison: $keyColumns")

    val areKeyColumnsVerified = keyColComparison(keyColumns, generated, expected)
    if (!areKeyColumnsVerified) {
      areKeyColumnsVerified
    } else {
      log.info("Key columns are equal | now comparing common columns")
      val prepExpectedDF = expected.select(commonCols: _*)
      val prepGeneratedDF = generated.select(commonCols: _*)

      val finalDiff = prepGeneratedDF.except(prepExpectedDF)
      val result = finalDiff.isEmpty
      if (!result) {
        log.info("Final diff wasn't empty:")
        log.info("Expected Data:")
        prepExpectedDF.sort(keyColumns: _*).show(false)
        log.info("Generated Data:")
        prepGeneratedDF.sort(keyColumns: _*).show(false)
        log.info("Diff Found:")
        finalDiff.sort(keyColumns: _*).show()
        val columns = finalDiff.schema.fields.map(_.name)
        val selectiveDifferences = columns.map(columnName => {
          prepGeneratedDF.select(col(columnName) :: keyColumns: _*)
            .except(prepExpectedDF.select(col(columnName) :: keyColumns: _*))
        })
        selectiveDifferences.foreach(diff => {
          if (!diff.isEmpty) diff.sort(keyColumns: _*).show
        })
      } else {
        log.info("Common columns are equal! Yippee")
      }
      result
    }
  }

  def keyColComparison(keyColumns: List[Column], expected: DataFrame, generated: DataFrame): Boolean = {
    if (keyColumns.isEmpty) {
      log.info("Key column Not specified | skipping key column matching")
      true
    } else {
      val keyExpect = expected.select(keyColumns: _*)
      val keyGen = generated.select(keyColumns: _*)
      val keyDiff = keyGen.except(keyExpect)
      val keyCompResult = keyDiff.isEmpty
      if (!keyCompResult) {
        log.info("Key columns are different for the following")
        keyDiff.show()
      }
      keyCompResult
    }
  }

}
