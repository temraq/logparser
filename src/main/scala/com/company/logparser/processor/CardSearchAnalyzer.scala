package com.company.logparser.processor

import com.company.logparser.model._
import org.apache.spark.sql.{Dataset, SparkSession}

object CardSearchAnalyzer {

  def countSpecificDocumentSearch(
                                   events: Dataset[Event],
                                   documentId: String
                                 )(implicit spark: SparkSession): Long = {

    events.filter {
      case cs: CardSearchStart => cs.documentIds.contains(documentId)
      case _ => false
    }.count()
  }
}
