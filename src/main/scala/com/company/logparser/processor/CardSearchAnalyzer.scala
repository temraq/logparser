package com.company.logparser.processor

import com.company.logparser.model._
import org.apache.spark.sql.{Dataset, SparkSession}

object CardSearchAnalyzer {

  def countSpecificDocumentSearch(
                                   events: Dataset[Event],
                                   documentId: String
                                 )(implicit spark: SparkSession): Long = {
    
    println(s"Всего событий: ${events.count()}")

    val matchingEvents = events.filter {
      case cs: CardSearch =>
        val containsDoc = cs.documentIds.contains(documentId)
        println(s"CardSearch событие с searchId = ${cs.searchId}, содержит документ $documentId: $containsDoc")
        containsDoc
      case _ => false
    }

    val count = matchingEvents.count()
    println(s"Найдено событий CardSearch с документом $documentId: $count")
    count
  }
}
