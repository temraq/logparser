package com.company.logparser.processor

import com.company.logparser.model._
import org.apache.spark.sql.{Dataset, SparkSession, DataFrame, Encoders}

object QuickSearchAnalyzer {

  def countDocumentOpensPerDay(
                                events: Dataset[Event]
                              )(implicit spark: SparkSession): DataFrame = {


    implicit val tuple2Encoder = Encoders.tuple(Encoders.STRING, Encoders.STRING)
    implicit val tuple3Encoder = Encoders.tuple(Encoders.STRING, Encoders.STRING, Encoders.STRING)

    val quickSearchDocs = events.flatMap {
        case qs: QuickSearch => qs.documentIds.map(docId => (qs.searchId, docId))
        case _ => Seq.empty
      }(tuple2Encoder)
      .toDF("searchId", "docId")
      .distinct()

    val docOpens = events.flatMap {
        case doEvent: DocumentOpen =>
          val date = doEvent.dateTime.toLocalDate
          val dateStr = f"${date.getDayOfMonth}%02d.${date.getMonthValue}%02d.${date.getYear}%04d"
          Seq((doEvent.searchId, doEvent.documentId, dateStr))
        case _ => Seq.empty
      }(tuple3Encoder)
      .toDF("searchId", "docId", "date")

    val joinedData = docOpens.join(quickSearchDocs, Seq("searchId", "docId"))

    val result = joinedData.groupBy("date", "docId").count()

    result
  }
}
