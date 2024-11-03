package com.company.logparser.processor

import com.company.logparser.model._
import org.apache.spark.sql.{Dataset, SparkSession, DataFrame, Encoders}

object QuickSearchAnalyzer {

  def countDocumentOpensPerDay(
                                events: Dataset[Event]
                              )(implicit spark: SparkSession): DataFrame = {


    // Явное указание типов для Encoder
    implicit val tuple2Encoder: org.apache.spark.sql.Encoder[(String, String)] =
      Encoders.tuple(Encoders.STRING, Encoders.STRING)

    // Изменение кодировщика на String для поля date
    implicit val tuple3Encoder: org.apache.spark.sql.Encoder[(String, String, String)] =
      Encoders.tuple(Encoders.STRING, Encoders.STRING, Encoders.STRING)

    // Получаем QS события с их документами
    val quickSearchDocs = events.flatMap {
        case qs: QuickSearch => qs.documentIds.map(docId => (qs.sessionId, docId))
        case _ => Seq.empty
      }(tuple2Encoder) // Указываем какой именно Encoder использовать явно, чтобы избежать конфликта
      .toDF("sessionId", "docId")
      .distinct()

    // Получаем открытия документов и преобразуем LocalDate в String
    val docOpens = events.flatMap {
        case doEvent: DocumentOpen =>
          val dateStr = doEvent.dateTime.toLocalDate.toString // Форматирование даты как строки
          Seq((doEvent.sessionId, doEvent.documentId, dateStr))
        case _ => Seq.empty
      }(tuple3Encoder) // Явно указываем Encoder для трехэлементного кортежа
      .toDF("sessionId", "docId", "date")

    // Соединяем открытия с QS документами
    val joinedData = docOpens.join(quickSearchDocs, Seq("sessionId", "docId"))

    // Считаем количество открытий документов за день
    val result = joinedData.groupBy("date", "docId").count()

    result
  }
}
