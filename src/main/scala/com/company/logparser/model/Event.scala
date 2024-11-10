package com.company.logparser.model

import java.time.LocalDateTime
import org.apache.spark.sql.{Encoder, Encoders}

sealed trait Event {
  def dateTime: LocalDateTime
}

case class SessionStart(dateTime: LocalDateTime) extends Event

case class SessionEnd(dateTime: LocalDateTime) extends Event

case class QuickSearch(
                        dateTime: LocalDateTime,
                        query: String,
                        searchId: String,
                        documentIds: Seq[String]
                      ) extends Event

case class CardSearch(
                       dateTime: LocalDateTime,
                       searchId: String,
                       parameters: Map[String, String],
                       documentIds: Seq[String]
                     ) extends Event

case class DocumentOpen(
                         dateTime: LocalDateTime,
                         searchId: String,
                         documentId: String
                       ) extends Event

object Event {
  implicit val eventEncoder: Encoder[Event] = Encoders.kryo[Event]
}
