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
                        sessionId: String,
                        documentIds: Seq[String]
                      ) extends Event

case class CardSearchStart(
                            dateTime: LocalDateTime,
                            sessionId: String,
                            parameters: Map[String, String],
                            documentIds: Seq[String]
                          ) extends Event

case class DocumentOpen(
                         dateTime: LocalDateTime,
                         sessionId: String,
                         documentId: String
                       ) extends Event

object Event {
  // Явно определить неявный Encoder для Event используя Kryo-сериализацию
  implicit val eventEncoder: Encoder[Event] = Encoders.kryo[Event]
}