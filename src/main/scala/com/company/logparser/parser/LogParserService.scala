package com.company.logparser.parser

import com.company.logparser.model._
import scala.util.matching.Regex
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object LogParserService {

  private val dateTimePattern: DateTimeFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")
  private val sessionStartPattern: Regex = """SESSION_START (\d{2}\.\d{2}\.\d{4}_\d{2}:\d{2}:\d{2})""".r
  private val sessionEndPattern: Regex = """SESSION_END (\d{2}\.\d{2}\.\d{4}_\d{2}:\d{2}:\d{2})""".r
  private val qsPattern: Regex = """QS (\d{2}\.\d{2}\.\d{4}_\d{2}:\d{2}:\d{2}) \{(.+)""".r
  private val cardSearchPattern: Regex = """CARD_SEARCH_START (\d{2}\.\d{2}\.\d{4}_\d{2}:\d{2}:\d{2})""".r
  private val docOpenPattern: Regex = """DOC_OPEN (\d{2}\.\d{2}\.\d{4}_\d{2}:\d{2}:\d{2}) (\S+) (\S+)""".r
  private val resultPattern: Regex = """^(\S+) (.+)$""".r

  def parseLines(lines: Seq[String]): Seq[Event] = {
    val events = scala.collection.mutable.ListBuffer[Event]()
    var currentSessionId: Option[String] = None
    var expectResult: Boolean = false
    var lastEventType: Option[String] = None

    lines.foreach {
      case sessionStartPattern(dateTimeStr) =>
        val dateTime = LocalDateTime.parse(dateTimeStr, dateTimePattern)
        events += SessionStart(dateTime)
        currentSessionId = None
        lastEventType = Some("SESSION_START")

      case sessionEndPattern(dateTimeStr) =>
        val dateTime = LocalDateTime.parse(dateTimeStr, dateTimePattern)
        events += SessionEnd(dateTime)
        currentSessionId = None
        lastEventType = Some("SESSION_END")

      case qsPattern(dateTimeStr, query) =>
        val dateTime = LocalDateTime.parse(dateTimeStr, dateTimePattern)
        lastEventType = Some("QS")
        expectResult = true

      case cardSearchPattern(dateTimeStr) =>
        val dateTime = LocalDateTime.parse(dateTimeStr, dateTimePattern)
        lastEventType = Some("CARD_SEARCH_START")
        expectResult = true

      case docOpenPattern(dateTimeStr, sessionId, docId) =>
        val dateTime = LocalDateTime.parse(dateTimeStr, dateTimePattern)
        events += DocumentOpen(dateTime, sessionId, docId)
        lastEventType = Some("DOC_OPEN")

      case resultPattern(sessionId, docs) if expectResult =>
        val docIds = docs.split(" ").toSeq
        lastEventType match {
          case Some("QS") =>
            events += QuickSearch(LocalDateTime.MIN, "", sessionId, docIds)
          case Some("CARD_SEARCH_START") =>
            events += CardSearchStart(LocalDateTime.MIN, sessionId, Map.empty, docIds)
          case _ => // Игнорировать
        }
        expectResult = false
        currentSessionId = Some(sessionId)

      case _ => // Неизвестная строка, можно логировать или игнорировать
    }

    events.toSeq
  }
}
