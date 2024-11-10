package com.company.logparser.parser

import com.company.logparser.model._
import scala.util.matching.Regex
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object LogParserService {

  private val dateTimePattern: DateTimeFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")

  private val sessionStartPattern: Regex = """SESSION_START\s+(\d{2}\.\d{2}\.\d{4}_\d{2}:\d{2}:\d{2})?""".r

  private val sessionEndPattern: Regex = """SESSION_END\s+(\d{2}\.\d{2}\.\d{4}_\d{2}:\d{2}:\d{2})?""".r

  private val quickSearchPattern: Regex = """QS\s*(\d{2}\.\d{2}\.\d{4}_\d{2}:\d{2}:\d{2})?\s*\{?(.*)""".r

  private val cardSearchStartPattern: Regex = """CARD_SEARCH_START\s*(\d{2}\.\d{2}\.\d{4}_\d{2}:\d{2}:\d{2})?""".r

  private val cardSearchEndPattern: Regex = """CARD_SEARCH_END\s*.*""".r

  private val docOpenPattern: Regex = """DOC_OPEN\s*(\d{2}\.\d{2}\.\d{4}_\d{2}:\d{2}:\d{2})?\s*(\S*)\s*(\S*)""".r

  private val resultPattern: Regex = """^\s*(\S+)\s+(.+?)\s*$""".r

  private val paramPattern: Regex = """\$(\d+)\s*(.*)""".r


  def parseLines(lines: Seq[String]): Seq[Event] = {
    val events = scala.collection.mutable.ListBuffer[Event]()
    var collectingCardParams = false
    var cardSearchParameters = scala.collection.mutable.Map[String, String]()
    var cardSearchDateTime: Option[LocalDateTime] = None
    var expectResult = false
    var lastEventType: Option[String] = None
    var quickSearchQuery: Option[String] = None
    var quickSearchDateTime: Option[LocalDateTime] = None

    lines.foreach { line =>
      line match {
        case sessionStartPattern(dateTimeStr) =>
          val dateTime = LocalDateTime.parse(dateTimeStr, dateTimePattern)
          events += SessionStart(dateTime)
          lastEventType = Some("SESSION_START")
          println(s"Обработано событие SessionStart: дата и время = $dateTime")

        case sessionEndPattern(dateTimeStr) =>
          val dateTime = LocalDateTime.parse(dateTimeStr, dateTimePattern)
          events += SessionEnd(dateTime)
          lastEventType = Some("SESSION_END")
          println(s"Обработано событие SessionEnd: дата и время = $dateTime")

        case quickSearchPattern(dateTimeStr, query) =>
          val dateTime = LocalDateTime.parse(dateTimeStr, dateTimePattern)
          quickSearchDateTime = Some(dateTime)
          quickSearchQuery = Some(query)
          expectResult = true
          lastEventType = Some("QUICK_SEARCH")
          println(s"Начат QuickSearch: дата и время = $dateTime, запрос = '$query'")

        case cardSearchStartPattern(dateTimeStr) =>
          val dateTime = LocalDateTime.parse(dateTimeStr, dateTimePattern)
          cardSearchDateTime = Some(dateTime)
          collectingCardParams = true
          cardSearchParameters.clear()
          expectResult = false
          lastEventType = Some("CARD_SEARCH")
          println(s"Начат CardSearch: дата и время = $dateTime")

        case cardSearchEndPattern() =>
          collectingCardParams = false
          expectResult = true
          println(s"Завершен ввод параметров CardSearch, ожидаем результаты")

        case paramPattern(paramId, paramValue) if collectingCardParams =>
          cardSearchParameters += (paramId -> paramValue)
          println(s"Собран параметр CardSearch: id = $paramId, значение = '$paramValue'")

        case docOpenPattern(dateTimeStr, searchId, docId) =>
          val dateTime = LocalDateTime.parse(dateTimeStr, dateTimePattern)
          events += DocumentOpen(dateTime, searchId, docId)
          lastEventType = Some("DOCUMENT_OPEN")
          println(s"Обработано событие DocumentOpen: дата и время = $dateTime, searchId = $searchId, docId = $docId")

        case resultPattern(searchId, docs) if expectResult =>
          val docIds = docs.split("\\s+").toSeq
          lastEventType match {
            case Some("CARD_SEARCH") =>
              events += CardSearch(
                dateTime = cardSearchDateTime.getOrElse(LocalDateTime.MIN),
                searchId = searchId,
                parameters = cardSearchParameters.toMap,
                documentIds = docIds
              )
              println(s"Обработаны результаты CardSearch: searchId = $searchId, документы = ${docIds.mkString(", ")}, параметры = $cardSearchParameters")
              cardSearchParameters.clear()
              cardSearchDateTime = None
              lastEventType = None
            case Some("QUICK_SEARCH") =>
              events += QuickSearch(
                dateTime = quickSearchDateTime.getOrElse(LocalDateTime.MIN),
                query = quickSearchQuery.getOrElse(""),
                searchId = searchId,
                documentIds = docIds
              )
              println(s"Обработаны результаты QuickSearch: searchId = $searchId, документы = ${docIds.mkString(", ")}, запрос = '${quickSearchQuery.getOrElse("")}'")

              quickSearchQuery = None
              quickSearchDateTime = None
              lastEventType = None
            case _ =>
              println(s"Неожиданный результат с lastEventType = $lastEventType")
          }
          expectResult = false

        case other =>
          if (collectingCardParams) {
            println(s"Строка '$other' не соответствует шаблону параметров, игнорируется")
          } else {
            println(s"Строка проигнорирована: '$other'")
          }
      }
    }

    events.toSeq
  }
}
