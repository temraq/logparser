package com.company.logparser

import com.company.logparser.model.Event
import com.company.logparser.parser.LogParserService
import com.company.logparser.processor.{CardSearchAnalyzer, QuickSearchAnalyzer}
import org.apache.spark.sql.{Dataset, SparkSession}

import java.nio.file.{Files, Paths}


object MainApp {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("LogParserApp")
      .master("local[*]")
      .getOrCreate()

    val dataPath = "input"

    val events = readAndParseLogs(dataPath)

    // Задание 1: Количество поисков документа ACC_45616 через карточку поиска
    val documentIdToSearch = "ACC_45616"
    val cardSearchCount = CardSearchAnalyzer.countSpecificDocumentSearch(events, documentIdToSearch)

    // Задание 2: Количество открытий каждого документа, найденного через быстрый поиск, за каждый день
    val docOpenCounts = QuickSearchAnalyzer.countDocumentOpensPerDay(events)
    docOpenCounts.show(truncate = false)

    docOpenCounts.write
      .option("header", "true")
      .mode("overwrite")
      .csv("output/doc_open_counts")

    spark.stop()
    println(s"Количество поисков документа $documentIdToSearch через карточку поиска: $cardSearchCount")
  }

  private def readAndParseLogs(dataPath: String)(implicit spark: SparkSession): Dataset[Event] = {
    import spark.implicits._

    val files = Files.list(Paths.get(dataPath)).toArray.map(_.toString)

    val eventsSeq = files.flatMap { file =>

      val encoding = "Windows-1251"
      val source = scala.io.Source.fromFile(file)(encoding)
      try {
        val lines = source.getLines().toSeq
        LogParserService.parseLines(lines)
      } catch {
        case e: java.nio.charset.MalformedInputException =>
          println(s"Ошибка декодирования файла $file: ${e.getMessage}")
          Seq.empty[Event]
        case e: Exception =>
          println(s"Неизвестная ошибка при обработке файла $file: ${e.getMessage}")
          Seq.empty[Event]
      } finally {
        source.close()
      }
    }.toSeq

    eventsSeq.toDS()
  }
}
