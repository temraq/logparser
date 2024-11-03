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

    // ВАЖНО: import spark.implicits._ должен быть после создания SparkSession

    val dataPath = "path_to_logs"

    // Читаем и парсим логи
    val events = readAndParseLogs(dataPath)

    // Задание 1: Количество поисков документа ACC_45616 через карточку поиска
    val documentIdToSearch = "ACC_45616"
    val cardSearchCount = CardSearchAnalyzer.countSpecificDocumentSearch(events, documentIdToSearch)
    println(s"Количество поисков документа $documentIdToSearch через карточку поиска: $cardSearchCount")

    // Задание 2: Количество открытий каждого документа, найденного через быстрый поиск, за каждый день
    val docOpenCounts = QuickSearchAnalyzer.countDocumentOpensPerDay(events)
    docOpenCounts.show(truncate = false)

    // Сохранение результатов
    docOpenCounts.write
      .option("header", "true")
      .csv("output/doc_open_counts")

    spark.stop()
  }

  def readAndParseLogs(dataPath: String)(implicit spark: SparkSession): Dataset[Event] = {
    import spark.implicits._

    val files = Files.list(Paths.get(dataPath)).toArray.map(_.toString)

    val eventsSeq = files.flatMap { file =>
      val lines = scala.io.Source.fromFile(file).getLines().toSeq
      LogParserService.parseLines(lines)
    }.toSeq

    // Создаем Dataset из последовательности (Seq) напрямую
    eventsSeq.toDS()
  }
}
