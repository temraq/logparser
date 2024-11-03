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

    val dataPath = {
      "input"
    }

    // Читаем и парсим логи
    val events = readAndParseLogs(dataPath)

    // Задание 1: Количество поисков документа ACC_45616 через карточку поиска
    val documentIdToSearch = "ACC_45616"
    val cardSearchCount = CardSearchAnalyzer.countSpecificDocumentSearch(events, documentIdToSearch)


    // Задание 2: Количество открытий каждого документа, найденного через быстрый поиск, за каждый день
    val docOpenCounts = QuickSearchAnalyzer.countDocumentOpensPerDay(events)
    docOpenCounts.show(truncate = false)

    // Сохранение результатов
    docOpenCounts.write
      .option("header", "true")
      .mode("overwrite") // Добавлен режим перезаписи
      .csv("output/doc_open_counts")

    println(s"Количество поисков документа $documentIdToSearch через карточку поиска: $cardSearchCount")

    spark.stop()
  }

  private def readAndParseLogs(dataPath: String)(implicit spark: SparkSession): Dataset[Event] = {
    import spark.implicits._

    val files = Files.list(Paths.get(dataPath)).toArray.map(_.toString)

    val eventsSeq = files.flatMap { file =>
      // Укажите правильную кодировку, например, "Windows-1251" или "UTF-8"
      val encoding = "Windows-1251" // Замените на фактическую кодировку ваших файлов
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

    // Создаём Dataset из последовательности (Seq) напрямую
    eventsSeq.toDS()
  }
}