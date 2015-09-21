package net.mkowalski.sparkfim.test

import java.io.File

import net.mkowalski.sparkfim.model.Item

import scala.io.Source

object TestFileUtil {

  def resultFromFile(resultDirectoryPath: String): Map[Item, Int] = {
    val allResultFiles = filesInDirectory(new File(resultDirectoryPath))
    allResultFiles.flatMap(
      resultFile => readExpectedResultFromFile(resultFile.getPath,
        supportDelimiter = ',',
        lineProcessor = line => line.filterNot("()".toSet))
    ).toMap
  }

  def readExpectedResultFromFile(resultFilePath: String,
                                 itemDelimiter: Char = ' ',
                                 supportDelimiter: Char = '\t',
                                 lineProcessor: String => String = identity): Map[Item, Int] = {

    val lines = Source.fromFile(resultFilePath).getLines()
    lines.filter(line => line.nonEmpty && !line.startsWith("#")).map(lineProcessor).map(line => {
      val split = line.split(supportDelimiter)
      val itemIds = split(0).split(itemDelimiter).map(_.toInt).sorted
      val support = split(1).toInt
      (Item(itemIds).sorted(), support)
    }).toMap

  }

  private def filesInDirectory(directory: File): Array[File] = {
    val current = directory.listFiles
    val dataFilesAndDirs = current.filterNot(file => file.isDirectory || !file.getName.startsWith("part"))
    dataFilesAndDirs ++ current.filter(_.isDirectory).flatMap(filesInDirectory)
  }

}
