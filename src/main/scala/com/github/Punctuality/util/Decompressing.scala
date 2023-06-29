package com.github.Punctuality.util

import java.io.{File, InputStream, InputStreamReader}
import java.util.zip.GZIPInputStream
import scala.io.Source

object Decompressing {
  def decompressFile(filePath: String, to: Option[String] = None): (File, Long) =
    decompressStream(
      filePath.split("/").last,
      new GZIPInputStream(new java.io.FileInputStream(filePath)),
      to
    )

  def decompressResource(resourcePath: String, to: Option[String] = None): (File, Long) = {
    val uncompressedStream = new GZIPInputStream(getClass.getClassLoader.getResourceAsStream(resourcePath))

    decompressStream(resourcePath, uncompressedStream, to)
  }

  def decompressStream(name: String, is: InputStream, to: Option[String] = None): (File, Long) = {
    val uncompressedFile = to.fold(File.createTempFile(name.stripSuffix(".gz"), ".jsonl"))(new File(_))

    val uncompressedWriter = new java.io.FileWriter(uncompressedFile)

    val totalSize =
      Source
        .fromInputStream(is)
        .getLines()
        .foldLeft(0L) { (size, str) =>
          uncompressedWriter.write(str + "\n")
          size + str.length
        }

    uncompressedFile -> totalSize
  }

}
