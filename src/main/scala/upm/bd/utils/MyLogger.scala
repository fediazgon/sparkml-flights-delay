package upm.bd.utils

import com.jcabi.log.MulticolorLayout
import org.apache.log4j.Logger

object MyLogger {

  object Colors extends Enumeration {
    val Cyan, Green, White = Value
  }

  private val HEADER_SIZE = 70

  private val logger: Logger = Logger.getLogger("mylogger")
  private val consoleAppender = logger.getAppender("consoleInfo")
  private val defaultLayout = consoleAppender.getLayout

  private val headerLayout = new MulticolorLayout()
  headerLayout.setConversionPattern("[%color-cyan{%p}] %color-white{%m%n}")
  headerLayout.setColors("cyan:1;36,white:1")

  def printHeader(header: String): Unit = {

    consoleAppender.setLayout(headerLayout)

    //val padLength = (HEADER_SIZE / 2) + (header.length / 2)
    logger.info("-" * HEADER_SIZE)
    headerLayout.setColors("cyan:1;36,white:1;32")
    //logger.info(("%1$" + padLength + "s").format(header))
    logger.info(header)
    headerLayout.setColors("cyan:1;36,white:1")
    logger.info("-" * HEADER_SIZE)

    consoleAppender.setLayout(defaultLayout)
  }

  def info(message: String): Unit = {
    logger.info(message)
  }


}
