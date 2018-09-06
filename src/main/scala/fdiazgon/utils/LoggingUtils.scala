package fdiazgon.utils

import com.jcabi.log.MulticolorLayout
import org.apache.log4j.Logger

object LoggingUtils {

  private val HEADER_SIZE = 70

  private val logger: Logger = Logger.getLogger("mylogger")
  private val consoleAppender = logger.getAppender("consoleColoured")
  private val defaultLayout = consoleAppender.getLayout

  def printHeader(header: String): Unit = {
    val headerLayout = new MulticolorLayout()
    headerLayout.setConversionPattern("%color-1;39;39{%m%n}")
    consoleAppender.setLayout(headerLayout)
    logger.info("-" * HEADER_SIZE)
    headerLayout.setConversionPattern("%color-1;39;32{%m%n}")
    logger.info(header.toUpperCase)
    headerLayout.setConversionPattern("%color-1;39;39{%m%n}")
    logger.info("-" * HEADER_SIZE)
    consoleAppender.setLayout(defaultLayout)
  }

}
