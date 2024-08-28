import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.ConsoleAppender
import org.slf4j.LoggerFactory
import kotlin.apply
import kotlin.jvm.java

object Logger {
    private val logging = LoggerFactory.getLogger(Logger::class.java)

    init {
        configureLogging()
    }

    fun info(message: String) {
        logging.info(message)
    }

    private fun configureLogging() {
        val context = LoggerFactory.getILoggerFactory() as LoggerContext
        context.reset()

        val encoder =
            PatternLayoutEncoder().apply {
                pattern = "%d{YYYY-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n"
                setContext(context)
                start()
            }

        val consoleAppender =
            ConsoleAppender<ILoggingEvent>().apply {
                setContext(context)
                setEncoder(encoder)
                start()
            }

        context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).apply {
            level = ch.qos.logback.classic.Level.TRACE
            addAppender(consoleAppender)
        }
    }
}
