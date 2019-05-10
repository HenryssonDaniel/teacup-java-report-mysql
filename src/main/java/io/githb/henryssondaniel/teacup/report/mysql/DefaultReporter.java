package io.githb.henryssondaniel.teacup.report.mysql;

import com.mysql.cj.jdbc.MysqlDataSource;
import io.github.henryssondaniel.teacup.core.configuration.Factory;
import io.github.henryssondaniel.teacup.core.reporting.Reporter;
import io.github.henryssondaniel.teacup.core.testing.Node;
import io.github.henryssondaniel.teacup.core.testing.Result;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import javax.sql.DataSource;

/**
 * Reporter that saves the logs into a MySQL database.
 *
 * @since 1.0
 */
public class DefaultReporter implements Reporter {
  private static final String CONNECTION_ERROR =
      "Could not establish a connection to the database %s";
  private static final String EXECUTION = "execution";
  private static final String LOG = "log";
  private static final Logger LOGGER = Logger.getLogger(DefaultReporter.class.getName());
  private static final String MYSQL_PROPERTY = "reporter.mysql.";
  private static final String NODE = "node";
  private static final Properties PROPERTIES = Factory.getProperties();
  private static final String RESULT = "result";
  private static final String SCHEMA = "teacup_report";
  private static final String SESSION_EXECUTION = "session_" + EXECUTION;
  private static final String SESSION_LOG = "session_" + LOG;
  private static final String SKIPPED = "skipped";
  private static final String UPDATED_ERROR = "%s could not be updated with %s";
  private final DataSource dataSource;
  private final Map<Node, Integer> map = new HashMap<>(0);

  private int sessionId;

  /**
   * Constructor.
   *
   * @since 1.0
   */
  public DefaultReporter() {
    this(createMysqlDataSource());
  }

  DefaultReporter(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public void finished(Node node, Result result) {
    LOGGER.log(Level.FINE, "Finished");

    if (sessionId > 0) {
      var optionalId = Optional.ofNullable(map.get(node));

      if (optionalId.isPresent())
        try (var connection = dataSource.getConnection()) {
          int id = optionalId.get();

          insertResult(connection, id, node, result);
          updateFinished(connection, id, node);
        } catch (SQLException e) {
          LOGGER.log(Level.WARNING, String.format(CONNECTION_ERROR, SCHEMA), e);
        }
      else
        LOGGER.warning(
            node.getName()
                + "finished but was not expected to do so. This might be because it has already "
                + "finished, was never initialized or the session terminated before the node "
                + "finished.");
    }
  }

  @Override
  public void initialize() {
    try (var connection = dataSource.getConnection()) {
      createSchema(connection);
      createNode(connection);
      createSessionExecution(connection);
      createSessionLog(connection);
      createExecution(connection);
      createLog(connection);
      createSkipped(connection);
      createResult(connection);

      insertSessionExecution(connection);
    } catch (SQLException e) {
      LOGGER.log(Level.SEVERE, "Could not initialize the database", e);
    }
  }

  @Override
  public void initialized(Collection<? extends Node> nodes) {
    LOGGER.log(Level.FINE, "Initialized");

    if (sessionId > 0 && !nodes.isEmpty())
      try (var connection = dataSource.getConnection()) {
        insertExecutions(connection, nodes);
      } catch (SQLException e) {
        LOGGER.log(Level.WARNING, String.format(CONNECTION_ERROR, SCHEMA), e);
      }
  }

  @Override
  public void log(LogRecord logRecord, Node node) {
    LOGGER.log(Level.FINE, "Log");

    if (sessionId > 0)
      try (var connection = dataSource.getConnection()) {
        var optionalId = Optional.ofNullable(map.get(node));

        if (optionalId.isPresent()) insertLog(connection, optionalId.get(), logRecord);
        else insertSessionLog(connection, logRecord);
      } catch (SQLException e) {
        LOGGER.log(Level.SEVERE, "Could not save the log", e);
      }
  }

  @Override
  public void skipped(Node node, String reason) {
    LOGGER.log(Level.INFO, "Skipped");

    if (sessionId > 0) {
      var optionalId = Optional.ofNullable(map.get(node));

      if (optionalId.isPresent())
        try (var connection = dataSource.getConnection();
            var preparedStatement =
                connection.prepareStatement(
                    "INSERT INTO `"
                        + SCHEMA
                        + "`.`"
                        + SKIPPED
                        + "`(`"
                        + EXECUTION
                        + ".id`, reason) VALUES(?, ?)")) {
          preparedStatement.setInt(1, optionalId.get());
          preparedStatement.setString(2, reason);

          preparedStatement.execute();
        } catch (SQLException e) {
          LOGGER.log(Level.WARNING, String.format(UPDATED_ERROR, node.getName(), SKIPPED), e);
        }
      else
        LOGGER.warning(
            String.format(
                "%s skipped but was not expected to do so. This might be because it has already "
                    + "skipped, was never initialized or the session terminated before the node "
                    + "skipped.",
                node.getName()));
    }
  }

  @Override
  public void started(Node node) {
    LOGGER.log(Level.FINE, "Started");

    if (sessionId > 0) {
      var optionalId = Optional.ofNullable(map.get(node));

      if (optionalId.isPresent())
        try (var connection = dataSource.getConnection();
            var preparedStatement =
                connection.prepareStatement(
                    "UPDATE `" + SCHEMA + "`.`" + EXECUTION + "` SET started = ? WHERE ID = ?")) {
          preparedStatement.setTimestamp(1, new Timestamp(node.getTimeStarted()));
          preparedStatement.setInt(2, optionalId.get());

          preparedStatement.execute();
        } catch (SQLException e) {
          LOGGER.log(Level.WARNING, String.format(UPDATED_ERROR, node.getName(), "started"), e);
        }
      else
        LOGGER.warning(
            node.getName()
                + "started but was not expected to do so. This might be because it was never "
                + "initialized or the session terminated before the "
                + NODE
                + " finished.");
    }
  }

  @Override
  public void terminated() {
    LOGGER.log(Level.FINE, "Terminated");

    if (sessionId > 0) {
      map.clear();

      try (var connection = dataSource.getConnection();
          var preparedStatement =
              connection.prepareStatement(
                  "UPDATE `"
                      + SCHEMA
                      + "`.`"
                      + SESSION_EXECUTION
                      + "` SET terminated_time = ? WHERE ID = ?")) {
        preparedStatement.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        preparedStatement.setInt(2, sessionId);

        preparedStatement.execute();
      } catch (SQLException e) {
        LOGGER.log(Level.WARNING, "Could not terminate the session", e);
      }
    }
  }

  private static void createExecution(Connection connection) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE IF NOT EXISTS `"
              + SCHEMA
              + "`.`"
              + EXECUTION
              + "` ( `finished` TIMESTAMP(3) NULL, `id` INT UNSIGNED NOT NULL AUTO_INCREMENT, `"
              + NODE
              + ".id` INT UNSIGNED NOT NULL, `"
              + SESSION_EXECUTION
              + ".id` INT UNSIGNED NOT NULL, `started` TIMESTAMP(3) NULL, PRIMARY KEY (`id`), "
              + "UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE, INDEX `"
              + NODE
              + ".id_idx` (`node.id` ASC) VISIBLE, INDEX `"
              + SESSION_EXECUTION
              + ".id_idx` (`"
              + SESSION_EXECUTION
              + ".id` ASC) VISIBLE, CONSTRAINT `"
              + EXECUTION
              + '.'
              + NODE
              + "` FOREIGN KEY (`"
              + NODE
              + ".id`) REFERENCES `teacup_report`.`"
              + NODE
              + "` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION, CONSTRAINT `"
              + EXECUTION
              + '.'
              + SESSION_EXECUTION
              + "` FOREIGN KEY (`"
              + SESSION_EXECUTION
              + ".id`) REFERENCES `teacup_report`.`"
              + SESSION_EXECUTION
              + "` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION)");
    }
  }

  private static void createLog(Connection connection) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE IF NOT EXISTS `"
              + SCHEMA
              + "`.`"
              + LOG
              + "` (`"
              + EXECUTION
              + ".id` INT UNSIGNED NOT NULL, `id` INT UNSIGNED NOT NULL AUTO_INCREMENT, `level` "
              + "ENUM('config', 'fine', 'finer', 'finest', 'info', 'severe', 'warning') NOT NULL, "
              + "`message` TEXT NULL, `time` TIMESTAMP(3) NOT NULL, PRIMARY KEY (`id`), "
              + "UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE, INDEX `"
              + LOG
              + '.'
              + EXECUTION
              + "_idx` (`"
              + EXECUTION
              + ".id` ASC) VISIBLE, CONSTRAINT `"
              + LOG
              + '.'
              + EXECUTION
              + "` FOREIGN KEY (`"
              + EXECUTION
              + ".id`) REFERENCES `teacup_report`.`"
              + EXECUTION
              + "` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION)");
    }
  }

  private static DataSource createMysqlDataSource() {
    var mysqlDataSource = new MysqlDataSource();
    mysqlDataSource.setPassword(PROPERTIES.getProperty(MYSQL_PROPERTY + "password"));
    mysqlDataSource.setServerName(PROPERTIES.getProperty(MYSQL_PROPERTY + "server.name"));
    mysqlDataSource.setUser(PROPERTIES.getProperty(MYSQL_PROPERTY + "user"));

    return mysqlDataSource;
  }

  private static void createNode(Connection connection) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE IF NOT EXISTS `"
              + SCHEMA
              + "`.`"
              + NODE
              + "` ( `id` INT UNSIGNED NOT NULL AUTO_INCREMENT, `name` VARCHAR(255) NOT NULL, "
              + "PRIMARY KEY (`id`), UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE, UNIQUE INDEX "
              + "`name_UNIQUE` (`name` ASC) VISIBLE)");
    }
  }

  private static void createResult(Connection connection) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE IF NOT EXISTS `"
              + SCHEMA
              + "`.`"
              + RESULT
              + "` ( `error` TEXT NULL, `"
              + EXECUTION
              + ".id` INT UNSIGNED NOT NULL, `id` INT UNSIGNED NOT NULL AUTO_INCREMENT, `status` "
              + "ENUM('aborted', 'failed', 'successful') NOT NULL, PRIMARY KEY (`id`), "
              + "UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE, UNIQUE INDEX `"
              + EXECUTION
              + ".id_UNIQUE` (`"
              + EXECUTION
              + ".id` ASC) VISIBLE, CONSTRAINT `"
              + RESULT
              + '.'
              + EXECUTION
              + "` FOREIGN KEY (`"
              + EXECUTION
              + ".id`) REFERENCES `teacup_report`.`"
              + EXECUTION
              + "` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION)");
    }
  }

  private static void createSchema(Connection connection) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);
    }
  }

  private static void createSessionExecution(Connection connection) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE IF NOT EXISTS `"
              + SCHEMA
              + "`.`"
              + SESSION_EXECUTION
              + "` ( `id` INT UNSIGNED NOT NULL AUTO_INCREMENT, `initialized` TIMESTAMP(3) "
              + "NOT NULL DEFAULT NOW(3), `terminated_time` TIMESTAMP(3) NULL, PRIMARY KEY (`id`), "
              + "UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE)");
    }
  }

  private static void createSessionLog(Connection connection) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE IF NOT EXISTS `"
              + SCHEMA
              + "`.`"
              + SESSION_LOG
              + "` ( `id` INT UNSIGNED NOT NULL AUTO_INCREMENT, `level` "
              + "ENUM('config', 'fine', 'finer', 'finest', 'info', 'severe', 'warning') NOT NULL, "
              + "`message` TEXT NULL, `"
              + SESSION_EXECUTION
              + ".id` INT UNSIGNED NOT NULL, `time` TIMESTAMP(3) NOT NULL, PRIMARY KEY (`id`), "
              + "UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE, INDEX `"
              + SESSION_LOG
              + '.'
              + SESSION_EXECUTION
              + "_idx` (`"
              + SESSION_EXECUTION
              + ".id` ASC) VISIBLE, CONSTRAINT `"
              + SESSION_LOG
              + '.'
              + SESSION_EXECUTION
              + "` FOREIGN KEY (`"
              + SESSION_EXECUTION
              + ".id`) REFERENCES `teacup_report`.`"
              + SESSION_EXECUTION
              + "` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION)");
    }
  }

  private static void createSkipped(Connection connection) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE IF NOT EXISTS `"
              + SCHEMA
              + "`.`"
              + SKIPPED
              + "` ( `"
              + EXECUTION
              + ".id` INT UNSIGNED NOT NULL, `id` INT UNSIGNED NOT NULL AUTO_INCREMENT, "
              + "`reason` TEXT NULL, PRIMARY KEY (`id`), UNIQUE INDEX `id_UNIQUE` (`id` ASC) "
              + "VISIBLE, UNIQUE INDEX `"
              + EXECUTION
              + ".id_UNIQUE` (`"
              + EXECUTION
              + ".id` ASC) VISIBLE, CONSTRAINT `skipped."
              + EXECUTION
              + "` FOREIGN KEY (`"
              + EXECUTION
              + ".id`) REFERENCES `teacup_report`.`"
              + EXECUTION
              + "` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION)");
    }
  }

  private static void executeLogStatement(
      int id, LogRecord logRecord, PreparedStatement preparedStatement) throws SQLException {
    preparedStatement.setInt(1, getOrdinal(logRecord));
    preparedStatement.setString(2, new SimpleFormatter().formatMessage(logRecord));
    preparedStatement.setInt(3, id);
    preparedStatement.setTimestamp(4, new Timestamp(logRecord.getMillis()));

    preparedStatement.execute();
  }

  private static int getNodeId(Statement statement) throws SQLException {
    var id = 0;

    try (var resultSet = statement.getGeneratedKeys()) {
      if (resultSet.next()) id = resultSet.getInt(1);
      else LOGGER.log(Level.WARNING, "No key was not generated. The logs will not be saved.");
    }

    return id;
  }

  private static int getOrdinal(LogRecord logRecord) {
    int ordinal;
    switch (logRecord.getLevel().getName()) {
      case "CONFIG":
        ordinal = 1;
        break;
      case "FINE":
        ordinal = 2;
        break;
      case "FINER":
        ordinal = 3;
        break;
      case "FINEST":
        ordinal = 4;
        break;
      case "INFO":
        ordinal = 5;
        break;
      case "SEVERE":
        ordinal = 6;
        break;
      default:
        ordinal = 7;
        break;
    }
    return ordinal;
  }

  private void insertExecution(
      Connection connection, Node node, PreparedStatement preparedStatement) throws SQLException {
    try (var resultSet = preparedStatement.executeQuery()) {
      insertExecution(
          connection,
          resultSet.next() ? resultSet.getInt(1) : insertNode(connection, node.getName()),
          node);
    }
  }

  private void insertExecution(Connection connection, int id, Node node) throws SQLException {
    try (var preparedStatement =
        connection.prepareStatement(
            "INSERT INTO `"
                + SCHEMA
                + "`.`"
                + EXECUTION
                + "`(`"
                + NODE
                + ".id`, `"
                + SESSION_EXECUTION
                + ".id`) VALUES(?, ?)",
            Statement.RETURN_GENERATED_KEYS)) {
      preparedStatement.setInt(1, id);
      preparedStatement.setInt(2, sessionId);

      preparedStatement.execute();

      putExecutionId(node, preparedStatement);
    }
  }

  private void insertExecutions(Connection connection, Iterable<? extends Node> nodes) {
    for (var node : nodes) {
      try (var preparedStatement =
          connection.prepareStatement(
              "SELECT id FROM `" + SCHEMA + "`.`" + NODE + "` WHERE name = ?")) {
        preparedStatement.setString(1, node.getName());

        insertExecution(connection, node, preparedStatement);
      } catch (SQLException e) {
        LOGGER.log(
            Level.WARNING,
            "Could not retrieve the " + NODE + " ID. The logs will not be saved.",
            e);
      }

      insertExecutions(connection, node.getNodes());
    }
  }

  private static void insertLog(Connection connection, Integer id, LogRecord logRecord)
      throws SQLException {
    try (var preparedStatement =
        connection.prepareStatement(
            "INSERT INTO `"
                + SCHEMA
                + "`.`"
                + LOG
                + "`(level, message, `"
                + EXECUTION
                + ".id`, time) VALUES(?, ?, ?, ?)")) {
      executeLogStatement(id, logRecord, preparedStatement);
    }
  }

  private static int insertNode(Connection connection, String name) throws SQLException {
    try (var preparedStatement =
        connection.prepareStatement(
            "INSERT INTO `" + SCHEMA + "`.`" + NODE + "` SET name = ?",
            Statement.RETURN_GENERATED_KEYS)) {
      preparedStatement.setString(1, name);
      preparedStatement.execute();

      return getNodeId(preparedStatement);
    }
  }

  private static void insertResult(Connection connection, int id, Node node, Result result) {
    try (var preparedStatement =
        connection.prepareStatement(
            "INSERT INTO `"
                + SCHEMA
                + "`.`"
                + RESULT
                + "`(error, `"
                + EXECUTION
                + ".id`, status) VALUES(?, ?, ?)")) {
      preparedStatement.setString(1, result.getThrowable().map(Object::toString).orElse(null));
      preparedStatement.setInt(2, id);
      preparedStatement.setInt(3, result.getStatus().ordinal() + 1);

      preparedStatement.execute();
    } catch (SQLException e) {
      LOGGER.log(
          Level.WARNING, String.format("Could not set the result for %s", node.getName()), e);
    }
  }

  private void insertSessionExecution(Connection connection) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.executeUpdate(
          "INSERT INTO `" + SCHEMA + "`.`" + SESSION_EXECUTION + "`() VALUES()",
          Statement.RETURN_GENERATED_KEYS);

      setSessionId(statement);
    }
  }

  private void insertSessionLog(Connection connection, LogRecord logRecord) throws SQLException {
    try (var preparedStatement =
        connection.prepareStatement(
            "INSERT INTO `"
                + SCHEMA
                + "`.`"
                + SESSION_LOG
                + "`(level, message, `"
                + SESSION_EXECUTION
                + ".id`, time) VALUES(?, ? ,? , ?)")) {
      executeLogStatement(sessionId, logRecord, preparedStatement);
    }
  }

  private void putExecutionId(Node node, Statement statement) throws SQLException {
    try (var resultSet = statement.getGeneratedKeys()) {
      if (resultSet.next()) map.put(node, resultSet.getInt(1));
      else LOGGER.log(Level.WARNING, "Could not retrieve the log ID. No logs will be saved.");
    }
  }

  private void setSessionId(Statement statement) throws SQLException {
    try (var resultSet = statement.getGeneratedKeys()) {
      if (resultSet.next()) sessionId = resultSet.getInt(1);
      else LOGGER.log(Level.WARNING, "Could not retrieve the session ID. No logs will be saved.");
    }
  }

  private static void updateFinished(Connection connection, int id, Node node) {
    try (var preparedStatement =
        connection.prepareStatement(
            "UPDATE `" + SCHEMA + "`.`" + EXECUTION + "` SET finished = ? WHERE ID = ?")) {
      preparedStatement.setTimestamp(1, new Timestamp(node.getTimeFinished()));
      preparedStatement.setInt(2, id);

      preparedStatement.execute();
    } catch (SQLException e) {
      LOGGER.log(Level.WARNING, String.format("Could not finish %s", node.getName()), e);
    }
  }
}
