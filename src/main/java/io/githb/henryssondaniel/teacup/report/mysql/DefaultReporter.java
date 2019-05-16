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
  private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS ";
  private static final String EXECUTION_FK =
      " FOREIGN KEY (`execution`) REFERENCES `teacup_report`.`execution` (`id`)";
  private static final String EXECUTION_INT = "`execution` INT UNSIGNED NOT NULL,";
  private static final String GENERATED_ID_ERROR = "Could not retrieve the generated ID";
  private static final String ID = "`id` INT UNSIGNED NOT NULL AUTO_INCREMENT,";
  private static final String ID_ERROR =
      "{0} {1} but was not expected to do so. This might be because {2}";
  private static final String LEVEL_ENUM =
      "`level` ENUM('config', 'fine', 'finer', 'finest', 'info', 'severe', 'warning') NOT NULL,";
  private static final String LOG =
      "the session terminated before the node {1}, or the session was never initialized.";
  private static final Logger LOGGER = Logger.getLogger(DefaultReporter.class.getName());
  private static final String MESSAGE_TEXT = "`message` TEXT NOT NULL,";
  private static final String MYSQL_PROPERTY = "reporter.mysql.";
  private static final String NO_ACTION = " ON DELETE NO ACTION ON UPDATE NO ACTION";
  private static final Map<Level, Integer> ORDINALS = new HashMap<>(7);
  private static final String PRIMARY_KEY = "PRIMARY KEY (`id`),";
  private static final Properties PROPERTIES = Factory.getProperties();
  private static final String SESSION_EXECUTION_FK =
      " FOREIGN KEY (`session_execution`) REFERENCES `teacup_report`.`session_execution` (`id`)";
  private static final String TIME_TIMESTAMP = "`time` TIMESTAMP(3) NOT NULL,";
  private static final String UNIQUE_INDEX_EXECUTION =
      "UNIQUE INDEX `execution_UNIQUE` (`execution` ASC) VISIBLE,";
  private static final String UNIQUE_INDEX_ID = "UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE";

  static {
    ORDINALS.put(Level.CONFIG, 1);
    ORDINALS.put(Level.FINE, 2);
    ORDINALS.put(Level.FINER, 3);
    ORDINALS.put(Level.FINEST, 4);
    ORDINALS.put(Level.INFO, 5);
    ORDINALS.put(Level.SEVERE, 6);
    ORDINALS.put(Level.WARNING, 7);
  }

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
      var id = map.remove(node);

      if (id == null)
        LOGGER.log(
            Level.WARNING,
            ID_ERROR,
            new Object[] {
              node.getName(),
              "finished",
              "it has already finished or was never initialized. or the session terminated before "
                  + "the node finished."
            });
      else
        try (var connection = dataSource.getConnection()) {
          updateResult(connection, id, node, result);
        } catch (SQLException e) {
          LOGGER.log(Level.WARNING, "Could not update the result", e);
        }
    } else LOGGER.log(Level.WARNING, ID_ERROR, new Object[] {node.getName(), "finished", LOG});
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
      createReason(connection);
      createResult(connection);
      createError(connection);

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
        LOGGER.log(Level.WARNING, "Could not establish a connection to the database", e);
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
        LOGGER.log(Level.SEVERE, "Could not insert the log", e);
      }
  }

  @Override
  public void skipped(Node node, String reason) {
    LOGGER.log(Level.INFO, "Skipped");

    if (sessionId > 0) {
      var id = map.remove(node);

      if (id == null)
        LOGGER.log(
            Level.WARNING,
            ID_ERROR,
            new Object[] {
              node.getName(), "skipped", "it has already skipped or was never initialized"
            });
      else insertSkipped(id, reason);
    } else LOGGER.log(Level.WARNING, ID_ERROR, new Object[] {node.getName(), "skipped", LOG});
  }

  @Override
  public void started(Node node) {
    LOGGER.log(Level.FINE, "Started");

    if (sessionId > 0) {
      var id = map.get(node);

      if (id == null)
        LOGGER.log(
            Level.WARNING,
            ID_ERROR,
            new Object[] {node.getName(), "started", "it was never initialized"});
      else
        try (var connection = dataSource.getConnection();
            var preparedStatement =
                connection.prepareStatement(
                    "UPDATE `teacup_report`.`result` SET started = ? WHERE id = ?")) {
          preparedStatement.setTimestamp(1, new Timestamp(node.getTimeStarted()));
          preparedStatement.setInt(2, id);

          preparedStatement.execute();
        } catch (SQLException e) {
          LOGGER.log(Level.WARNING, "Could not update result", e);
        }
    } else LOGGER.log(Level.WARNING, ID_ERROR, new Object[] {node.getName(), "started", LOG});
  }

  @Override
  public void terminated() {
    LOGGER.log(Level.FINE, "Terminated");

    if (sessionId > 0) {
      var id = sessionId;

      map.clear();
      sessionId = 0;

      try (var connection = dataSource.getConnection();
          var preparedStatement =
              connection.prepareStatement(
                  "UPDATE `teacup_report`.`session_execution` SET terminated_time = ? WHERE id = ?")) {
        preparedStatement.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        preparedStatement.setInt(2, id);

        preparedStatement.execute();
      } catch (SQLException e) {
        LOGGER.log(Level.WARNING, "Could not terminate the session", e);
      }
    }
  }

  private static void createError(Connection connection) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.execute(
          CREATE_TABLE
              + "`teacup_report`.`error` ("
              + ID
              + MESSAGE_TEXT
              + "  `result` INT UNSIGNED NOT NULL,"
              + PRIMARY_KEY
              + UNIQUE_INDEX_ID
              + ','
              + "  UNIQUE INDEX `result_UNIQUE` (`result` ASC) VISIBLE,"
              + "  CONSTRAINT `error_result`"
              + "    FOREIGN KEY (`result`)"
              + "    REFERENCES `teacup_report`.`result` (`id`)"
              + NO_ACTION
              + ");");
    }
  }

  private static void createExecution(Connection connection) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.execute(
          CREATE_TABLE
              + "`teacup_report`.`execution` ("
              + ID
              + "  `node` INT UNSIGNED NOT NULL,"
              + "  `session_execution` INT UNSIGNED NOT NULL,"
              + PRIMARY_KEY
              + UNIQUE_INDEX_ID
              + ','
              + "  INDEX `execution.nod_idx` (`node` ASC) VISIBLE,"
              + "  INDEX `execution.session_execution_idx` (`session_execution` ASC) VISIBLE,"
              + "  CONSTRAINT `execution.node`"
              + "    FOREIGN KEY (`node`)"
              + "    REFERENCES `teacup_report`.`node` (`id`)"
              + NO_ACTION
              + ','
              + "  CONSTRAINT `execution.session_execution`"
              + SESSION_EXECUTION_FK
              + NO_ACTION
              + ");");
    }
  }

  private static void createLog(Connection connection) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.execute(
          CREATE_TABLE
              + "`teacup_report`.`log` ("
              + EXECUTION_INT
              + ID
              + LEVEL_ENUM
              + MESSAGE_TEXT
              + TIME_TIMESTAMP
              + PRIMARY_KEY
              + UNIQUE_INDEX_ID
              + ','
              + "  INDEX `log.execution_idx` (`execution` ASC) VISIBLE,"
              + "  CONSTRAINT `log.execution`"
              + EXECUTION_FK
              + NO_ACTION
              + ");");
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
          CREATE_TABLE
              + "`teacup_report`.`node` ("
              + ID
              + "  `name` VARCHAR(255) NOT NULL,"
              + PRIMARY_KEY
              + UNIQUE_INDEX_ID
              + ','
              + "  UNIQUE INDEX `name_UNIQUE` (`name` ASC) VISIBLE);");
    }
  }

  private static void createReason(Connection connection) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.execute(
          CREATE_TABLE
              + "`teacup_report`.`reason` ("
              + ID
              + "  `reason` TEXT NOT NULL,"
              + "  `skipped` INT UNSIGNED NOT NULL,"
              + PRIMARY_KEY
              + UNIQUE_INDEX_ID
              + ','
              + "  UNIQUE INDEX `skipped_UNIQUE` (`skipped` ASC) VISIBLE,"
              + "  CONSTRAINT `reason.skipped`"
              + "    FOREIGN KEY (`skipped`)"
              + "    REFERENCES `teacup_report`.`skipped` (`id`)"
              + NO_ACTION
              + ");");
    }
  }

  private static void createResult(Connection connection) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.execute(
          CREATE_TABLE
              + "`teacup_report`.`result` ("
              + EXECUTION_INT
              + "  `finished` TIMESTAMP(3) NULL,"
              + ID
              + "  `started` TIMESTAMP(3) NULL,"
              + "  `status` ENUM('aborted', 'failed', 'successful') NULL,"
              + PRIMARY_KEY
              + UNIQUE_INDEX_ID
              + ','
              + UNIQUE_INDEX_EXECUTION
              + "  CONSTRAINT `result.execution`"
              + EXECUTION_FK
              + NO_ACTION
              + ");");
    }
  }

  private static void createSchema(Connection connection) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.execute("CREATE SCHEMA IF NOT EXISTS teacup_report");
    }
  }

  private static void createSessionExecution(Connection connection) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.execute(
          CREATE_TABLE
              + "`teacup_report`.`session_execution` ("
              + ID
              + "  `initialized` TIMESTAMP(3) NOT NULL DEFAULT NOW(3),"
              + "  `terminated_time` TIMESTAMP(3) NULL,"
              + PRIMARY_KEY
              + UNIQUE_INDEX_ID
              + ");");
    }
  }

  private static void createSessionLog(Connection connection) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.execute(
          CREATE_TABLE
              + "`teacup_report`.`session_log` ("
              + ID
              + LEVEL_ENUM
              + MESSAGE_TEXT
              + "  `session_execution` INT UNSIGNED NOT NULL,"
              + TIME_TIMESTAMP
              + PRIMARY_KEY
              + UNIQUE_INDEX_ID
              + ','
              + "  INDEX `session_log.session_execution_idx` (`session_execution` ASC) VISIBLE,"
              + "  CONSTRAINT `session_log.session_execution`"
              + SESSION_EXECUTION_FK
              + NO_ACTION
              + ");");
    }
  }

  private static void createSkipped(Connection connection) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.execute(
          CREATE_TABLE
              + "`teacup_report`.`skipped` ("
              + EXECUTION_INT
              + ID
              + PRIMARY_KEY
              + UNIQUE_INDEX_ID
              + ','
              + UNIQUE_INDEX_EXECUTION
              + "  CONSTRAINT `skipped.execution`"
              + EXECUTION_FK
              + NO_ACTION
              + ");");
    }
  }

  private static void executeLogStatement(
      int id, LogRecord logRecord, PreparedStatement preparedStatement) throws SQLException {
    preparedStatement.setInt(1, id);
    preparedStatement.setInt(2, ORDINALS.get(logRecord.getLevel()));
    preparedStatement.setString(3, new SimpleFormatter().formatMessage(logRecord));
    preparedStatement.setTimestamp(4, new Timestamp(logRecord.getMillis()));

    preparedStatement.execute();
  }

  private static Optional<Integer> getId(Statement statement) {
    Integer id = null;

    try (var resultSet = statement.getGeneratedKeys()) {
      if (resultSet.next()) id = resultSet.getInt(1);
      else LOGGER.log(Level.WARNING, GENERATED_ID_ERROR);
    } catch (SQLException e) {
      LOGGER.log(Level.WARNING, GENERATED_ID_ERROR, e);
    }

    return Optional.ofNullable(id);
  }

  private static void insertError(Connection connection, Integer id, Throwable throwable) {
    try (var preparedStatement =
        connection.prepareStatement(
            "INSERT INTO `teacup_report`.`error`(message, result) VALUES(?, ?)")) {
      preparedStatement.setString(1, throwable.getMessage());
      preparedStatement.setInt(2, id);

      preparedStatement.execute();
    } catch (SQLException e) {
      LOGGER.log(Level.WARNING, "Could not insert the error", e);
    }
  }

  private void insertExecution(
      Connection connection, Node node, PreparedStatement preparedStatement) throws SQLException {
    try (var resultSet = preparedStatement.executeQuery()) {
      insertExecution(
          connection, resultSet.next() ? resultSet.getInt(1) : insertNode(connection, node), node);
    }
  }

  private void insertExecution(Connection connection, int id, Node node) throws SQLException {
    if (id > 0)
      try (var preparedStatement =
          connection.prepareStatement(
              "INSERT INTO `teacup_report`.`execution`(`node`, `session_execution`) VALUES(?, ?)",
              Statement.RETURN_GENERATED_KEYS)) {
        preparedStatement.setInt(1, id);
        preparedStatement.setInt(2, sessionId);

        preparedStatement.execute();

        getId(preparedStatement)
            .ifPresent(executionId -> insertResult(connection, executionId, node));
      }
  }

  private void insertExecutions(Connection connection, Iterable<? extends Node> nodes) {
    for (var node : nodes) {
      try (var preparedStatement =
          connection.prepareStatement("SELECT id FROM `teacup_report`.`node` WHERE name = ?")) {
        preparedStatement.setString(1, node.getName());

        insertExecution(connection, node, preparedStatement);
      } catch (SQLException e) {
        LOGGER.log(Level.WARNING, "Could not insert the execution", e);
      }

      insertExecutions(connection, node.getNodes());
    }
  }

  private static void insertLog(Connection connection, Integer id, LogRecord logRecord)
      throws SQLException {
    try (var preparedStatement =
        connection.prepareStatement(
            "INSERT INTO `teacup_report`.`log`(execution, level, message, time) VALUES(?, ?, ?, ?)")) {
      executeLogStatement(id, logRecord, preparedStatement);
    }
  }

  private static int insertNode(Connection connection, Node node) throws SQLException {
    try (var preparedStatement =
        connection.prepareStatement(
            "INSERT INTO `teacup_report`.`node` SET name = ?", Statement.RETURN_GENERATED_KEYS)) {
      preparedStatement.setString(1, node.getName());
      preparedStatement.execute();

      return getId(preparedStatement).orElse(0);
    }
  }

  private static void insertReason(Connection connection, int id, String reason) {
    try (var prep =
        connection.prepareStatement(
            "INSERT INTO `teacup_report`.`reason`(reason, skipped) VALUES(?, ?)")) {
      prep.setString(1, reason);
      prep.setInt(2, id);

      prep.execute();
    } catch (SQLException e) {
      LOGGER.log(Level.WARNING, "Could not insert reason", e);
    }
  }

  private void insertResult(Connection connection, int id, Node node) {
    map.put(node, id);

    try (var preparedStatement =
        connection.prepareStatement("INSERT INTO `teacup_report`.`result` SET execution = ?")) {
      preparedStatement.setInt(1, id);

      preparedStatement.execute();
    } catch (SQLException e) {
      LOGGER.log(Level.WARNING, "Could not insert result", e);
    }
  }

  private void insertSessionExecution(Connection connection) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.execute(
          "INSERT INTO `teacup_report`.`session_execution`() VALUES()",
          Statement.RETURN_GENERATED_KEYS);

      getId(statement).ifPresent(id -> sessionId = id);
    }
  }

  private void insertSessionLog(Connection connection, LogRecord logRecord) throws SQLException {
    try (var preparedStatement =
        connection.prepareStatement(
            "INSERT INTO `teacup_report`.`session_log`(session_execution, level, message, time) VALUES(?, ? ,? , ?)")) {
      executeLogStatement(sessionId, logRecord, preparedStatement);
    }
  }

  private void insertSkipped(int id, String reason) {
    try (var connection = dataSource.getConnection();
        var preparedStatement =
            connection.prepareStatement(
                "INSERT INTO `teacup_report`.`skipped` SET execution = ?",
                Statement.RETURN_GENERATED_KEYS)) {
      preparedStatement.setInt(1, id);
      preparedStatement.execute();

      if (reason != null)
        getId(preparedStatement)
            .ifPresent(skippedId -> insertReason(connection, skippedId, reason));
    } catch (SQLException e) {
      LOGGER.log(Level.WARNING, "Could not insert skipped", e);
    }
  }

  private static void updateResult(Connection connection, int id, Node node, Result result)
      throws SQLException {
    try (var preparedStatement =
        connection.prepareStatement(
            "UPDATE `teacup_report`.`result` SET finished = ?, status = ? WHERE id = ?")) {
      preparedStatement.setTimestamp(1, new Timestamp(node.getTimeFinished()));
      preparedStatement.setInt(2, result.getStatus().ordinal() + 1);
      preparedStatement.setInt(3, id);

      preparedStatement.execute();

      result.getThrowable().ifPresent(throwable -> insertError(connection, id, throwable));
    }
  }
}
