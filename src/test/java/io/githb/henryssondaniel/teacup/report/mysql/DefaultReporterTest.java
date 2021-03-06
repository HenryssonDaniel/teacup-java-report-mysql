package io.githb.henryssondaniel.teacup.report.mysql;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.github.henryssondaniel.teacup.core.reporting.Reporter;
import io.github.henryssondaniel.teacup.core.testing.Node;
import io.github.henryssondaniel.teacup.core.testing.Result;
import io.github.henryssondaniel.teacup.core.testing.Status;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

class DefaultReporterTest {
  private static final String REASON = "reason";
  private static final String TEST = "test";

  private final Connection connection = mock(Connection.class);
  private final DataSource dataSource = mock(DataSource.class);
  private final LogRecord logRecord = mock(LogRecord.class);
  private final Node node = mock(Node.class);
  private final PreparedStatement preparedStatement = mock(PreparedStatement.class);
  private final Result result = mock(Result.class);
  private final ResultSet resultSet = mock(ResultSet.class);
  private final Statement statement = mock(Statement.class);

  @BeforeEach
  void beforeEach() throws SQLException {
    MockitoAnnotations.initMocks(this);

    try (var conn = dataSource.getConnection()) {
      when(conn).thenReturn(connection);
    }

    when(result.getStatus()).thenReturn(Status.SUCCESSFUL);
    when(result.getThrowable()).thenReturn(Optional.of(new SQLException(TEST)));

    try (var generatedKeys = statement.getGeneratedKeys()) {
      when(generatedKeys).thenReturn(resultSet);
    }

    setupConnection();
    setupPreparedStatement();
    setupResultSet();
  }

  @Test
  void finished() {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.initialized(Collections.singletonList(node));
    reporter.finished(node, result);

    verify(node).getName();
    verify(node).getNodes();
    verify(node).getTimeFinished();
    verify(result).getStatus();
    verify(result).getThrowable();
  }

  @Test
  void finishedWhenInsertErrorError() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.initialized(Collections.singletonList(node));

    try (var connectionStatement =
        connection.prepareStatement(
            "INSERT INTO `teacup_report`.`error`(message, result) VALUES(?, ?)")) {
      when(connectionStatement).thenThrow(new SQLException(TEST));
    }

    reporter.finished(node, result);

    verify(node).getName();
    verify(node).getNodes();
    verify(node).getTimeFinished();
    verify(result).getStatus();
    verify(result).getThrowable();
  }

  @Test
  void finishedWhenNoId() {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.finished(node, result);

    verify(node).getName();
    verify(node, times(0)).getNodes();
    verify(node, times(0)).getTimeFinished();
    verifyNoInteractions(result);
  }

  @Test
  void finishedWhenNoSessionId() {
    new DefaultReporter().finished(node, result);

    verify(node).getName();
    verify(node, times(0)).getNodes();
    verify(node, times(0)).getTimeFinished();
    verifyNoInteractions(result);
  }

  @Test
  void finishedWhenUpdateResultError() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.initialized(Collections.singletonList(node));

    try (var connectionStatement = connection.prepareStatement(anyString())) {
      when(connectionStatement).thenThrow(new SQLException(TEST));
    }

    reporter.finished(node, result);

    verify(node).getName();
    verify(node).getNodes();
    verify(node, times(0)).getTimeFinished();
    verifyNoInteractions(result);
  }

  @Test
  void initialize() throws SQLException {
    new DefaultReporter(dataSource).initialize();

    verify(connection, times(11)).createStatement();
    verify(dataSource).getConnection();
    verify(statement, times(11)).close();
    verify(statement, times(10)).execute(anyString());
    verify(statement).execute(anyString(), same(Statement.RETURN_GENERATED_KEYS));
    verify(statement).getGeneratedKeys();
  }

  @Test
  void initializeWhenConnectionError() throws SQLException {
    try (var conn = dataSource.getConnection()) {
      when(conn).thenThrow(new SQLException(TEST));
    }

    new DefaultReporter(dataSource).initialize();

    verify(connection).close();
    verify(dataSource).getConnection();
  }

  @Test
  void initializeWhenNoKey() throws SQLException {
    when(resultSet.next()).thenReturn(false);

    new DefaultReporter(dataSource).initialize();

    verify(connection, times(11)).createStatement();
    verify(dataSource).getConnection();
    verify(statement, times(11)).close();
    verify(statement, times(10)).execute(anyString());
    verify(statement).execute(anyString(), same(Statement.RETURN_GENERATED_KEYS));
    verify(statement).getGeneratedKeys();
  }

  @Test
  void initializeWhenSessionIdError() throws SQLException {
    try (var generatedKeys = statement.getGeneratedKeys()) {
      when(generatedKeys).thenThrow(new SQLException(TEST));
    }

    new DefaultReporter(dataSource).initialize();

    verify(connection, times(11)).createStatement();
    verify(dataSource).getConnection();
    verify(statement, times(11)).close();
    verify(statement, times(10)).execute(anyString());
    verify(statement).execute(anyString(), same(Statement.RETURN_GENERATED_KEYS));
    verify(statement).getGeneratedKeys();
  }

  @Test
  void initialized() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();

    reporter.initialized(Collections.singletonList(node));

    verify(dataSource, times(2)).getConnection();
    verify(node).getName();
    verify(node).getNodes();
    verify(preparedStatement, times(3)).close();
    verify(preparedStatement, times(2)).execute();
    verify(preparedStatement).executeQuery();
    verify(preparedStatement).getGeneratedKeys();
    verify(preparedStatement, times(2)).setInt(1, 1);
    verify(preparedStatement).setInt(2, 1);
    verify(preparedStatement).setString(1, null);
  }

  @Test
  void initializedWhenConnectionError() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();

    try (var conn = dataSource.getConnection()) {
      when(conn).thenThrow(new SQLException(TEST));
    }

    reporter.initialized(Collections.singletonList(node));

    verify(dataSource, times(2)).getConnection();
    verifyNoInteractions(node);
    verifyNoInteractions(preparedStatement);
  }

  @Test
  void initializedWhenExecutionIdError() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();

    when(preparedStatement.getGeneratedKeys()).thenThrow(new SQLException(TEST));

    reporter.initialized(Collections.singletonList(node));

    verify(dataSource, times(2)).getConnection();
    verify(node).getName();
    verify(node).getNodes();
    verify(preparedStatement, times(2)).close();
    verify(preparedStatement).execute();
    verify(preparedStatement).executeQuery();
    verify(preparedStatement).getGeneratedKeys();
    verify(preparedStatement).setInt(1, 1);
    verify(preparedStatement).setInt(2, 1);
    verify(preparedStatement).setString(1, null);
  }

  @Test
  void initializedWhenInsertExecutionError() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();

    try (var connectionStatement =
        connection.prepareStatement(anyString(), same(Statement.RETURN_GENERATED_KEYS))) {
      when(connectionStatement).thenThrow(new SQLException(TEST));
    }

    reporter.initialized(Collections.singletonList(node));

    verify(dataSource, times(2)).getConnection();
    verify(node).getNodes();
    verify(preparedStatement).close();
    verify(preparedStatement, never()).execute();
    verify(preparedStatement).executeQuery();
    verify(preparedStatement, never()).getGeneratedKeys();
    verify(preparedStatement, never()).setInt(1, 1);
    verify(preparedStatement, never()).setInt(2, 1);
    verify(preparedStatement).setString(1, null);
  }

  @Test
  void initializedWhenInsertNodeIdError() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();

    try (var generatedKeys = preparedStatement.getGeneratedKeys()) {
      when(generatedKeys).thenThrow(new SQLException(TEST));
    }

    when(resultSet.next()).thenReturn(false);

    reporter.initialized(Collections.singletonList(node));

    verify(dataSource, times(2)).getConnection();
    verify(node, times(2)).getName();
    verify(node).getNodes();
    verify(preparedStatement, times(2)).close();
    verify(preparedStatement).execute();
    verify(preparedStatement).executeQuery();
    verify(preparedStatement).getGeneratedKeys();
    verify(preparedStatement, never()).setInt(1, 0);
    verify(preparedStatement, never()).setInt(2, 1);
    verify(preparedStatement, times(2)).setString(1, null);
  }

  @Test
  void initializedWhenInsertNodeNoExecutionId() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();

    when(resultSet.next()).thenReturn(false, true, false);

    reporter.initialized(Collections.singletonList(node));

    verify(dataSource, times(2)).getConnection();
    verify(node, times(2)).getName();
    verify(node).getNodes();
    verify(preparedStatement, times(3)).close();
    verify(preparedStatement, times(2)).execute();
    verify(preparedStatement).executeQuery();
    verify(preparedStatement, times(2)).getGeneratedKeys();
    verify(preparedStatement).setInt(1, 1);
    verify(preparedStatement).setInt(2, 1);
    verify(preparedStatement, times(2)).setString(1, null);
  }

  @Test
  void initializedWhenInsertNodeNoId() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();

    when(resultSet.next()).thenReturn(false);

    reporter.initialized(Collections.singletonList(node));

    verify(dataSource, times(2)).getConnection();
    verify(node, times(2)).getName();
    verify(node).getNodes();
    verify(preparedStatement, times(2)).close();
    verify(preparedStatement).execute();
    verify(preparedStatement).executeQuery();
    verify(preparedStatement).getGeneratedKeys();
    verify(preparedStatement, never()).setInt(1, 0);
    verify(preparedStatement, never()).setInt(2, 1);
    verify(preparedStatement, times(2)).setString(1, null);
  }

  @Test
  void initializedWhenInsertResultError() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();

    try (var state = connection.prepareStatement(anyString())) {
      when(state).thenReturn(preparedStatement).thenThrow(new SQLException(TEST));
    }

    reporter.initialized(Collections.singletonList(node));

    verify(preparedStatement, times(3)).close();
    verify(preparedStatement).execute();
    verify(preparedStatement).executeQuery();
    verify(preparedStatement).getGeneratedKeys();
    verify(preparedStatement).setInt(1, 1);
    verify(preparedStatement).setInt(2, 1);
    verify(preparedStatement).setString(1, null);
  }

  @Test
  void initializedWhenNoNodes() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.initialized(Collections.emptyList());

    verify(dataSource).getConnection();
    verifyNoInteractions(node);
    verifyNoInteractions(preparedStatement);
  }

  @Test
  void initializedWhenNoSessionId() {
    new DefaultReporter(dataSource).initialized(null);

    verifyNoInteractions(dataSource);
    verifyNoInteractions(preparedStatement);
  }

  @Test
  void logWhenConfig() throws SQLException {
    when(logRecord.getLevel()).thenReturn(Level.CONFIG);

    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.initialized(Collections.singletonList(node));
    reporter.log(logRecord, node);

    verify(dataSource, times(3)).getConnection();
    verifyLogRecord();
    verify(node).getName();
    verify(node).getNodes();
  }

  @Test
  void logWhenConnectionError() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();

    try (var conn = dataSource.getConnection()) {
      when(conn).thenThrow(new SQLException(TEST));
    }

    reporter.log(logRecord, node);

    verify(dataSource, times(2)).getConnection();
    verifyNoInteractions(logRecord);
    verifyNoInteractions(node);
    verifyNoInteractions(preparedStatement);
  }

  @Test
  void logWhenFine() throws SQLException {
    when(logRecord.getLevel()).thenReturn(Level.FINE);

    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.initialized(Collections.singletonList(node));
    reporter.log(logRecord, node);

    verify(dataSource, times(3)).getConnection();
    verifyLogRecord();
    verify(node).getName();
    verify(node).getNodes();
  }

  @Test
  void logWhenFiner() throws SQLException {
    when(logRecord.getLevel()).thenReturn(Level.FINER);

    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.initialized(Collections.singletonList(node));
    reporter.log(logRecord, node);

    verify(dataSource, times(3)).getConnection();
    verifyLogRecord();
    verify(node).getName();
    verify(node).getNodes();
  }

  @Test
  void logWhenFinest() throws SQLException {
    when(logRecord.getLevel()).thenReturn(Level.FINEST);

    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.initialized(Collections.singletonList(node));
    reporter.log(logRecord, node);

    verify(dataSource, times(3)).getConnection();
    verifyLogRecord();
    verify(node).getName();
    verify(node).getNodes();
  }

  @Test
  void logWhenInfo() throws SQLException {
    when(logRecord.getLevel()).thenReturn(Level.INFO);

    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.initialized(Collections.singletonList(node));
    reporter.log(logRecord, node);

    verify(dataSource, times(3)).getConnection();
    verifyLogRecord();
    verify(node).getName();
    verify(node).getNodes();
  }

  @Test
  void logWhenNoNodeId() throws SQLException {
    when(logRecord.getLevel()).thenReturn(Level.WARNING);

    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.log(logRecord, node);

    verify(dataSource, times(2)).getConnection();
    verifyLogRecord();

    verifyNoInteractions(node);
  }

  @Test
  void logWhenNoSessionId() {
    new DefaultReporter(dataSource).log(logRecord, node);
    verifyNoInteractions(dataSource);
  }

  @Test
  void logWhenSevere() throws SQLException {
    when(logRecord.getLevel()).thenReturn(Level.SEVERE);

    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.initialized(Collections.singletonList(node));
    reporter.log(logRecord, node);

    verify(dataSource, times(3)).getConnection();
    verifyLogRecord();
    verify(node).getName();
    verify(node).getNodes();
  }

  @Test
  void skipped() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.initialized(Collections.singletonList(node));
    reporter.skipped(node, REASON);

    verify(connection, times(3)).prepareStatement(anyString());
    verify(dataSource, times(3)).getConnection();
    verify(node).getName();
    verify(preparedStatement, times(4)).execute();
    verify(preparedStatement, times(3)).setInt(1, 1);
    verify(preparedStatement).setString(1, null);
  }

  @Test
  void skippedIdError() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.initialized(Collections.singletonList(node));

    try (var generatedKeys = preparedStatement.getGeneratedKeys()) {
      when(generatedKeys).thenThrow(new SQLException(TEST));
    }

    reporter.skipped(node, REASON);

    verify(connection, times(2)).prepareStatement(anyString());
    verify(dataSource, times(3)).getConnection();
    verify(node).getName();
    verify(preparedStatement, times(3)).execute();
    verify(preparedStatement, times(3)).setInt(1, 1);
    verify(preparedStatement).setString(1, null);
  }

  @Test
  void skippedMissingId() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.initialized(Collections.singletonList(node));

    when(resultSet.next()).thenReturn(false);

    reporter.skipped(node, REASON);

    verify(connection, times(2)).prepareStatement(anyString());
    verify(dataSource, times(3)).getConnection();
    verify(node).getName();
    verify(preparedStatement, times(3)).execute();
    verify(preparedStatement, times(3)).setInt(1, 1);
    verify(preparedStatement).setString(1, null);
  }

  @Test
  void skippedWhenInsertSkippedError() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.initialized(Collections.singletonList(node));

    try (var conn = dataSource.getConnection()) {
      when(conn).thenThrow(new SQLException(TEST));
    }

    reporter.skipped(node, REASON);

    verify(connection, times(2)).prepareStatement(anyString());
    verify(dataSource, times(3)).getConnection();
    verify(node).getName();
    verify(preparedStatement, times(2)).execute();
    verify(preparedStatement, times(2)).setInt(1, 1);
    verify(preparedStatement, never()).setString(2, REASON);
  }

  @Test
  void skippedWhenNoReason() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.initialized(Collections.singletonList(node));
    reporter.skipped(node, null);

    verify(connection, times(2)).prepareStatement(anyString());
    verify(dataSource, times(3)).getConnection();
    verify(node).getName();
    verify(preparedStatement, times(3)).execute();
    verify(preparedStatement, times(3)).setInt(1, 1);
    verify(preparedStatement).setString(1, null);
  }

  @Test
  void skippedWhenNoSessionId() {
    new DefaultReporter(dataSource).skipped(node, REASON);

    verifyNoInteractions(dataSource);
    verify(node).getName();
  }

  @Test
  void skippedWhenNotInitialized() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.skipped(node, REASON);

    verify(connection, never()).prepareStatement(anyString());
    verify(dataSource).getConnection();
    verify(node).getName();
    verify(preparedStatement, never()).execute();
    verify(preparedStatement, never()).setInt(1, 1);
    verify(preparedStatement, never()).setString(2, REASON);
  }

  @Test
  void skippedWhenReasonError() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.initialized(Collections.singletonList(node));

    try (var prepare = connection.prepareStatement(anyString())) {
      when(prepare).thenThrow(new SQLException(TEST));
    }

    reporter.skipped(node, REASON);

    verify(connection, times(3)).prepareStatement(anyString());
    verify(dataSource, times(3)).getConnection();
    verify(node).getName();
    verify(preparedStatement, times(3)).execute();
    verify(preparedStatement, times(3)).setInt(1, 1);
    verify(preparedStatement).setString(1, null);
  }

  @Test
  void started() {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.initialized(Collections.singletonList(node));
    reporter.started(node);

    verify(node).getName();
    verify(node).getNodes();
    verify(node).getTimeStarted();
  }

  @Test
  void startedWhenConnectionError() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.initialized(Collections.singletonList(node));

    try (var conn = dataSource.getConnection()) {
      when(conn).thenThrow(new SQLException(TEST));
    }

    reporter.started(node);

    verify(node).getName();
    verify(node).getNodes();
    verify(node, times(0)).getTimeStarted();
  }

  @Test
  void startedWhenNoId() {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.started(node);

    verify(node).getName();
    verify(node, times(0)).getNodes();
    verify(node, times(0)).getTimeFinished();
  }

  @Test
  void startedWhenNoSessionId() {
    new DefaultReporter(dataSource).started(node);

    verify(node).getName();
    verify(node, times(0)).getNodes();
    verify(node, times(0)).getTimeFinished();
  }

  @Test
  void terminated() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();

    reporter.terminated();

    verify(dataSource, times(2)).getConnection();
    verify(connection).prepareStatement(anyString());
  }

  @Test
  void terminatedWhenConnectionError() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();

    when(dataSource.getConnection()).thenThrow(new SQLException(TEST));

    reporter.terminated();

    verify(dataSource, times(2)).getConnection();
    verify(connection, never()).prepareStatement(anyString());
  }

  @Test
  void terminatedWhenNoSessionId() {
    new DefaultReporter(dataSource).terminated();
    verifyNoInteractions(dataSource);
  }

  private void setupConnection() throws SQLException {
    try (var connectionStatement = connection.createStatement()) {
      when(connectionStatement).thenReturn(statement);
    }

    try (var connectionStatement = connection.prepareStatement(anyString())) {
      when(connectionStatement).thenReturn(preparedStatement);
    }

    try (var connectionStatement =
        connection.prepareStatement(anyString(), same(Statement.RETURN_GENERATED_KEYS))) {
      when(connectionStatement).thenReturn(preparedStatement);
    }
  }

  private void setupPreparedStatement() throws SQLException {
    try (var query = preparedStatement.executeQuery()) {
      when(query).thenReturn(resultSet);
    }

    try (var generatedKeys = preparedStatement.getGeneratedKeys()) {
      when(generatedKeys).thenReturn(resultSet);
    }
  }

  private void setupResultSet() throws SQLException {
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getInt(1)).thenReturn(1);
  }

  private void verifyLogRecord() {
    verify(logRecord).getLevel();
    verify(logRecord).getMessage();
    verify(logRecord).getParameters();
    verify(logRecord).getResourceBundle();
    verify(logRecord).getMillis();
  }
}
