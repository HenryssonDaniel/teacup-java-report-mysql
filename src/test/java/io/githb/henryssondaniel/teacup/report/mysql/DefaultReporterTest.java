package io.githb.henryssondaniel.teacup.report.mysql;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
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
  void finishedWhenConnectionError() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.initialized(Collections.singletonList(node));

    try (var conn = dataSource.getConnection()) {
      when(conn).thenThrow(new SQLException(TEST));
    }

    reporter.finished(node, result);

    verify(node).getName();
    verify(node).getNodes();
    verify(node, times(0)).getTimeFinished();
    verifyZeroInteractions(result);
  }

  @Test
  void finishedWhenInsertResultError() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.initialized(Collections.singletonList(node));

    try (var connectionStatement = connection.prepareStatement(anyString())) {
      when(connectionStatement).thenThrow(new SQLException(TEST));
    }

    reporter.finished(node, result);

    verify(node, times(3)).getName();
    verify(node).getNodes();
    verify(node, times(0)).getTimeFinished();
    verifyZeroInteractions(result);
  }

  @Test
  void finishedWhenNoId() {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.finished(node, result);

    verify(node).getName();
    verify(node, times(0)).getNodes();
    verify(node, times(0)).getTimeFinished();
    verifyZeroInteractions(result);
  }

  @Test
  void finishedWhenNoSessionId() {
    new DefaultReporter().finished(node, result);

    verifyZeroInteractions(node);
    verifyZeroInteractions(result);
  }

  @Test
  void finishedWhenUpdateFinishedError() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.initialized(Collections.singletonList(node));

    try (var connectionStatement = connection.prepareStatement(anyString())) {
      when(connectionStatement).thenReturn(preparedStatement).thenThrow(new SQLException(TEST));
    }

    reporter.finished(node, result);

    verify(node, times(2)).getName();
    verify(node).getNodes();
    verify(node, times(0)).getTimeFinished();
    verify(result).getStatus();
    verify(result).getThrowable();
  }

  @Test
  void initialize() throws SQLException {
    new DefaultReporter(dataSource).initialize();

    verify(connection, times(9)).createStatement();
    verify(dataSource).getConnection();
    verify(statement, times(9)).close();
    verify(statement, times(8)).execute(anyString());
    verify(statement).executeUpdate(anyString(), same(Statement.RETURN_GENERATED_KEYS));
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
  void initializeWhenSessionIdError() throws SQLException {
    when(resultSet.next()).thenReturn(false);

    new DefaultReporter(dataSource).initialize();

    verify(connection, times(9)).createStatement();
    verify(dataSource).getConnection();
    verify(statement, times(9)).close();
    verify(statement, times(8)).execute(anyString());
    verify(statement).executeUpdate(anyString(), same(Statement.RETURN_GENERATED_KEYS));
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
    verifyPrepareStatement();
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
    verifyZeroInteractions(node);
    verifyZeroInteractions(preparedStatement);
  }

  @Test
  void initializedWhenExecutionIdError() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();

    when(resultSet.next()).thenReturn(true, false);

    reporter.initialized(Collections.singletonList(node));

    verify(dataSource, times(2)).getConnection();
    verify(node).getName();
    verify(node).getNodes();
    verifyPrepareStatement();
  }

  @Test
  void initializedWhenInsertExecutionsError() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();

    try (var connectionStatement = connection.prepareStatement(anyString())) {
      when(connectionStatement).thenThrow(new SQLException(TEST));
    }

    reporter.initialized(Collections.singletonList(node));

    verify(dataSource, times(2)).getConnection();
    verify(node).getNodes();
    verify(preparedStatement).close();
    verify(preparedStatement, never()).execute();
    verify(preparedStatement, never()).executeQuery();
    verify(preparedStatement, never()).getGeneratedKeys();
    verify(preparedStatement, never()).setInt(1, 1);
    verify(preparedStatement, never()).setInt(2, 1);
    verify(preparedStatement, never()).setString(1, null);
  }

  @Test
  void initializedWhenNoNodes() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.initialized(Collections.emptyList());

    verify(dataSource).getConnection();
    verifyZeroInteractions(node);
    verifyZeroInteractions(preparedStatement);
  }

  @Test
  void initializedWhenNoSessionId() {
    new DefaultReporter(dataSource).initialized(null);

    verifyZeroInteractions(dataSource);
    verifyZeroInteractions(preparedStatement);
  }

  @Test
  void initializedWhenNodeId() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();

    when(resultSet.next()).thenReturn(false, true);

    reporter.initialized(Collections.singletonList(node));

    verify(dataSource, times(2)).getConnection();
    verify(node).getNodes();
    verifyPrepareStatement(1);
  }

  @Test
  void initializedWhenNodeIdError() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();

    when(resultSet.next()).thenReturn(false);

    reporter.initialized(Collections.singletonList(node));

    verify(dataSource, times(2)).getConnection();
    verify(node).getNodes();
    verifyPrepareStatement(0);
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
    verifyZeroInteractions(logRecord);
    verifyZeroInteractions(node);
    verifyZeroInteractions(preparedStatement);
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

    verifyZeroInteractions(node);
  }

  @Test
  void logWhenNoSessionId() {
    new DefaultReporter(dataSource).log(logRecord, node);
    verifyZeroInteractions(dataSource);
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

    verify(connection, times(2)).prepareStatement(anyString());
    verify(dataSource, times(3)).getConnection();
    verify(node).getName();
    verify(preparedStatement, times(2)).execute();
    verify(preparedStatement, times(2)).setInt(1, 1);
    verify(preparedStatement).setString(2, REASON);
  }

  @Test
  void skippedWhenConnectionError() throws SQLException {
    Reporter reporter = new DefaultReporter(dataSource);
    reporter.initialize();
    reporter.initialized(Collections.singletonList(node));

    when(dataSource.getConnection()).thenThrow(new SQLException(TEST));

    reporter.skipped(node, REASON);

    verify(connection).prepareStatement(anyString());
    verify(dataSource, times(3)).getConnection();
    verify(node, times(2)).getName();
    verify(preparedStatement).execute();
    verify(preparedStatement).setInt(1, 1);
    verify(preparedStatement, never()).setString(2, REASON);
  }

  @Test
  void skippedWhenNoSessionId() {
    new DefaultReporter(dataSource).skipped(node, REASON);

    verifyZeroInteractions(dataSource);
    verifyZeroInteractions(node);
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

    verify(node, times(2)).getName();
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
    verifyZeroInteractions(node);
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
    verifyZeroInteractions(dataSource);
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

  private void verifyPrepareStatement() throws SQLException {
    verify(preparedStatement, times(2)).close();
    verify(preparedStatement).execute();
    verify(preparedStatement).executeQuery();
    verify(preparedStatement).getGeneratedKeys();
    verify(preparedStatement).setInt(1, 1);
    verify(preparedStatement).setInt(2, 1);
    verify(preparedStatement).setString(1, null);
  }

  private void verifyPrepareStatement(int id) throws SQLException {
    verify(preparedStatement, times(3)).close();
    verify(preparedStatement, times(2)).execute();
    verify(preparedStatement).executeQuery();
    verify(preparedStatement, times(2)).getGeneratedKeys();
    verify(preparedStatement).setInt(1, id);
    verify(preparedStatement).setInt(2, 1);
    verify(preparedStatement, times(2)).setString(1, null);
  }
}
