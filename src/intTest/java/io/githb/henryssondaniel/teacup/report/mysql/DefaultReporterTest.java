package io.githb.henryssondaniel.teacup.report.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.henryssondaniel.teacup.core.reporting.Reporter;
import io.github.henryssondaniel.teacup.core.testing.Factory;
import io.github.henryssondaniel.teacup.core.testing.Node;
import io.github.henryssondaniel.teacup.core.testing.Result;
import io.github.henryssondaniel.teacup.core.testing.Status;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled("Needs a SQL server")
class DefaultReporterTest {
  private final Reporter defaultReporter = new DefaultReporter();
  private final Node node = Factory.createNode("name", Collections.emptyList());
  private final Result result = Factory.createResult(Status.SUCCESSFUL, new SQLException("test"));

  @BeforeEach
  void beforeEach() {
    var time = System.currentTimeMillis();

    node.setTimeFinished(time);
    node.setTimeStarted(time);
  }

  @Test
  void finished() throws IllegalAccessException, NoSuchFieldException {
    defaultReporter.initialize();
    defaultReporter.initialized(Collections.singletonList(node));
    defaultReporter.finished(node, result);

    assertThat(getMap()).isEmpty();
  }

  @Test
  void finishedWhenNoId() throws IllegalAccessException, NoSuchFieldException {
    defaultReporter.initialize();
    defaultReporter.finished(node, result);

    assertThat(getMap()).isEmpty();
  }

  @Test
  void finishedWhenNoSessionId() throws IllegalAccessException, NoSuchFieldException {
    defaultReporter.finished(node, result);
    assertThat(getMap()).isEmpty();
  }

  @Test
  void finishedWhenNoThrowable() throws IllegalAccessException, NoSuchFieldException {
    defaultReporter.initialize();
    defaultReporter.initialized(Collections.singletonList(node));
    defaultReporter.finished(node, Factory.createResult(Status.SUCCESSFUL, null));

    assertThat(getMap()).isEmpty();
  }

  @Test
  void initialize() throws IllegalAccessException, NoSuchFieldException {
    defaultReporter.initialize();
    assertThat(getMap()).isEmpty();
  }

  @Test
  void initialized() throws IllegalAccessException, NoSuchFieldException {
    defaultReporter.initialize();
    defaultReporter.initialized(Collections.singletonList(node));
    assertThat(getMap()).containsOnlyKeys(node);
  }

  @Test
  void initializedWhenNoNodes() throws IllegalAccessException, NoSuchFieldException {
    defaultReporter.initialize();
    defaultReporter.initialized(Collections.emptyList());
    assertThat(getMap()).isEmpty();
  }

  @Test
  void initializedWhenNoSessionId() throws IllegalAccessException, NoSuchFieldException {
    defaultReporter.initialized(Collections.singletonList(node));
    assertThat(getMap()).isEmpty();
  }

  @Test
  void log() throws IllegalAccessException, NoSuchFieldException {
    defaultReporter.initialize();
    defaultReporter.initialized(Collections.singletonList(node));
    defaultReporter.log(new LogRecord(Level.INFO, "test"), node);
    assertThat(getMap()).containsOnlyKeys(node);
  }

  @Test
  void logWhenNoNode() throws IllegalAccessException, NoSuchFieldException {
    defaultReporter.initialize();
    defaultReporter.log(new LogRecord(Level.INFO, "test"), null);
    assertThat(getMap()).isEmpty();
  }

  @Test
  void logWhenNoSessionId() throws IllegalAccessException, NoSuchFieldException {
    defaultReporter.log(new LogRecord(Level.INFO, "test"), node);
    assertThat(getMap()).isEmpty();
  }

  @Test
  void skipped() throws IllegalAccessException, NoSuchFieldException {
    defaultReporter.initialize();
    defaultReporter.initialized(Collections.singletonList(node));
    defaultReporter.skipped(node, "reason");
    assertThat(getMap()).isEmpty();
  }

  @Test
  void skippedWhenNoId() throws IllegalAccessException, NoSuchFieldException {
    defaultReporter.initialize();
    defaultReporter.skipped(node, null);
    assertThat(getMap()).isEmpty();
  }

  @Test
  void skippedWhenNoReason() throws IllegalAccessException, NoSuchFieldException {
    defaultReporter.initialize();
    defaultReporter.initialized(Collections.singletonList(node));
    defaultReporter.skipped(node, null);
    assertThat(getMap()).isEmpty();
  }

  @Test
  void skippedWhenNoSessionId() throws IllegalAccessException, NoSuchFieldException {
    defaultReporter.skipped(node, null);
    assertThat(getMap()).isEmpty();
  }

  @SuppressWarnings("unchecked")
  private Map<Node, Integer> getMap() throws IllegalAccessException, NoSuchFieldException {
    var field = DefaultReporter.class.getDeclaredField("map");
    field.setAccessible(true);

    return (Map<Node, Integer>) field.get(defaultReporter);
  }
}
