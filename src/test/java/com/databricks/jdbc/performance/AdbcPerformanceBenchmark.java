package com.databricks.jdbc.performance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.client.jdbc.Driver;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Performance benchmarking test suite for comparing ADBC vs non-ADBC execution performance.
 *
 * <p>This benchmark provides a simple interface where you just need to provide: - JDBC URL (without
 * ADBC parameters) - SQL Query to benchmark
 *
 * <p>The suite automatically: - Runs the query with ADBC mode enabled - Runs the same query with
 * standard JDBC mode - Measures execution time, result processing time, and memory usage - Provides
 * detailed performance comparison reports
 *
 * <p>Usage Example:
 *
 * <pre>
 * AdbcPerformanceBenchmark benchmark = new AdbcPerformanceBenchmark();
 * BenchmarkResult result = benchmark.runBenchmark(
 *     "jdbc:databricks://host:443/default;httpPath=/sql/1.0/warehouses/123;AuthMech=3;",
 *     "SELECT * FROM large_table LIMIT 10000"
 * );
 * result.printReport();
 * </pre>
 */
public class AdbcPerformanceBenchmark {

  private static final String DATABRICKS_TOKEN = System.getenv("DATABRICKS_EXAMPLE_TOKEN");
  private static final int WARMUP_RUNS = 2;
  private static final int BENCHMARK_RUNS = 5;

  @BeforeAll
  static void setup() {
    try {
      DriverManager.registerDriver(new Driver());
    } catch (Exception e) {
      throw new RuntimeException("Failed to register Databricks JDBC driver", e);
    }
  }

  /** Benchmark result containing performance metrics for both ADBC and non-ADBC execution. */
  public static class BenchmarkResult {
    private final String query;
    private final ExecutionMetrics adbcMetrics;
    private final ExecutionMetrics jdbcMetrics;
    private final int rowCount;
    private final int columnCount;

    public BenchmarkResult(
        String query,
        ExecutionMetrics adbcMetrics,
        ExecutionMetrics jdbcMetrics,
        int rowCount,
        int columnCount) {
      this.query = query;
      this.adbcMetrics = adbcMetrics;
      this.jdbcMetrics = jdbcMetrics;
      this.rowCount = rowCount;
      this.columnCount = columnCount;
    }

    /** Print a detailed performance comparison report. */
    public void printReport() {
      System.out.println("\n" + "=".repeat(80));
      System.out.println("ADBC vs JDBC Performance Benchmark Report");
      System.out.println("=".repeat(80));
      System.out.println(
          "Query: " + (query.length() > 60 ? query.substring(0, 60) + "..." : query));
      System.out.println("Result Set: " + rowCount + " rows, " + columnCount + " columns");
      System.out.println("-".repeat(80));

      System.out.printf(
          "%-20s | %-15s | %-15s | %-10s%n", "Metric", "ADBC Mode", "JDBC Mode", "Improvement");
      System.out.println("-".repeat(80));

      printMetricComparison(
          "Connection Time", adbcMetrics.connectionTimeMs, jdbcMetrics.connectionTimeMs, "ms");
      printMetricComparison(
          "Execution Time", adbcMetrics.executionTimeMs, jdbcMetrics.executionTimeMs, "ms");
      printMetricComparison(
          "Processing Time", adbcMetrics.processingTimeMs, jdbcMetrics.processingTimeMs, "ms");
      printMetricComparison("Total Time", adbcMetrics.totalTimeMs, jdbcMetrics.totalTimeMs, "ms");
      printMetricComparison(
          "Memory Used", adbcMetrics.memoryUsedMB, jdbcMetrics.memoryUsedMB, "MB");
      printMetricComparison(
          "Throughput",
          adbcMetrics.throughputRowsPerSec,
          jdbcMetrics.throughputRowsPerSec,
          "rows/sec");

      System.out.println("-".repeat(80));
      System.out.printf(
          "Overall Performance: ADBC is %.2fx %s than JDBC%n",
          Math.abs(getSpeedupFactor()), getSpeedupFactor() > 1 ? "faster" : "slower");
      System.out.println("=".repeat(80));
    }

    private void printMetricComparison(
        String metric, double adbcValue, double jdbcValue, String unit) {
      double improvement = ((jdbcValue - adbcValue) / jdbcValue) * 100;
      String improvementStr = String.format("%+.1f%%", improvement);
      System.out.printf(
          "%-20s | %11.2f %-3s | %11.2f %-3s | %-10s%n",
          metric, adbcValue, unit, jdbcValue, unit, improvementStr);
    }

    private double getSpeedupFactor() {
      return jdbcMetrics.totalTimeMs / adbcMetrics.totalTimeMs;
    }

    // Getters for programmatic access
    public ExecutionMetrics getAdbcMetrics() {
      return adbcMetrics;
    }

    public ExecutionMetrics getJdbcMetrics() {
      return jdbcMetrics;
    }

    public double getSpeedup() {
      return getSpeedupFactor();
    }

    public int getRowCount() {
      return rowCount;
    }

    public int getColumnCount() {
      return columnCount;
    }
  }

  /** Execution metrics for a single benchmark run. */
  public static class ExecutionMetrics {
    private final double connectionTimeMs;
    private final double executionTimeMs;
    private final double processingTimeMs;
    private final double totalTimeMs;
    private final double memoryUsedMB;
    private final double throughputRowsPerSec;
    private final String resultSetType;

    public ExecutionMetrics(
        double connectionTimeMs,
        double executionTimeMs,
        double processingTimeMs,
        double totalTimeMs,
        double memoryUsedMB,
        double throughputRowsPerSec,
        String resultSetType) {
      this.connectionTimeMs = connectionTimeMs;
      this.executionTimeMs = executionTimeMs;
      this.processingTimeMs = processingTimeMs;
      this.totalTimeMs = totalTimeMs;
      this.memoryUsedMB = memoryUsedMB;
      this.throughputRowsPerSec = throughputRowsPerSec;
      this.resultSetType = resultSetType;
    }

    // Getters
    public double getConnectionTimeMs() {
      return connectionTimeMs;
    }

    public double getExecutionTimeMs() {
      return executionTimeMs;
    }

    public double getProcessingTimeMs() {
      return processingTimeMs;
    }

    public double getTotalTimeMs() {
      return totalTimeMs;
    }

    public double getMemoryUsedMB() {
      return memoryUsedMB;
    }

    public double getThroughputRowsPerSec() {
      return throughputRowsPerSec;
    }

    public String getResultSetType() {
      return resultSetType;
    }
  }

  /**
   * Run performance benchmark comparing ADBC vs JDBC execution.
   *
   * @param baseJdbcUrl JDBC URL without ADBC-specific parameters
   * @param query SQL query to benchmark
   * @return BenchmarkResult containing detailed performance metrics
   */
  public BenchmarkResult runBenchmark(String baseJdbcUrl, String query) {
    System.out.println("Starting ADBC vs JDBC Performance Benchmark...");
    System.out.println("Query: " + query);

    // Run warmup iterations
    System.out.println("Running warmup iterations...");
    for (int i = 0; i < WARMUP_RUNS; i++) {
      runSingleBenchmark(baseJdbcUrl, query, true, false); // ADBC warmup
      runSingleBenchmark(baseJdbcUrl, query, false, false); // JDBC warmup
    }

    // Run actual benchmark iterations
    System.out.println("Running benchmark iterations...");
    List<ExecutionMetrics> adbcResults = new ArrayList<>();
    List<ExecutionMetrics> jdbcResults = new ArrayList<>();
    int rowCount = 0;
    int columnCount = 0;

    for (int i = 0; i < BENCHMARK_RUNS; i++) {
      System.out.printf("Benchmark run %d/%d...%n", i + 1, BENCHMARK_RUNS);

      SingleRunResult adbcResult = runSingleBenchmark(baseJdbcUrl, query, true, true);
      SingleRunResult jdbcResult = runSingleBenchmark(baseJdbcUrl, query, false, true);

      adbcResults.add(adbcResult.metrics);
      jdbcResults.add(jdbcResult.metrics);

      if (i == 0) {
        rowCount = adbcResult.rowCount;
        columnCount = adbcResult.columnCount;
      }
    }

    // Calculate average metrics
    ExecutionMetrics avgAdbcMetrics = calculateAverageMetrics(adbcResults);
    ExecutionMetrics avgJdbcMetrics = calculateAverageMetrics(jdbcResults);

    return new BenchmarkResult(query, avgAdbcMetrics, avgJdbcMetrics, rowCount, columnCount);
  }

  private static class SingleRunResult {
    final ExecutionMetrics metrics;
    final int rowCount;
    final int columnCount;

    SingleRunResult(ExecutionMetrics metrics, int rowCount, int columnCount) {
      this.metrics = metrics;
      this.rowCount = rowCount;
      this.columnCount = columnCount;
    }
  }

  private SingleRunResult runSingleBenchmark(
      String baseJdbcUrl, String query, boolean useAdbc, boolean collectMetrics) {
    String jdbcUrl =
        useAdbc
            ? baseJdbcUrl + ";EnableAdbcMode=1;enableArrow=1;AdbcApiVersion=1.1.0"
            : baseJdbcUrl + ";EnableAdbcMode=0;enableArrow=0";

    long startTime = System.nanoTime();
    long memoryBefore = getUsedMemoryMB();

    try {
      // Connection phase
      long connectionStart = System.nanoTime();
      Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
      long connectionTime = System.nanoTime() - connectionStart;

      // Execution phase
      long executionStart = System.nanoTime();
      Statement stmt = con.createStatement();
      ResultSet rs = stmt.executeQuery(query);
      long executionTime = System.nanoTime() - executionStart;

      // Processing phase
      long processingStart = System.nanoTime();
      int rowCount = 0;
      int columnCount = rs.getMetaData().getColumnCount();
      String resultSetType = rs.getClass().getSimpleName();

      // Consume all results to measure processing time
      while (rs.next()) {
        rowCount++;
        // Access all columns to simulate real data processing
        for (int i = 1; i <= columnCount; i++) {
          rs.getObject(i);
        }
      }
      long processingTime = System.nanoTime() - processingStart;

      rs.close();
      stmt.close();
      con.close();

      long totalTime = System.nanoTime() - startTime;
      long memoryAfter = getUsedMemoryMB();
      double memoryUsed = Math.max(0, memoryAfter - memoryBefore);

      // Calculate metrics
      double connectionTimeMs = connectionTime / 1_000_000.0;
      double executionTimeMs = executionTime / 1_000_000.0;
      double processingTimeMs = processingTime / 1_000_000.0;
      double totalTimeMs = totalTime / 1_000_000.0;
      double throughput = rowCount / (totalTimeMs / 1000.0);

      if (collectMetrics) {
        System.out.printf(
            "  %s: %d rows, %.2f ms total, %s%n",
            useAdbc ? "ADBC" : "JDBC", rowCount, totalTimeMs, resultSetType);
      }

      ExecutionMetrics metrics =
          new ExecutionMetrics(
              connectionTimeMs,
              executionTimeMs,
              processingTimeMs,
              totalTimeMs,
              memoryUsed,
              throughput,
              resultSetType);

      return new SingleRunResult(metrics, rowCount, columnCount);

    } catch (Exception e) {
      throw new RuntimeException(
          "Benchmark failed for " + (useAdbc ? "ADBC" : "JDBC") + " mode", e);
    }
  }

  private ExecutionMetrics calculateAverageMetrics(List<ExecutionMetrics> metricsList) {
    double avgConnection =
        metricsList.stream().mapToDouble(ExecutionMetrics::getConnectionTimeMs).average().orElse(0);
    double avgExecution =
        metricsList.stream().mapToDouble(ExecutionMetrics::getExecutionTimeMs).average().orElse(0);
    double avgProcessing =
        metricsList.stream().mapToDouble(ExecutionMetrics::getProcessingTimeMs).average().orElse(0);
    double avgTotal =
        metricsList.stream().mapToDouble(ExecutionMetrics::getTotalTimeMs).average().orElse(0);
    double avgMemory =
        metricsList.stream().mapToDouble(ExecutionMetrics::getMemoryUsedMB).average().orElse(0);
    double avgThroughput =
        metricsList.stream()
            .mapToDouble(ExecutionMetrics::getThroughputRowsPerSec)
            .average()
            .orElse(0);
    String resultSetType = metricsList.get(0).getResultSetType();

    return new ExecutionMetrics(
        avgConnection,
        avgExecution,
        avgProcessing,
        avgTotal,
        avgMemory,
        avgThroughput,
        resultSetType);
  }

  private long getUsedMemoryMB() {
    Runtime runtime = Runtime.getRuntime();
    return (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
  }

  // Test methods demonstrating usage

  @Test
  void benchmarkSimpleQuery() {
    String jdbcUrl =
        "jdbc:databricks://sample-host.cloud.databricks.com:443/default;"
            + "httpPath=/sql/1.0/warehouses/999999999999;"
            + "transportMode=https;ssl=1;AuthMech=3;";

    String query = "SELECT 1 as id, 'test' as name, 42.5 as value";

    if (DATABRICKS_TOKEN != null) {
      BenchmarkResult result = runBenchmark(jdbcUrl, query);
      result.printReport();

      // Assert that the benchmark completed successfully
      assertTrue(result.getRowCount() > 0, "Benchmark should return at least one row");
      assertTrue(
          result.getAdbcMetrics().getTotalTimeMs() > 0, "ADBC execution time should be positive");
      assertTrue(
          result.getJdbcMetrics().getTotalTimeMs() > 0, "JDBC execution time should be positive");
    } else {
      System.out.println("Skipping benchmark test - DATABRICKS_EXAMPLE_TOKEN not set");
    }
  }

  @Test
  void benchmarkComplexQuery() {
    String jdbcUrl =
        "jdbc:databricks://sample-host.cloud.databricks.com:443/default;"
            + "httpPath=/sql/1.0/warehouses/999999999999;"
            + "transportMode=https;ssl=1;AuthMech=3;";

    String query =
        "SELECT "
            + "  row_number() OVER (ORDER BY 1) as row_num, "
            + "  CAST(row_number() OVER (ORDER BY 1) as BIGINT) as big_num, "
            + "  CAST('2023-12-01' as DATE) as sample_date, "
            + "  CAST(row_number() OVER (ORDER BY 1) * 1.5 as DECIMAL(10,2)) as decimal_val, "
            + "  'Row ' || CAST(row_number() OVER (ORDER BY 1) as STRING) as description "
            + "FROM (SELECT 1) t1 "
            + "CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t2 "
            + "CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t3";

    if (DATABRICKS_TOKEN != null) {
      BenchmarkResult result = runBenchmark(jdbcUrl, query);
      result.printReport();

      // Verify the benchmark ran with substantial data
      assertTrue(result.getRowCount() >= 25, "Complex query should return at least 25 rows");
      assertTrue(result.getColumnCount() >= 5, "Complex query should have at least 5 columns");
    } else {
      System.out.println("Skipping complex benchmark test - DATABRICKS_EXAMPLE_TOKEN not set");
    }
  }

  /**
   * Convenience method for running benchmarks programmatically.
   *
   * @param jdbcUrl Base JDBC URL (without ADBC parameters)
   * @param query SQL query to benchmark
   */
  public static void main(String[] args) {
    if (args.length < 2) {
      System.out.println("Usage: java AdbcPerformanceBenchmark <jdbcUrl> <query>");
      System.out.println(
          "Example: java AdbcPerformanceBenchmark "
              + "\"jdbc:databricks://host:443/default;httpPath=/sql/1.0/warehouses/123;AuthMech=3\" "
              + "\"SELECT * FROM my_table LIMIT 1000\"");
      return;
    }

    AdbcPerformanceBenchmark benchmark = new AdbcPerformanceBenchmark();
    try {
      DriverManager.registerDriver(new Driver());
      BenchmarkResult result = benchmark.runBenchmark(args[0], args[1]);
      result.printReport();
    } catch (Exception e) {
      System.err.println("Benchmark failed: " + e.getMessage());
      e.printStackTrace();
    }
  }
}
