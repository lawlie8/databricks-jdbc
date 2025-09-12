ADBC Integration Proposal for Databricks JDBC Driver

  Current Architecture Analysis

  The current codebase already has strong Arrow integration:
  - Arrow Version: 17.0.0 (pom.xml:43)
  - Existing Arrow Support: ArrowStreamResult class handles Arrow-based query results (ArrowStreamResult.java:28)
  - Stream Processing: ArrowResultChunkIterator provides row-by-row iteration over Arrow chunks (ArrowResultChunkIterator.java:9)
  - Result Set Types: SEA_ARROW_ENABLED and THRIFT_ARROW_ENABLED already support Arrow streaming (DatabricksResultSet.java:52-56)

  Phase 1: Core ADBC Interface Extension

  1.1 Extend IDatabricksResultSet Interface

  public interface IDatabricksResultSet extends ResultSet {
      // Existing methods...

      // New ADBC-specific methods
      ArrowReader getArrowReader() throws SQLException;
      Stream<VectorSchemaRoot> getArrowStream() throws SQLException;
      boolean supportsArrowStreaming();
  }

  1.2 Create ADBC-Compliant Result Set Implementation

  Create AdbcDatabricksResultSet extending DatabricksResultSet:
  - Implement native Arrow streaming without JDBC row-by-row conversion
  - Provide direct ArrowIPC stream access
  - Maintain backward compatibility with existing JDBC interface

  Phase 2: ArrowIPC Stream Iterator Integration

  2.1 Enhanced ArrowStreamResult Integration

  Extend ArrowStreamResult.java to support ADBC streaming patterns:
  - Add getArrowReader() method returning Apache Arrow's ArrowReader
  - Implement zero-copy data access where possible
  - Support both push and pull streaming models

  2.2 New Iterator Pattern

  Create AdbcArrowStreamIterator that:
  - Leverages existing ArrowResultChunkIterator (ArrowResultChunkIterator.java:27)
  - Provides batch-level iteration instead of row-level
  - Implements Iterator<VectorSchemaRoot> for columnar data access

  Phase 3: Connection and Statement Extensions

  3.1 ADBC Connection Interface

  Create IAdbcConnection extending IDatabricksConnection:
  public interface IAdbcConnection extends Connection {
      AdbcStatement createAdbcStatement() throws SQLException;
      boolean getAutoCommit() throws SQLException; // ADBC-specific behavior
      ArrowReader executeQuery(String sql) throws SQLException;
  }

  3.2 ADBC Statement Implementation

  Implement AdbcDatabricksStatement:
  - Direct Arrow result return (no JDBC conversion overhead)
  - Bulk parameter binding using Arrow format
  - Support for prepared statements with Arrow parameters

  Phase 4: Configuration and Feature Detection

  4.1 Connection Parameters

  Add new connection parameters:
  - EnableAdbcMode=true: Enable ADBC-compliant behavior
  - ArrowStreamingOnly=true: Disable JDBC row-by-row fallback
  - AdbcApiVersion=1.1.0: Specify ADBC API compliance level

  4.2 Capability Detection

  Extend existing capability detection in connection context:
  public boolean isAdbcModeEnabled();
  public boolean supportsArrowStreamingNatively();
  public String getAdbcApiVersion();

  Implementation Strategy

  Recommended Phased Approach:

  1. Phase 1 (Foundation): Extend interfaces and create ADBC-specific result set
    - Leverage existing ArrowStreamResult architecture
    - Add Arrow reader access methods
    - Maintain JDBC compatibility
  2. Phase 2 (Streaming): Implement ArrowIPC stream iterator
    - Build on existing ArrowResultChunkIterator
    - Add batch-level streaming capabilities
    - Optimize for zero-copy data access
  3. Phase 3 (Full ADBC): Complete ADBC protocol implementation
    - Create ADBC-specific connection and statement classes
    - Implement bulk operations
    - Add comprehensive parameter binding
  4. Phase 4 (Integration): Testing and optimization
    - Performance benchmarking against JDBC mode
    - Comprehensive integration testing
    - Documentation and migration guides

  Key Technical Considerations

  1. Backward Compatibility: All existing JDBC functionality must remain unchanged
  2. Performance: ADBC mode should provide significant performance improvements for analytical workloads
  3. Memory Management: Leverage existing Arrow memory management patterns
  4. Error Handling: Maintain consistent error handling patterns across both modes
  5. Testing: Extensive testing required for both JDBC and ADBC code paths

  Benefits of This Approach

  - Minimal Disruption: Builds on existing Arrow infrastructure
  - Performance Gains: Direct Arrow streaming eliminates conversion overhead
  - Standard Compliance: Follows ADBC 1.1.0 specification
  - Flexibility: Supports both JDBC and ADBC usage patterns
  - Future-Proof: Positions driver for emerging Arrow-native ecosystems

  This proposal leverages the strong existing Arrow foundation in the codebase while adding ADBC-compliant interfaces that can coexist with the current JDBC implementation.
