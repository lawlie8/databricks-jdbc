package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.api.IDatabricksResultSet;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.core.StatementStatus;
import java.sql.SQLException;

/**
 * Factory for creating appropriate DatabricksResultSet implementations based on connection
 * configuration and ADBC mode settings.
 *
 * <p>This factory determines whether to create:
 *
 * <ul>
 *   <li>{@link DatabricksResultSet} - Standard JDBC implementation
 *   <li>{@link AdbcDatabricksResultSet} - ADBC-compliant implementation with Arrow streaming
 * </ul>
 */
public class DatabricksResultSetFactory {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksResultSetFactory.class);

  /**
   * Creates a DatabricksResultSet from SEA (SQL Execution API) response. Chooses between standard
   * JDBC and ADBC implementations based on session configuration.
   */
  public static IDatabricksResultSet createFromSeaResponse(
      StatementStatus statementStatus,
      StatementId statementId,
      ResultData resultData,
      ResultManifest resultManifest,
      StatementType statementType,
      IDatabricksSession session,
      IDatabricksStatementInternal parentStatement)
      throws DatabricksSQLException {

    boolean adbcModeEnabled = session.getConnectionContext().isAdbcModeEnabled();

    LOGGER.debug(
        "Creating result set for statement {} with ADBC mode: {}",
        statementId.toSQLExecStatementId(),
        adbcModeEnabled);

    if (adbcModeEnabled) {
      return new AdbcDatabricksResultSet(
          statementStatus,
          statementId,
          resultData,
          resultManifest,
          statementType,
          session,
          parentStatement,
          true);
    } else {
      return new DatabricksResultSet(
          statementStatus,
          statementId,
          resultData,
          resultManifest,
          statementType,
          session,
          parentStatement);
    }
  }

  /**
   * Creates a DatabricksResultSet from Thrift response. Chooses between standard JDBC and ADBC
   * implementations based on session configuration.
   */
  public static IDatabricksResultSet createFromThriftResponse(
      StatementStatus statementStatus,
      StatementId statementId,
      TFetchResultsResp resultsResp,
      StatementType statementType,
      IDatabricksStatementInternal parentStatement,
      IDatabricksSession session)
      throws SQLException {

    boolean adbcModeEnabled = session.getConnectionContext().isAdbcModeEnabled();

    LOGGER.debug(
        "Creating result set from Thrift for statement {} with ADBC mode: {}",
        statementId.toString(),
        adbcModeEnabled);

    if (adbcModeEnabled) {
      return new AdbcDatabricksResultSet(
          statementStatus, statementId, resultsResp, statementType, parentStatement, session, true);
    } else {
      return new DatabricksResultSet(
          statementStatus, statementId, resultsResp, statementType, parentStatement, session);
    }
  }

  /**
   * Creates a DatabricksResultSet from Thrift accessor (simpler signature).
   * Used by DatabricksThriftAccessor for synchronous execution.
   */
  public static IDatabricksResultSet createFromThriftAccessor(
      StatementStatus statementStatus,
      StatementId statementId,
      TFetchResultsResp resultsResp,
      StatementType statementType,
      IDatabricksStatementInternal parentStatement,
      IDatabricksSession session)
      throws SQLException {

    return createFromThriftResponse(
        statementStatus, statementId, resultsResp, statementType, parentStatement, session);
  }

  /**
   * Creates a metadata result set (always uses standard implementation). Metadata operations don't
   * benefit from ADBC streaming optimizations.
   */
  public static IDatabricksResultSet createMetadataResultSet(
      StatementStatus statementStatus,
      StatementId statementId,
      java.util.List<String> columnNames,
      java.util.List<String> columnTypeText,
      int[] columnTypes,
      int[] columnTypePrecisions,
      int[] isNullables,
      Object[][] rows,
      StatementType statementType) {

    LOGGER.debug("Creating metadata result set for statement {}", statementId.toString());

    return new DatabricksResultSet(
        statementStatus,
        statementId,
        columnNames,
        columnTypeText,
        columnTypes,
        columnTypePrecisions,
        isNullables,
        rows,
        statementType);
  }

  /**
   * Determines if ADBC mode should be used for the given session and result characteristics.
   *
   * @param session The database session
   * @param hasArrowData Whether the result contains Arrow-formatted data
   * @return true if ADBC mode should be used, false for standard JDBC mode
   */
  public static boolean shouldUseAdbcMode(IDatabricksSession session, boolean hasArrowData) {
    if (!session.getConnectionContext().isAdbcModeEnabled()) {
      return false;
    }

    // If Arrow streaming only mode is enabled, force ADBC even without Arrow data
    if (session.getConnectionContext().isArrowStreamingOnly()) {
      return true;
    }

    // Otherwise, use ADBC only when we have Arrow data
    return hasArrowData;
  }
}
