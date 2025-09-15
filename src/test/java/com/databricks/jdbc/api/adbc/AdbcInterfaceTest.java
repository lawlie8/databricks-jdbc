package com.databricks.jdbc.api.adbc;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.junit.jupiter.api.Test;

/** Test to verify ADBC interfaces are available and can be used. */
public class AdbcInterfaceTest {

  @Test
  public void testAdbcInterfacesAvailable() {
    // Just verify the interfaces can be imported and referenced
    Class<?> connectionClass = AdbcConnection.class;
    Class<?> statementClass = AdbcStatement.class;
    Class<?> databaseClass = AdbcDatabase.class;

    System.out.println("ADBC interfaces available:");
    System.out.println("- " + connectionClass.getName());
    System.out.println("- " + statementClass.getName());
    System.out.println("- " + databaseClass.getName());
  }
}
