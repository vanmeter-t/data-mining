package com.cs235.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class PostgresSQL {
  public static final String POSTGRES_URL = "jdbc:postgresql://localhost:32770/postgres?user=postgres";
  private static final String DROP_TABLE = "DROP TABLE ${name}";

  public static void dropTable(String tableName) throws Exception {
    String sql = new StringTemplate(DROP_TABLE).put("name", SQLUtils.escapeIdentifier(tableName)).build();
    try (Connection connection = DriverManager.getConnection(PostgresSQL.POSTGRES_URL);
         Statement stmt = connection.createStatement()) {
      stmt.execute(sql);
    }
  }
}
