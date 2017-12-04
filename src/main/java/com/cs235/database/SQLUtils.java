package com.cs235.database;

import java.sql.SQLException;

public final class SQLUtils {
  private SQLUtils() {
    throw new IllegalAccessError("Utility class");
  }

  public static String escapeIdentifier(String identifier) {
    String[] fqIdentifier = identifier.split("\\.");
    if (fqIdentifier.length == 2) {
      String p = fqIdentifier[0];
      String q = fqIdentifier[1];
      return escapeIdentifier(p) + "." + escapeIdentifier(q);
    }

    StringBuffer sbuf = new StringBuffer(2 + identifier.length() * 11 / 10); // Add 10% for escaping.
    try {
      org.postgresql.core.Utils.appendEscapedIdentifier(sbuf, identifier);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return sbuf.toString();
  }

}
