package com.cs235.classifiers;

import com.cs235.Attribute;
import com.cs235.Features;
import com.cs235.Main;
import com.cs235.database.IdGenerator;
import com.cs235.database.SQLUtils;
import com.cs235.database.StringTemplate;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class Classifier {

  protected static final List<Features> attributes = new ArrayList<Features>() {{
    add(Features.WEATHER_COLUMN);
    add(Features.ALCOHOL_COLUMN);
    add(Features.TIME_CAT_COLUMN);
    add(Features.COL_TYPE_COLUMN);
    add(Features.ROAD_SURF_COLUMN);
    add(Features.ROAD_COND_COLUMN);
    add(Features.LIGHTING_COLUMN);
  }};

  protected static final List<Features> allAttributes = new ArrayList<Features>() {{
    add(Features.SEVERITY_COLUMN);
    add(Features.WEATHER_COLUMN);
    add(Features.ALCOHOL_COLUMN);
    add(Features.TIME_CAT_COLUMN);
    add(Features.COL_TYPE_COLUMN);
    add(Features.ROAD_SURF_COLUMN);
    add(Features.ROAD_COND_COLUMN);
    add(Features.LIGHTING_COLUMN);
  }};

  protected final String tableName;
  protected String trainingDataTable;
  protected String testDataTable;
  protected StringTemplate getCount = new StringTemplate("SELECT COUNT(*) FROM ${tableName}");

  public Classifier(String tableName) {
    this.tableName = tableName;
  }

  /**
   * Get all records for the specific columns to determine all existing itemsets
   *
   * @param tableName
   * @return
   * @throws Exception
   */
  protected static List<List<Attribute>> loadItemsets(String tableName) throws Exception {
    List<List<Attribute>> dataset = new ArrayList<>();

    String itemsetsSql = new StringTemplate("SELECT ${fields} FROM ${tableName}")
      .put("fields", allAttributes.stream().map(Features::getLabel).map(SQLUtils::escapeIdentifier).collect(Collectors.joining(",")))
      .put("tableName", SQLUtils.escapeIdentifier(tableName)).build();

    try (Connection connection = DriverManager.getConnection(Main.POSTGRES_URL);
         PreparedStatement ps = connection.prepareStatement(itemsetsSql)) {
      ResultSet rs = ps.executeQuery();
      while (rs.next()) {
        List<Attribute> recordValues = new ArrayList<>();
        for (Features attribute : allAttributes) {
          recordValues.add(new Attribute(attribute, rs.getString(attribute.getLabel())));
        }
        dataset.add(recordValues);
      }
    }
    return dataset;
  }

  public abstract String execute() throws Exception;

  /**
   * Create two separate tables randomly with 80% of the data in the training dataset and 20% in the test dataset
   *
   * @param tableName the input dataset table to select from
   * @throws Exception
   */
  protected void createTrainingTestSets(String tableName) throws Exception {

    trainingDataTable = IdGenerator.generate("training_");
    String trainingSql = new StringTemplate("CREATE TABLE ${newTable} AS SELECT * FROM ${table} WHERE (random() <= 0.80)")
      .put("newTable", SQLUtils.escapeIdentifier(trainingDataTable))
      .put("table", SQLUtils.escapeIdentifier(tableName))
      .build();

    testDataTable = IdGenerator.generate("test_");
    String testSql = new StringTemplate("CREATE TABLE ${newTable} AS SELECT * FROM ${table} WHERE ${oid} NOT IN (SELECT ${oid} FROM ${trainingTable})")
      .put("newTable", SQLUtils.escapeIdentifier(testDataTable))
      .put("table", SQLUtils.escapeIdentifier(tableName))
      .put("oid", SQLUtils.escapeIdentifier(Features.OID_COLUMN.getLabel()))
      .put("trainingTable", SQLUtils.escapeIdentifier(trainingDataTable))
      .build();

    try (Connection connection = DriverManager.getConnection(Main.POSTGRES_URL);
         PreparedStatement trainPs = connection.prepareStatement(trainingSql);
         PreparedStatement testPs = connection.prepareStatement(testSql)) {
      trainPs.execute();
      testPs.execute();
    }
  }

  /**
   * get the total count of records for the given dataset table
   *
   * @param table a table in the PostgreSQL database
   * @return
   * @throws Exception
   */
  protected int getTotalCount(String table) throws Exception {
    String countSql = getCount.put("tableName", SQLUtils.escapeIdentifier(table)).build();
    try (Connection connection = DriverManager.getConnection(Main.POSTGRES_URL);
         PreparedStatement ps = connection.prepareStatement(countSql)) {
      ResultSet rs = ps.executeQuery();
      if (rs.next()) {
        return rs.getInt(1);
      }
    }
    return 0;
  }

  /**
   * generate the probability for each feature given the severity level
   *
   * @param table      the severity table to get the probability for each attribute value
   * @param column     the specific attribute
   * @param totalCount the total count of records for this severity
   * @return mapping of attribute value to probability
   * @throws Exception
   */
  protected Map<String, Double> getAttributeValProbabilities(String table, String column, int totalCount) throws Exception {
    Map<String, Double> featureProb = new LinkedHashMap<>();
    String distinctCountSql = new StringTemplate("SELECT ${column}, COUNT(*) FROM ${tableName} GROUP BY ${column}")
      .put("column", SQLUtils.escapeIdentifier(column))
      .put("tableName", SQLUtils.escapeIdentifier(table)).build();

    try (Connection connection = DriverManager.getConnection(Main.POSTGRES_URL);
         PreparedStatement ps = connection.prepareStatement(distinctCountSql)) {
      ResultSet rs = ps.executeQuery();
      while (rs.next()) {
        String featureValue = rs.getString(1);
        Integer featureCount = rs.getInt(2);
        featureProb.put(featureValue, (double) featureCount / totalCount);
      }
    }
    return featureProb;
  }

  /**
   * Separate the input dataset into multiple tables for each of the severity values that exist
   *
   * @param tableName the input dataset table
   * @return the mapping of severity level to dataset table name
   * @throws Exception
   */
  protected Map<Integer, String> separateBySeverity(String tableName) throws Exception {

    Map<Integer, String> severityLevelToTable = new LinkedHashMap<>();

    // create multiple tables of the different severitys
    String getDistinctSql = new StringTemplate("SELECT DISTINCT ${field} AS severity FROM ${table}")
      .put("field", SQLUtils.escapeIdentifier(Features.SEVERITY_COLUMN.getLabel()))
      .put("table", SQLUtils.escapeIdentifier(tableName))
      .build();

    try (Connection connection = DriverManager.getConnection(Main.POSTGRES_URL);
         PreparedStatement ps = connection.prepareStatement(getDistinctSql)) {
      ResultSet rs = ps.executeQuery();
      while (rs.next()) {
        Integer severity = rs.getInt("severity");
        String sevTableName = IdGenerator.generate(tableName + "_" + severity.toString() + "_");

        String createSeverityTable = new StringTemplate("CREATE TABLE ${newTable} AS (SELECT ${fields} FROM ${table} WHERE ${field} = ${severity})")
          .put("newTable", SQLUtils.escapeIdentifier(sevTableName))
          .put("fields", attributes.stream().map(Features::getLabel).map(SQLUtils::escapeIdentifier).collect(Collectors.joining(",")))
          .put("table", SQLUtils.escapeIdentifier(tableName))
          .put("field", SQLUtils.escapeIdentifier(Features.SEVERITY_COLUMN.getLabel()))
          .put("severity", severity)
          .build();

        Statement stmt = connection.createStatement();
        stmt.execute(createSeverityTable);

        severityLevelToTable.put(severity, sevTableName);
      }
    }
    return severityLevelToTable;
  }


  /**
   * For each severity level, determine the total count
   *
   * @param severityTables - the severity level and corresponding database table
   * @return map of severity level to the total count
   * @throws Exception
   */
  protected Map<Integer, Integer> getSeverityCount(Map<Integer, String> severityTables) throws Exception {
    Map<Integer, Integer> out = new LinkedHashMap<>();
    for (Map.Entry<Integer, String> entry : severityTables.entrySet()) {
      String countSql = getCount.put("tableName", SQLUtils.escapeIdentifier(entry.getValue())).build();
      try (Connection connection = DriverManager.getConnection(Main.POSTGRES_URL);
           PreparedStatement ps = connection.prepareStatement(countSql)) {
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
          out.put(entry.getKey(), rs.getInt(1));
        }
      }
    }
    return out;
  }

}
