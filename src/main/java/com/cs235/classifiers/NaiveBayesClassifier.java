package com.cs235.classifiers;

import com.cs235.Main;
import com.cs235.database.IdGenerator;
import com.cs235.database.PostgresSQL;
import com.cs235.database.SQLUtils;
import com.cs235.database.StringTemplate;
import com.google.gson.Gson;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class NaiveBayesClassifier {

  private final String tableName;
  private String trainingDataTable;
  private String testDataTable;
  private Map<Integer, Double> severityTypeProbabilities;
  private StringTemplate getCount = new StringTemplate("SELECT COUNT(*) FROM ${tableName}");

  public NaiveBayesClassifier(String tableName) {
    this.tableName = tableName;
  }

  public String execute() throws Exception {
    createTrainingTestSets(tableName);
    Map<Integer, Map<String, Map<String, Double>>> trainedProbabilities = train();
    Double accuracy = executeOnTestData(trainedProbabilities);

    Gson gson = new Gson();
    String json = gson.toJson(trainedProbabilities);

    return String.format("\n\nNaive Bayes Accuracy %s\n\nTraining Set Probabilities:%s", accuracy, json);
  }

  public void createTrainingTestSets(String tableName) throws Exception {

    trainingDataTable = IdGenerator.generate("training_");
    String trainingSql = new StringTemplate("CREATE TABLE ${newTable} AS SELECT * FROM ${table} WHERE (random() <= 0.80)")
      .put("newTable", SQLUtils.escapeIdentifier(trainingDataTable))
      .put("table", SQLUtils.escapeIdentifier(tableName))
      .build();

    testDataTable = IdGenerator.generate("test_");
    String testSql = new StringTemplate("CREATE TABLE ${newTable} AS SELECT * FROM ${table} WHERE ${oid} NOT IN (SELECT ${oid} FROM ${trainingTable})")
      .put("newTable", SQLUtils.escapeIdentifier(testDataTable))
      .put("table", SQLUtils.escapeIdentifier(tableName))
      .put("oid", SQLUtils.escapeIdentifier(Main.OID_COLUMN))
      .put("trainingTable", SQLUtils.escapeIdentifier(trainingDataTable))
      .build();

    try (Connection connection = DriverManager.getConnection(PostgresSQL.POSTGRES_URL);
         PreparedStatement trainPs = connection.prepareStatement(trainingSql);
         PreparedStatement testPs = connection.prepareStatement(testSql)) {
      trainPs.execute();
      testPs.execute();
    }
  }

  public Map<Integer, String> separateBySeverity(String tableName) throws Exception {

    Map<Integer, String> severityLevelToTable = new LinkedHashMap<>();

    // create multiple tables of the different severitys
    String getDistinctSql = new StringTemplate("SELECT DISTINCT ${field} AS severity FROM ${table}")
      .put("field", SQLUtils.escapeIdentifier(Main.SEVERITY_COLUMN))
      .put("table", SQLUtils.escapeIdentifier(tableName))
      .build();

    try (Connection connection = DriverManager.getConnection(PostgresSQL.POSTGRES_URL);
         PreparedStatement ps = connection.prepareStatement(getDistinctSql)) {
      ResultSet rs = ps.executeQuery();
      while (rs.next()) {
        Integer severity = rs.getInt("severity");
        String sevTableName = IdGenerator.generate(tableName + "_" + severity.toString() + "_");

        String createSeverityTable = new StringTemplate("CREATE TABLE ${newTable} AS (SELECT ${fields} FROM ${table} WHERE ${field} = ${severity})")
          .put("newTable", SQLUtils.escapeIdentifier(sevTableName))
          .put("fields", Main.attributes.stream().map(SQLUtils::escapeIdentifier).collect(Collectors.joining(",")))
          .put("table", SQLUtils.escapeIdentifier(tableName))
          .put("field", SQLUtils.escapeIdentifier(Main.SEVERITY_COLUMN))
          .put("severity", severity)
          .build();

        Statement stmt = connection.createStatement();
        stmt.execute(createSeverityTable);

        severityLevelToTable.put(severity, sevTableName);
      }
    }
    return severityLevelToTable;
  }

  public int getTotalCount(String table) throws Exception {
    String countSql = getCount.put("tableName", SQLUtils.escapeIdentifier(table)).build();
    try (Connection connection = DriverManager.getConnection(PostgresSQL.POSTGRES_URL);
         PreparedStatement ps = connection.prepareStatement(countSql)) {
      ResultSet rs = ps.executeQuery();
      if (rs.next()) {
        return rs.getInt(1);
      }
    }
    return 0;
  }

  public Map<Integer, Double> getSeverityProbability(int total, Map<Integer, String> severityTables) throws Exception {
    Map<Integer, Double> out = new LinkedHashMap<>();
    for (Map.Entry<Integer, String> entry : severityTables.entrySet()) {
      String countSql = getCount.put("tableName", SQLUtils.escapeIdentifier(entry.getValue())).build();
      try (Connection connection = DriverManager.getConnection(PostgresSQL.POSTGRES_URL);
           PreparedStatement ps = connection.prepareStatement(countSql)) {
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
          Integer severityCount = rs.getInt(1);
          out.put(entry.getKey(), (double) severityCount / total);
        }
      }
    }
    return out;
  }

  public Map<Integer, Integer> getSeverityCount(Map<Integer, String> severityTables) throws Exception {
    Map<Integer, Integer> out = new LinkedHashMap<>();
    for (Map.Entry<Integer, String> entry : severityTables.entrySet()) {
      String countSql = getCount.put("tableName", SQLUtils.escapeIdentifier(entry.getValue())).build();
      try (Connection connection = DriverManager.getConnection(PostgresSQL.POSTGRES_URL);
           PreparedStatement ps = connection.prepareStatement(countSql)) {
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
          out.put(entry.getKey(), rs.getInt(1));
        }
      }
    }
    return out;
  }

  /**
   * FROM TRAINING DATA
   * for each severity, for each attribute column, determine the probability given the severity
   * P(Alcohol - Yes | Severity_1)=0.66
   * P(Alcohol - No | Severity_1)=0.66
   * P(Weather A | Severity_1)=0.33
   * P(Weather B | Severity_1)=0.33
   * P(Weather C | Severity_1)=0.33
   * P(Weather D | Severity_1)=0.33
   * P(RoadCondition A | Severity_1)=0.33
   * P(RoadCondition B | Severity_1)=0.33
   * ...
   */
  public Map<Integer, Map<String, Map<String, Double>>> train() throws Exception {
    int trainingTotal = getTotalCount(trainingDataTable);
    Map<Integer, String> severityTables = separateBySeverity(trainingDataTable);
    Map<Integer, Integer> severityTypeCount = getSeverityCount(severityTables);
    severityTypeProbabilities = getSeverityProbability(trainingTotal, severityTables);

    Map<Integer, Map<String, Map<String, Double>>> trainedClassifier = new LinkedHashMap<>();
    for (Map.Entry<Integer, String> entry : severityTables.entrySet()) {
      Integer severity = entry.getKey();
      Map<String, Map<String, Double>> featureMapping = new LinkedHashMap<>();
      for (String attribute : Main.attributes) {
        Map<String, Double> featureProb = getFeatureProbabilities(severityTables.get(severity), attribute, severityTypeCount.get(severity));
        featureMapping.put(attribute, featureProb);
      }
      trainedClassifier.put(severity, featureMapping);
    }
    return trainedClassifier;
  }

  /**
   * generate the probability for each feature given the severity level
   */
  public Map<String, Double> getFeatureProbabilities(String table, String column, int totalCount) throws Exception {
    Map<String, Double> featureProb = new LinkedHashMap<>();
    String distinctCountSql = new StringTemplate("SELECT ${column}, COUNT(*) FROM ${tableName} GROUP BY ${column}")
      .put("column", SQLUtils.escapeIdentifier(column))
      .put("tableName", SQLUtils.escapeIdentifier(table)).build();

    try (Connection connection = DriverManager.getConnection(PostgresSQL.POSTGRES_URL);
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
   * for each feature, determine the severity class that the trainedProbabilities would classify as based on Naive Bayes
   */
  public Double executeOnTestData(Map<Integer, Map<String, Map<String, Double>>> trainedProbabilities) throws Exception {

    int classifiedProperly = 0;
    int totalTestData = getTotalCount(testDataTable);

    try {

      Map<Integer, String> severityLevelToTable = separateBySeverity(testDataTable);
      for (Map.Entry<Integer, String> entry : severityLevelToTable.entrySet()) {
        Integer actualSeverity = entry.getKey();

        String severityTable = severityLevelToTable.get(actualSeverity);
        String testDataSql = new StringTemplate("SELECT ${fields} FROM ${tableName}")
          .put("fields", Main.attributes.stream().map(SQLUtils::escapeIdentifier).collect(Collectors.joining(",")))
          .put("tableName", SQLUtils.escapeIdentifier(severityTable)).build();

        try (Connection connection = DriverManager.getConnection(PostgresSQL.POSTGRES_URL);
             PreparedStatement ps = connection.prepareStatement(testDataSql)) {
          ResultSet rs = ps.executeQuery();
          while (rs.next()) {

            Map<Integer, Double> recordFinalProb = new LinkedHashMap<>();
            for (Map.Entry<Integer, Map<String, Map<String, Double>>> trainedEntry : trainedProbabilities.entrySet()) {
              Integer severityLevel = trainedEntry.getKey();
              Map<String, Map<String, Double>> trainedData = trainedEntry.getValue();
              recordFinalProb.put(severityLevel, getRecordProbabilityPerSeverity(rs, severityLevel, trainedData));
            }

            // Did Naive Bayes classify it properly?
            Map.Entry<Integer, Double> classifiedSeverity = null;
            for (Map.Entry<Integer, Double> finalyEntry : recordFinalProb.entrySet()) {
              if (classifiedSeverity == null || finalyEntry.getValue().compareTo(classifiedSeverity.getValue()) > 0) {
                classifiedSeverity = finalyEntry;
              }
            }
            if (classifiedSeverity.getKey().equals(actualSeverity)) {
              classifiedProperly++;
            }
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return (double) classifiedProperly / totalTestData;
  }

  /**
   * for this record, get all of the probabilities from the trained data and generate the final probability given the severity level
   */
  private Double getRecordProbabilityPerSeverity(ResultSet rs, Integer severity, Map<String, Map<String, Double>> trainedData) throws Exception {
    List<Double> out = new ArrayList<>();
    for (String attribute : Main.attributes) {
      String featureValue = rs.getString(attribute);
      if (trainedData.containsKey(attribute)) {
        out.add(trainedData.get(attribute).get(featureValue));
      }
    }
    Double p = out.stream().filter(val -> val != null).reduce(1.0, (a, b) -> a * b);
    return p * severityTypeProbabilities.get(severity);
  }

}
