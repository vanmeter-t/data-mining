package com.cs235.classifiers;

import com.cs235.Features;
import com.cs235.Main;
import com.cs235.database.SQLUtils;
import com.cs235.database.StringTemplate;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class NaiveBayesClassifier extends Classifier {

  private static final Map<String, Map<String, AtomicInteger>> accuracyTable = new LinkedHashMap<>();

  private Map<Integer, Double> severityTypeProbabilities;

  public NaiveBayesClassifier(String tableName) {
    super(tableName);
  }

  /**
   * Execute Naive Bayes Classifier
   * 1. Separate the datasets into trained & test datasets
   * 2. The "given" in this case will be the attributes  i.e. P(Severity_Level|X)
   * 3. Train the classifier with the trained dataset
   * a. Break the data down into the separate Severity levels
   * b. For each severity level calculate the probability of all possible attribute values
   * c. Hold the results in a map to be used to calculate the Probability on the training dataset
   * 4. Classify the test dataset and calculate the accuracy of the classifier
   * a. For each record, given the attribute values, determine the probability for each severity level
   * b. Select the highest probability and compare to the actual severity level to determine accuracy
   *
   * @return the string of results: Accuracy, and the JSON data with the probabilities for all attribute values
   * @throws Exception
   */
  @Override
  public String execute() throws Exception {
    createTrainingTestSets(tableName);
    Map<Integer, Map<String, Map<String, Double>>> trainedProbabilities = train();
    Double accuracy = executeOnTestData(trainedProbabilities);

    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    String json = gson.toJson(trainedProbabilities);

    String accuracyTableJson = gson.toJson(accuracyTable);

    return String.format("\n\nNaive Bayes Accuracy %s\n\nAccuracy Table: %s\n\nTraining Set Probabilities:%s", accuracy, accuracyTableJson, json);
  }

  /**
   * For each severity, get the probability that it will occur in the training dataset
   *
   * @param severityTables - each of the corresponding database tables for each severity level
   * @return a map of severity to the probability of that severity
   * @throws Exception
   */
  public Map<Integer, Double> getSeverityProbability(Map<Integer, String> severityTables) throws Exception {
    Map<Integer, Double> out = new LinkedHashMap<>();
    int trainingTotal = getTotalCount(trainingDataTable);
    for (Map.Entry<Integer, String> entry : severityTables.entrySet()) {
      String countSql = getCount.put("tableName", SQLUtils.escapeIdentifier(entry.getValue())).build();
      try (Connection connection = DriverManager.getConnection(Main.POSTGRES_URL);
           PreparedStatement ps = connection.prepareStatement(countSql)) {
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
          Integer severityCount = rs.getInt(1);
          out.put(entry.getKey(), (double) severityCount / trainingTotal);
        }
      }
    }
    return out;
  }

  /**
   * * FROM TRAINING DATA
   * for each severity, for each attribute column, determine the probability given the severity
   * P(Alcohol - Yes | Severity_1)=0.66
   * P(Alcohol - No | Severity_1)=0.66
   * P(Weather A | Severity_1)=0.33
   * P(Weather B | Severity_1)=0.33
   * P(Weather C | Severity_1)=0.33
   * P(Weather D | Severity_1)=0.33
   * P(RoadCondition A | Severity_1)=0.33
   * P(RoadCondition B | Severity_1)=0.33
   *
   * @return mapping for each severity level, for each attribute column, for each attribute column value, the probability of P(Severity|X)
   * @throws Exception
   */
  public Map<Integer, Map<String, Map<String, Double>>> train() throws Exception {
    Map<Integer, String> severityTables = separateBySeverity(trainingDataTable);
    Map<Integer, Integer> severityTypeCount = getSeverityCount(severityTables);
    severityTypeProbabilities = getSeverityProbability(severityTables);

    Map<Integer, Map<String, Map<String, Double>>> trainedClassifier = new LinkedHashMap<>();
    for (Map.Entry<Integer, String> entry : severityTables.entrySet()) {
      Integer severity = entry.getKey();


      Map<String, Map<String, Double>> featureMapping = new LinkedHashMap<>();
      for (Features attribute : attributes) {
        Map<String, Double> featureProb = getAttributeValProbabilities(severityTables.get(severity), attribute.getLabel(), severityTypeCount.get(severity));
        featureMapping.put(attribute.getLabel(), featureProb);
      }
      trainedClassifier.put(severity, featureMapping);
    }
    return trainedClassifier;
  }

  /**
   * for each feature, determine the severity class that the trainedProbabilities would classify as based on Naive Bayes
   *
   * @param trainedProbabilities the probabilities for each attribute value given a certain severity level
   * @return the accuracy of the classifier
   * @throws Exception
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
          .put("fields", attributes.stream().map(Features::getLabel).map(SQLUtils::escapeIdentifier).collect(Collectors.joining(",")))
          .put("tableName", SQLUtils.escapeIdentifier(severityTable)).build();

        try (Connection connection = DriverManager.getConnection(Main.POSTGRES_URL);
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

            if (accuracyTable.containsKey(actualSeverity.toString())) {
              Map<String, AtomicInteger> predicted = accuracyTable.get(actualSeverity.toString());
              if (predicted.containsKey(classifiedSeverity.getKey().toString())) {
                predicted.get(classifiedSeverity.getKey().toString()).incrementAndGet();
              } else {
                predicted.put(classifiedSeverity.getKey().toString(), new AtomicInteger(1));
              }
            } else {
              Map<String, AtomicInteger> predicted = new LinkedHashMap<>();
              predicted.put(classifiedSeverity.getKey().toString(), new AtomicInteger(1));
              accuracyTable.put(actualSeverity.toString(), predicted);
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
   *
   * @param rs          result set containing the single record from the training set
   * @param severity    current severity level to determine the probability for given the records attribute values
   * @param trainedData trained data probabilities used as a lookup
   * @return probability for P(Severity|X) where X is a vector of all attribute values for this record
   * @throws Exception
   */
  private Double getRecordProbabilityPerSeverity(ResultSet rs, Integer severity, Map<String, Map<String, Double>> trainedData) throws Exception {
    List<Double> out = new ArrayList<>();
    for (Features attribute : attributes) {
      String attributeLabel = attribute.getLabel();
      String featureValue = rs.getString(attributeLabel);
      if (trainedData.containsKey(attributeLabel)) {
        out.add(trainedData.get(attributeLabel).get(featureValue));
      }
    }
    Double p = out.stream().filter(val -> val != null).reduce(1.0, (a, b) -> a * b);
    return p * severityTypeProbabilities.get(severity);
  }

}
