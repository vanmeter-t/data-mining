package com.cs235.classifiers;

import com.cs235.Attribute;
import com.cs235.Features;
import com.cs235.Main;
import com.cs235.database.SQLUtils;
import com.cs235.database.StringTemplate;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.stream.Collectors;

public class AssociationRules extends Classifier {

  private static final double minSupport = 0.25;

  public AssociationRules(String tableName) {
    super(tableName);
  }

  @Override
  public String execute() throws Exception {
    createTrainingTestSets(tableName);
    train();

    return "";
  }

  private List<List<Attribute>> loadItemsets(String tableName) throws Exception {
    List<List<Attribute>> dataset = new ArrayList<>();

    String itemsetsSql = new StringTemplate("SELECT ${fields} FROM ${tableName}")
      .put("fields", attributes.stream().map(Features::getLabel).map(SQLUtils::escapeIdentifier).collect(Collectors.joining(",")))
      .put("tableName", SQLUtils.escapeIdentifier(tableName)).build();

    try (Connection connection = DriverManager.getConnection(Main.POSTGRES_URL);
         PreparedStatement ps = connection.prepareStatement(itemsetsSql)) {
      ResultSet rs = ps.executeQuery();
      while (rs.next()) {
        List<Attribute> recordValues = new ArrayList<>();
        for (Features attribute : attributes) {
          recordValues.add(new Attribute(attribute, rs.getString(attribute.getLabel())));
        }
        dataset.add(recordValues);
      }
    }
    return dataset;
  }

  /**
   * Create all itemsets of size 1 (all distinct attribute values for each column)
   * determine the support for each of the values
   * if the support is greater than the minimum support, then keep
   * generate all valid combinations between remaining attribute values
   * (i.e. combine values from different columns only,
   * can't create an itemset within the same attribute column)
   * <p>
   * continue this process until the frequent itemset no longer exists in the data
   *
   * @throws Exception
   */
  private void train() throws Exception {

    int trainingTotal = getTotalCount(trainingDataTable);

    List<Features> completeItemset = new ArrayList<>();
    completeItemset.addAll(attributes);

    List<List<Attribute>> actualItemsets = loadItemsets(trainingDataTable);

    List<List<Attribute>> candidateItemsets = new ArrayList<>();
    Map<List<Attribute>, Double> candidateItemsetFrequency = new LinkedHashMap<>();

    // generate the initial support for C1
    for (Features attribute : attributes) {
      Map<String, Double> attributeValueProbablities = getAttributeValProbabilities(trainingDataTable, attribute.getLabel(), trainingTotal);

      for (String val : attributeValueProbablities.keySet()) {
        List<Attribute> itemset = Collections.singletonList(new Attribute(attribute, val));
        candidateItemsets.add(itemset);
        candidateItemsetFrequency.put(itemset, attributeValueProbablities.get(val));
      }
    }

    List<List<Attribute>> resultItemset;

    while (!candidateItemsets.isEmpty()) {

      // remove infrequent itemsets that are less than the minimum support
      Map<List<Attribute>, Double> checkFrequency = candidateItemsetFrequency;
      List<List<Attribute>> frequentItemsets = candidateItemsets.stream().filter(i -> checkFrequency.containsKey(i) && checkFrequency.get(i) >= minSupport).collect(Collectors.toList());

      // generate all combinations of valid itemsets from remaining
      resultItemset = new ArrayList<>();
      int startIdx = 0;
      for (List<Attribute> itemset : frequentItemsets) {
        generateItemsets(frequentItemsets, resultItemset, startIdx, 0, itemset.size(), itemset);
        startIdx++;
      }

      candidateItemsets = resultItemset.stream().map(l -> l.stream().map(Attribute::copy).collect(Collectors.toList())).collect(Collectors.toList());
      candidateItemsetFrequency = new LinkedHashMap<>();
      for (List<Attribute> itemset : candidateItemsets) {

        int freq = actualItemsets.stream().filter(i -> Collections.indexOfSubList(i, itemset) != -1).collect(Collectors.toList()).size();
        if (freq > 0) {
          candidateItemsetFrequency.put(itemset, (double) freq / trainingTotal);
        }
      }
    }
  }

  void generateItemsets(List<List<Attribute>> itemsets, List<List<Attribute>> resultItemset, int startIdx, int depth, int startSize, List<Attribute> current) {

    if (resultItemset.contains(current)) {
      return;
    }

    if (current.size() == (depth + 1 + startSize)) {
      resultItemset.add(current);
      return;
    }

    // FIXME: need to only generate additional lists if the two start with the same or size = 1
    for (int i = startIdx; i < itemsets.size(); ++i) {
      List<Attribute> addList = itemsets.get(i);
      if (!addList.equals(current) && current.stream().noneMatch(a -> a.feature.equals(addList.get(depth).feature))) {
        List<Attribute> newCurrent = new ArrayList<>();
        newCurrent.addAll(current);
        newCurrent.add(addList.get(depth));
        generateItemsets(itemsets, resultItemset, startIdx, depth, startSize, newCurrent);
      }
    }
  }

}
