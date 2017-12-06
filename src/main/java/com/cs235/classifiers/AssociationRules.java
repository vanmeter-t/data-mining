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

  private static final double minSupport = 0.2;
  private static final double minConfidence = 0.75;

  private static final List<Features> associationAttributes = new ArrayList<Features>() {{
    add(Features.SEVERITY_COLUMN);
    add(Features.WEATHER_COLUMN);
    add(Features.ALCOHOL_COLUMN);
    add(Features.TIME_CAT_COLUMN);
    add(Features.COL_TYPE_COLUMN);
    add(Features.ROAD_SURF_COLUMN);
    add(Features.ROAD_COND_COLUMN);
    add(Features.LIGHTING_COLUMN);
  }};

  public AssociationRules(String tableName) {
    super(tableName);
  }

  public static List<List<Attribute>> generateItemsets(List<List<Attribute>> itemsets, int depth) {
    List<List<Attribute>> resultItemset = new ArrayList<>();

    if (depth == -1) {
      for (int i = 0; i < itemsets.size() - 1; i++) {
        for (int j = i + 1; j < itemsets.size(); j++) {
          // if not from the same category column
          Attribute left = itemsets.get(i).get(0);
          Attribute right = itemsets.get(j).get(0);
          if (!left.feature.equals(right.feature)) {
            List<Attribute> combined = new ArrayList<>();
            combined.add(left);
            combined.add(right);
            resultItemset.add(combined);
          }
        }
      }
    } else {
      for (int i = 0; i < itemsets.size() - 1; i++) {
        for (int j = i + 1; j < itemsets.size(); j++) {

          List<Attribute> left = itemsets.get(i).subList(0, depth + 1);
          Attribute leftTail = itemsets.get(i).get(depth + 1);

          List<Attribute> right = itemsets.get(j).subList(0, depth + 1);
          Attribute rightTail = itemsets.get(j).get(depth + 1);

          // if not from the same category column but the sublist match
          // i.e. [a.1, b.1] , [a.1, c.1] -> [a.1, b.1, c.1] but [a.1, b.1] , [a.1, b.2] !-> [a.1, b.1, b.1]  (b category covered twice)
          if (left.containsAll(right) && !leftTail.feature.equals(rightTail.feature)) {
            List<Attribute> combined = new ArrayList<>();
            combined.addAll(itemsets.get(i));
            combined.add(rightTail);
            resultItemset.add(combined);
          }
        }
      }
    }

    return resultItemset;
  }

  private static List<List<Attribute>> loadItemsets(String tableName) throws Exception {
    List<List<Attribute>> dataset = new ArrayList<>();

    String itemsetsSql = new StringTemplate("SELECT ${fields} FROM ${tableName}")
      .put("fields", associationAttributes.stream().map(Features::getLabel).map(SQLUtils::escapeIdentifier).collect(Collectors.joining(",")))
      .put("tableName", SQLUtils.escapeIdentifier(tableName)).build();

    try (Connection connection = DriverManager.getConnection(Main.POSTGRES_URL);
         PreparedStatement ps = connection.prepareStatement(itemsetsSql)) {
      ResultSet rs = ps.executeQuery();
      while (rs.next()) {
        List<Attribute> recordValues = new ArrayList<>();
        for (Features attribute : associationAttributes) {
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
  private Map<List<Attribute>, Double> train(List<List<Attribute>> actualItemsets) throws Exception {

    int tableTotal = getTotalCount(trainingDataTable);

    List<Features> completeItemset = new ArrayList<>();
    completeItemset.addAll(associationAttributes);

    List<List<Attribute>> candidateItemsets = new ArrayList<>();
    Map<List<Attribute>, Double> candidateItemsetFrequency = new LinkedHashMap<>();

    // generate the initial support for C1
    for (Features attribute : associationAttributes) {
      Map<String, Double> attributeValueProbablities = getAttributeValProbabilities(trainingDataTable, attribute.getLabel(), tableTotal);

      for (String val : attributeValueProbablities.keySet()) {
        List<Attribute> itemset = Collections.singletonList(new Attribute(attribute, val));
        candidateItemsets.add(itemset);
        candidateItemsetFrequency.put(itemset, attributeValueProbablities.get(val));
      }
    }

    int depth = -1;
    Map<List<Attribute>, Double> resultItemset = new LinkedHashMap<>();

    // remove infrequent itemsets that are less than the minimum support
    Map<List<Attribute>, Double> checkFrequency = candidateItemsetFrequency;
    List<List<Attribute>> frequentItemsets = candidateItemsets.stream().filter(i -> checkFrequency.containsKey(i) && checkFrequency.get(i) >= minSupport).collect(Collectors.toList());

    // execute algorithm, generating permutations of frequent itemsets
    while (!candidateItemsets.isEmpty() && depth + 2 != associationAttributes.size() && !frequentItemsets.isEmpty()) {

      // generate all combinations of valid itemsets from remaining
      List<List<Attribute>> generatedItemset = generateItemsets(frequentItemsets, depth);
      candidateItemsets = generatedItemset.stream().map(l -> l.stream().map(Attribute::copy).collect(Collectors.toList())).collect(Collectors.toList());
      candidateItemsetFrequency = new LinkedHashMap<>();
      for (List<Attribute> itemset : candidateItemsets) {
        int freq = actualItemsets.stream().filter(record -> itemset.stream().allMatch(record::contains)).collect(Collectors.toList()).size();
        if (freq > 0) {
          candidateItemsetFrequency.put(itemset, (double) freq / tableTotal);
        }
      }

      // remove infrequent itemsets that are less than the minimum support
      Map<List<Attribute>, Double> checkFrequency2 = candidateItemsetFrequency;
      frequentItemsets = candidateItemsets.stream().filter(i -> checkFrequency2.containsKey(i) && checkFrequency2.get(i) >= minSupport).collect(Collectors.toList());

      if (!frequentItemsets.isEmpty()) {
        resultItemset = candidateItemsetFrequency;
      }

      depth++;
    }

    return resultItemset;
  }

  /**
   * for all subsets of the frequent itemsets, create an association rule and determine the support and confidence of the rules
   *
   * @param frequentItemsets
   * @return
   */
  public Map<Map<List<Attribute>, List<Attribute>>, RuleEvaluation> generateAssociationRules(List<List<Attribute>> actualItemsets, Map<List<Attribute>, Double> frequentItemsets) {
    Map<Map<List<Attribute>, List<Attribute>>, RuleEvaluation> associationRules = new LinkedHashMap<>();

    for (List<Attribute> itemset : frequentItemsets.keySet()) {
      boolean[] flags = new boolean[itemset.size()];
      for (int i = 0; i != itemset.size(); ) {
        List<Attribute> a = new ArrayList<>(), b = new ArrayList<>();
        for (int j = 0; j < itemset.size(); j++) {
          if (flags[j]) a.add(itemset.get(j));
          else b.add(itemset.get(j));
        }
        for (i = 0; i < itemset.size() && !(flags[i] = !flags[i]); i++) ;
        if (!a.isEmpty() && !b.isEmpty()) {

          int freqA = actualItemsets.stream().filter(record -> a.stream().allMatch(record::contains)).collect(Collectors.toList()).size();
          int freqItemset = actualItemsets.stream().filter(record -> itemset.stream().allMatch(record::contains)).collect(Collectors.toList()).size();

          Map<List<Attribute>, List<Attribute>> associationRule = new LinkedHashMap<>();
          associationRule.put(a, b);
          Double support = (double) freqItemset / actualItemsets.size();
          Double confidence = (double) freqItemset / freqA;
          if (confidence > minConfidence && support > minSupport) {
            associationRules.put(associationRule, new RuleEvaluation(support, confidence));
          }
        }
      }
    }

    return associationRules;
  }

  @Override
  public String execute() throws Exception {
    createTrainingTestSets(tableName);

    List<List<Attribute>> actualItemsets = loadItemsets(trainingDataTable);

    Map<List<Attribute>, Double> frequentItemsets = train(actualItemsets);

    Map<Map<List<Attribute>, List<Attribute>>, RuleEvaluation> associationRules = generateAssociationRules(actualItemsets, frequentItemsets);

    StringBuilder out = new StringBuilder();
    for (Map<List<Attribute>, List<Attribute>> rule : associationRules.keySet()) {

      out.append(String.format("{%s} -> {%s} \n(%s, %s)",
        rule.keySet().stream().findFirst().get().stream()
          .map(attribute -> String.format("%s.%s", attribute.feature, attribute.value)).collect(Collectors.joining(",")),
        rule.keySet().stream().findFirst().get().stream()
          .map(attribute -> String.format("%s.%s", attribute.feature, attribute.value)).collect(Collectors.joining(",")),
        associationRules.get(rule).support,
        associationRules.get(rule).confidence));
      out.append("\n\n");
    }

    return String.format("\n\n Association Apriori Frequent Itemsets:\n\n%s", out.toString());
  }

  public class RuleEvaluation {
    public Double support;
    public Double confidence;

    public RuleEvaluation(Double s, Double c) {
      support = s;
      confidence = c;
    }
  }

}
