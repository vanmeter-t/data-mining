package com.cs235.classifiers;

import com.cs235.Attribute;
import com.cs235.Features;

import java.util.*;
import java.util.stream.Collectors;

public class AssociationRules extends Classifier {

  private static final double minSupport = 0.09;
  private static final double minConfidence = 0.40;

  public AssociationRules(String tableName) {
    super(tableName);
  }

  /**
   * for the itemsets, generate all the new possible itemsets by joining
   * this is a join where only the last value must be from a different feature
   * not from the same category column but the sublist match
   * [a.1, b.1] , [a.1, c.1] -> [a.1, b.1, c.1] (GOOD) (b & c different category)
   * [a.1, b.1] , [a.1, b.2] !-> [a.1, b.1, b.1] (BAD) (b category covered twice)
   *
   * @param itemsets
   * @param depth
   * @return
   */
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
    completeItemset.addAll(allAttributes);

    List<List<Attribute>> candidateItemsets = new ArrayList<>();
    Map<List<Attribute>, Double> candidateItemsetFrequency = new LinkedHashMap<>();

    // generate the initial support for C1
    for (Features attribute : allAttributes) {
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
    while (!candidateItemsets.isEmpty() && depth + 2 != allAttributes.size() && !frequentItemsets.isEmpty()) {

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

    // split the array into two sub-arrays of all variations
    // only keep if the right array is a singular array of the Severity value
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
          if (b.size() == 1 && b.get(0).feature.equals(Features.SEVERITY_COLUMN) && confidence >= minConfidence) {
            associationRules.put(associationRule, new RuleEvaluation(support, confidence));
          }
        }
      }
    }

    return associationRules;
  }

  /**
   * execute the Apriori Assocation Rule Mining technique
   * find the frequent itemsets utilizing the specific columns within the dataset
   * for each freqent itemset, generate all possible rule combinations with "Severity" as the singular column on the right
   * only accept itemsets and rules satisfying the minSupport and minConfidence
   *
   * @return
   * @throws Exception
   */
  @Override
  public String execute() throws Exception {
    createTrainingTestSets(tableName);

    // get all data
    List<List<Attribute>> actualItemsets = loadItemsets(trainingDataTable);

    // find the frequent itemsets
    Map<List<Attribute>, Double> frequentItemsets = train(actualItemsets);

    // generate the association rules
    Map<Map<List<Attribute>, List<Attribute>>, RuleEvaluation> associationRules = generateAssociationRules(actualItemsets, frequentItemsets);

    // print out the results
    StringBuilder out = new StringBuilder();
    for (Map<List<Attribute>, List<Attribute>> rule : associationRules.keySet()) {
      out.append(String.format("{%s} -> {%s} \n(%s, %s)",
        rule.keySet().stream().findFirst().get().stream()
          .map(attribute -> String.format("%s.%s", attribute.feature, attribute.value)).collect(Collectors.joining(",")),
        rule.values().stream().findFirst().get().stream()
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
