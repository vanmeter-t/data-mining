package com.cs235.classifiers;

import com.cs235.Attribute;
import com.cs235.Features;
import com.cs235.TreeNode;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class DecisionTree extends Classifier {

  public DecisionTree(String tableName) {
    super(tableName);
  }

  /**
   * get the mode (most frequent value) from the list
   *
   * @param list
   * @return
   */
  public static String mode(List<String> list) {
    int max = 0;
    String modeValue = null;
    Map<String, AtomicInteger> map = new HashMap<>();
    for (String s : list) {
      if (map.containsKey(s)) {
        map.get(s).incrementAndGet();
      } else {
        map.put(s, new AtomicInteger(1));
      }

      if (map.get(s).get() > max) {
        modeValue = s;
        max = map.get(s).get();
      }
    }
    return modeValue;
  }

  /**
   * execute the Decision Tree classifier
   * break up the datasets into two, generate the tree
   * determine the accuracy of the tree with the test dataset
   *
   * @return
   * @throws Exception
   */
  @Override
  public String execute() throws Exception {
    createTrainingTestSets(tableName);

    // get all data
    List<List<Attribute>> allItemsets = loadItemsets(trainingDataTable);

    TreeNode root = train(allItemsets);

    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    String json = gson.toJson(root);

    try (BufferedWriter writer = new BufferedWriter(new FileWriter("out/tree.json"))) {
      writer.write(json);
    }

    Double accuracy = executeOnTestData(root);

    return String.format("\n\n Decision Tree Accuracy:\n\n%s", accuracy.toString());

  }

  /**
   * trace the Tree on the test dataset to determine accuracy
   *
   * @param root
   * @return
   * @throws Exception
   */
  public Double executeOnTestData(TreeNode root) throws Exception {
    int classifiedProperly = 0;
    int totalTestData = getTotalCount(testDataTable);

    List<List<Attribute>> allItemsets = loadItemsets(testDataTable);
    for (List<Attribute> item : allItemsets) {
      String classifiedSeverity = classify(root, item);
      Attribute a = item.stream().filter(f -> f.feature.equals(Features.SEVERITY_COLUMN)).findFirst().get();
      if (a.value.equals(classifiedSeverity)) {
        classifiedProperly++;
      }
    }
    return (double) classifiedProperly / totalTestData;
  }

  /**
   * for the particular item, get the Severity classification from the tree
   *
   * @param root
   * @param item
   * @return
   */
  public String classify(TreeNode root, List<Attribute> item) {
    TreeNode node = root;
    while (!node.children.isEmpty()) { // get to the leaf node
      Features parentFeat = node.feature;
      Optional<Attribute> a = item.stream().filter(f -> f.feature.equals(parentFeat)).findFirst();
      if (a.isPresent()) {
        Optional<TreeNode> newNode = node.children.stream()
          .filter(c -> (c.parentValue != null && c.parentValue.equals(a.get().value)) || (c.parentValue == null && a.get().value == null))
          .findFirst();

        if (newNode.isPresent()) {
          node = newNode.get();
        } else {
          return "INVALID_SEVERITY";
        }
      } else {
        return "INVALID_SEVERITY";
      }
    }
    return node.value;
  }

  public TreeNode train(List<List<Attribute>> trainingData) {
    List<Features> features = new ArrayList<>();
    features.addAll(attributes);

    return buildTree(trainingData, features, null);
  }

  /**
   * Build the Decision Tree using Information Gain Entropy(Purity) for splitting
   * The Feature with the lowest entropy will split the dataset and generate nodes for all values
   * The next level will again determine the best split based on entropy until we are at a leaf node
   * the leaf node is the Severity Attribute value
   *
   * @param actualItemsets
   * @param features
   * @param prevSplitVal
   * @return
   */
  private TreeNode buildTree(List<List<Attribute>> actualItemsets, List<Features> features, String prevSplitVal) {

    // all same severity value
    List<String> distinctValues = actualItemsets.stream()
      .map(i -> i.stream().filter(a -> a.feature.equals(Features.SEVERITY_COLUMN)).findFirst().get().value)
      .distinct().collect(Collectors.toList());

    boolean homogenous = distinctValues.size() == 1;
    if (homogenous) {
      return new TreeNode(prevSplitVal, Features.SEVERITY_COLUMN, distinctValues.get(0));
    }

    boolean stop = features.isEmpty();
    if (stop) {
      // get the mode value
      List<String> severityValues = actualItemsets.stream()
        .map(i -> i.stream().filter(a -> a.feature.equals(Features.SEVERITY_COLUMN)).findFirst().get().value)
        .collect(Collectors.toList());
      return new TreeNode(prevSplitVal, Features.SEVERITY_COLUMN, mode(severityValues));
    }

    Features splitOn = getSplitFeature(actualItemsets, features);
    Map<String, List<List<Attribute>>> splitItemsets = splitOnFeature(splitOn, actualItemsets);

    List<Features> newFeatures = features.stream().filter(f -> !f.equals(splitOn)).collect(Collectors.toList());
    TreeNode node = new TreeNode(prevSplitVal, splitOn, prevSplitVal);

    for (Map.Entry<String, List<List<Attribute>>> splitSet : splitItemsets.entrySet()) { // add children to current node according to split
      if (splitSet.getValue().isEmpty()) {
        // get the mode value
        List<String> severityValues = actualItemsets.stream()
          .map(i -> i.stream().filter(a -> a.feature.equals(Features.SEVERITY_COLUMN)).findFirst().get().value)
          .collect(Collectors.toList());
        node.children.add(new TreeNode(prevSplitVal, Features.SEVERITY_COLUMN, mode(severityValues)));
      } else {
        node.children.add(buildTree(splitSet.getValue(), newFeatures, splitSet.getKey()));
      }
    }

    return node;
  }

  private Features getSplitFeature(List<List<Attribute>> actualItemsets, List<Features> features) {
    Double impurity = 1.0;
    Features splitOn = null;
    for (Features f : features) {
      Map<String, List<List<Attribute>>> splitItemsets = splitOnFeature(f, actualItemsets);
      double splitImpurity = splitItemsets.values().stream().filter(list -> !list.isEmpty())
        .mapToDouble(list -> entropyImpurity(list.stream().map(i -> i.stream()
          .filter(feat -> feat.feature.equals(Features.SEVERITY_COLUMN)).findFirst().get()).collect(Collectors.toList())))
        .average().getAsDouble();
      if (splitImpurity < impurity) {
        impurity = splitImpurity;
        splitOn = f;
      }

    }
    return splitOn;
  }

  private Map<String, List<List<Attribute>>> splitOnFeature(Features feature, List<List<Attribute>> actualItemsets) {
    Map<String, List<List<Attribute>>> splitItemsets = new LinkedHashMap<>();
    for (List<Attribute> itemset : actualItemsets) {
      Attribute attr = itemset.stream().filter(a -> a.feature.equals(feature)).findFirst().get();
      if (splitItemsets.containsKey(attr.value)) {
        splitItemsets.get(attr.value).add(itemset);
      } else {
        splitItemsets.put(attr.value, new ArrayList<List<Attribute>>() {{
          add(itemset);
        }});
      }
    }
    return splitItemsets;
  }

  /**
   * calculate the entropy of the data from the split (the purity)  0.0 means all are the same (pure)
   *
   * @param data
   * @return
   */
  public Double entropyImpurity(List<Attribute> data) {
    List<String> values = data.stream().map(r -> r.value).distinct().collect(Collectors.toList());
    if (values.size() > 1) {
      double p = (double) data.parallelStream().filter(d -> d.value.equals(values.get(0))).count() / data.size();
      return -1.0 * p * Math.log(p) - ((1.0 - p) * Math.log(1.0 - p));
    } else if (values.size() == 1) {
      return 0.0;
    } else {
      throw new RuntimeException();
    }
  }


}
