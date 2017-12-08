package com.cs235;

import com.cs235.classifiers.DecisionTree;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class Main {

  public static String POSTGRES_URL;

  /**
   * Import the dataset into the database from a CSV file
   * Execute the clustering, and the three classifiers: Naive Bayes, Apriori Rule Association, Information Gain Decision Tree
   * Save the results to an our file and a cluster TSV file
   *
   * @param args [0] [1] [2] [3] - PostgreSQL address | Input CSV dataset | Output Results .txt file | Output clustering .tsv file
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {

    POSTGRES_URL = String.format("jdbc:postgresql://%s", args[0]);

    File file = new File(args[1]);

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(args[2]))) {

      String tableName = CSVImporter.importToDatabaseTable(file);

//      GridBasedClustering gridBasedClustering = new GridBasedClustering(tableName, args[3]);
//      gridBasedClustering.execute();
//
//      NaiveBayesClassifier naiveBayesClassifier = new NaiveBayesClassifier(tableName);
//      writer.write(naiveBayesClassifier.execute());

//      AssociationRules associationRules = new AssociationRules(tableName);
//      writer.write(associationRules.execute());

      DecisionTree decisionTree = new DecisionTree(tableName);
      writer.write(decisionTree.execute());
    }

    System.exit(0); //success
  }


}
