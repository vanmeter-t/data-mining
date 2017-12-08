package com.cs235;

import com.cs235.classifiers.AssociationRules;
import com.cs235.classifiers.DecisionTree;
import com.cs235.classifiers.GridBasedClustering;
import com.cs235.classifiers.NaiveBayesClassifier;

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
   * @param args [0] [1] - PostgreSQL address | Input CSV dataset
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {

    System.out.println("Starting data mining:");

    POSTGRES_URL = String.format("jdbc:postgresql://%s", args[0]);

    File file = new File(args[1]);

    try (BufferedWriter writer = new BufferedWriter(new FileWriter("out/out.txt"))) {

      String tableName = CSVImporter.importToDatabaseTable(file);

      Timers timer = Timers.start();
      System.out.println("Grid Based Clustering started...");
      GridBasedClustering gridBasedClustering = new GridBasedClustering(tableName);
      gridBasedClustering.execute();
      System.out.println(String.format(" finished (%s ms)...", timer.elapsedMillis()));

      timer = Timers.start();
      System.out.println("Naive Bayes Classifier started...");
      NaiveBayesClassifier naiveBayesClassifier = new NaiveBayesClassifier(tableName);
      writer.write(naiveBayesClassifier.execute());
      System.out.println(String.format(" finished (%s ms)...", timer.elapsedMillis()));

      timer = Timers.start();
      System.out.println("Decision Tree Classifier started...");
      DecisionTree decisionTree = new DecisionTree(tableName);
      writer.write(decisionTree.execute());
      System.out.println(String.format(" finished (%s ms)...", timer.elapsedMillis()));

      timer = Timers.start();
      System.out.println("Apriori Association Rule Mining started...");
      AssociationRules associationRules = new AssociationRules(tableName);
      writer.write(associationRules.execute());
      System.out.println(String.format(" finished (%s ms)...", timer.elapsedMillis()));

    }

    System.out.println("---------- PROCESS COMPLETED ----------");

    System.exit(0); //success
  }


}
