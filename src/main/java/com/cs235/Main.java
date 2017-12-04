package com.cs235;

import com.cs235.classifiers.GridBasedClustering;
import com.cs235.classifiers.NaiveBayesClassifier;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

public class Main {

  public static String SEVERITY_COLUMN = "collision_severity";
  public static String OID_COLUMN = "objectid";

  public static String WEATHER_COLUMN = "weather_1";
  public static String ALCOHOL_COLUMN = "alcohol_involved";
  public static String TIME_CAT_COLUMN = "timecat";
  public static String COL_TYPE_COLUMN = "type_of_collision";
  public static String ROAD_SURF_COLUMN = "road_surface";
  public static String ROAD_COND_COLUMN = "road_cond_1";
  public static String LIGHTING_COLUMN = "lighting";

  public static List<String> attributes = new ArrayList<String>() {{
    add(WEATHER_COLUMN);
    add(ALCOHOL_COLUMN);
    add(TIME_CAT_COLUMN);
    add(COL_TYPE_COLUMN);
    add(ROAD_SURF_COLUMN);
    add(ROAD_COND_COLUMN);
    add(LIGHTING_COLUMN);
  }};


  public static void main(String[] args) throws Exception {

    File file = new File(args[0]);

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(args[1]))) {

      // import data into the database
      String tableName = CSVImporter.importToDatabaseTable(file);

      GridBasedClustering gridBasedClustering = new GridBasedClustering(tableName, args[2]);
      List<GridBasedClustering.GridCluster> gridClusters = gridBasedClustering.execute();

      NaiveBayesClassifier naiveBayesClassifier = new NaiveBayesClassifier(tableName);
      String results = naiveBayesClassifier.execute();
      writer.write(results);


      //PostgresSQL.dropTable(tableName);
    }

    System.exit(0); //success
  }


}
