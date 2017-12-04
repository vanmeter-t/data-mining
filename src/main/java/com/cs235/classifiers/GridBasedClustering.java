package com.cs235.classifiers;

import com.cs235.Main;
import com.cs235.database.SQLUtils;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.WktImportFlags;
import com.esri.sds.util.SQLUtil;
import com.esri.sds.util.XSSPrevention;
import com.opencsv.CSVWriter;
import org.json.JSONException;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class GridBasedClustering {

  private static final Double BOX_SIZE = 0.035;
  private static final String COL_X = "x";
  private static final String COL_Y = "y";
  private static final String COL_OID = "oid";
  private static final String COL_COUNT = "binCount";

  private final String tableName;
  private final String outFile;

  public GridBasedClustering(String tableName, String outFile) {
    this.tableName = tableName;
    this.outFile = outFile;
  }

  public static List<GridCluster> generateBinData(String tableName) throws Exception {
    String binQuery = new StringBuilder(String.format("SELECT COUNT(*) AS %s, CAST(ROW_NUMBER() OVER () as INT) AS %s, ", SQLUtils.escapeIdentifier(COL_COUNT), SQLUtils.escapeIdentifier(COL_OID)))
      .append(String.format(" floor(%s/%.20f) AS %s, ", SQLUtils.escapeIdentifier(COL_X), BOX_SIZE, SQLUtils.escapeIdentifier(COL_X)))
      .append(String.format(" floor(%s/%.20f) AS %s ", SQLUtils.escapeIdentifier(COL_Y), BOX_SIZE, SQLUtils.escapeIdentifier(COL_Y)))
      .append(String.format(" FROM %s ", tableName))
      .append(String.format(" WHERE %s IS NOT NULL AND %s IS NOT NULL ", SQLUtils.escapeIdentifier(COL_X), SQLUtils.escapeIdentifier(COL_Y)))
      .append(String.format(" GROUP BY floor(%s/%.20f), floor(%s/%.20f) ", SQLUtils.escapeIdentifier(COL_X), BOX_SIZE, SQLUtils.escapeIdentifier(COL_Y), BOX_SIZE))
      .toString();

    try (Connection connection = DriverManager.getConnection(Main.POSTGRES_URL);
         PreparedStatement ps = connection.prepareStatement(binQuery)) {
      // calculate the gid based clusters to select the top N dense areas
      return createGenerateBinOutput(ps.executeQuery());
    }
  }

  public static List<GridCluster> createGenerateBinOutput(ResultSet rs) throws SQLException, JSONException {
    List<GridCluster> clusters = new ArrayList<>();

    GridCluster cluster;
    for (XSSPrevention xssPrevention = new XSSPrevention(); rs.next(); clusters.add(cluster)) {
      int fldIdx = 0;
      cluster = new GridCluster();

      try {
        int oid = Integer.parseInt(SQLUtil.getValue(rs, ++fldIdx, Types.BIGINT).toString());
        int count = Integer.parseInt(SQLUtil.getValue(rs, ++fldIdx, Types.BIGINT).toString());
        Double xVal = Double.parseDouble(SQLUtil.getValue(rs, ++fldIdx, Types.BIGINT).toString());
        Double yVal = Double.parseDouble(SQLUtil.getValue(rs, ++fldIdx, Types.BIGINT).toString());

        cluster.oid = oid;
        cluster.count = count;
        cluster.x = xVal;
        cluster.y = yVal;

        Double xMin = xVal * BOX_SIZE;
        Double yMin = yVal * BOX_SIZE;
        Double xMax = (xVal + 1) * BOX_SIZE;
        Double yMax = (yVal + 1) * BOX_SIZE;

        cluster.geometryWkt = String.format("polygon((%f %f, %f %f, %f %f, %f %f, %f %f))",
          xMin, yMin,
          xMax, yMin,
          xMax, yMax,
          xMin, yMax,
          xMin, yMin);

        cluster.geometry = GeometryEngine.geometryFromWkt(cluster.geometryWkt, WktImportFlags.wktImportDefaults, Geometry.Type.Polygon);
      } catch (NullPointerException ex) {
        // continue
      }
    }

    return clusters;
  }

  public List<GridBasedClustering.GridCluster> execute() throws Exception {
    List<GridBasedClustering.GridCluster> gridClusters = GridBasedClustering.generateBinData(tableName);

    // write the results to file
    try (FileOutputStream outStream = new FileOutputStream(outFile);
         CSVWriter out = new CSVWriter(new OutputStreamWriter(outStream, StandardCharsets.UTF_8), '\t', CSVWriter.DEFAULT_QUOTE_CHARACTER, '\\')) {
      gridClusters.stream()
        .map(cluster -> new String[]{cluster.oid.toString(), cluster.count.toString(), cluster.geometryWkt})
        .forEach(out::writeNext);
    }
    return gridClusters;
  }

  public static class GridCluster {
    public Integer oid;
    public Integer count;
    public Double x;
    public Double y;
    public Geometry geometry;
    public String geometryWkt;
  }

}

