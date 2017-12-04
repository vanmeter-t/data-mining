package com.cs235;

import com.cs235.database.IdGenerator;
import com.cs235.database.SQLUtils;
import com.cs235.database.StringTemplate;
import com.opencsv.CSVReader;
import org.apache.commons.lang3.math.NumberUtils;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.*;
import java.util.stream.Collectors;

public class CSVImporter {

  private static final String CREATE_TABLE = "CREATE TABLE ${name} (${fields})";
  private static final String COPY_TABLE = "COPY ${name} (${fields}) FROM STDIN CSV ${constraints}";
  private static final String FIELD = "${name} ${type}${constraints}";

  private static final Integer OVER_BIG_INT = 2_147_483_647;
  private static final Integer UNDER_BIG_INT = -2_147_483_648;

  public static FieldType getType(String thisStr) {
    String value = thisStr;
    if (value != null) {
      value = value.trim();
      if (value.isEmpty() || "null".equalsIgnoreCase(value)) {
        value = null;
      }
    }

    if (value != null && NumberUtils.isNumber(value)) {
      try {
        Integer.valueOf(value); // make sure this is an integer and not a double
        return FieldType.fieldTypeInteger;
      } catch (Exception e) {
        try {
          Double v = Double.parseDouble(value);
          if (!Double.isNaN(v) && !Double.isInfinite(v)) {
            if (v == Math.rint(v) && (v > OVER_BIG_INT || v < UNDER_BIG_INT)) {
              return FieldType.fieldTypeBigInt;
            } else {
              return FieldType.fieldTypeFloat;
            }
          } else {
            return FieldType.fieldTypeText;
          }
        } catch (Exception ex) {
          // do nothing
        }
      }
    }
    return FieldType.fieldTypeText;
  }

  public static String importToDatabaseTable(File file) throws Exception {

    String tableName = IdGenerator.generate("dataset_");

    List<String> fieldsForCreateTable = new ArrayList<>();
    List<String> fieldsForCopyTable = new ArrayList<>();

    Map<String, FieldType> headers = new LinkedHashMap<>();
    try (
      Reader fileReader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);
      CSVReader csvReader = new CSVReader(fileReader)) {

      String[] rowValues = csvReader.readNext();
      List<String> headerRow = Arrays.asList(rowValues);

      rowValues = csvReader.readNext();
      List<String> firstRecordRow = Arrays.asList(rowValues);

      for (int i = 0; i < headerRow.size(); i++) {
        headers.put(headerRow.get(i).toLowerCase().trim(), getType(firstRecordRow.get(i)));
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    headers.entrySet().stream().map(t -> String.format("%s %s", t.getKey(), t.getValue().toDBFieldType())).collect(Collectors.joining(","));

    for (Map.Entry<String, FieldType> entrySet : headers.entrySet()) {
      fieldsForCreateTable.add(new StringTemplate(FIELD)
        .put("name", SQLUtils.escapeIdentifier(entrySet.getKey()))
        .put("type", entrySet.getValue().toDBFieldType())
        .put("constraints", "")
        .build());

      fieldsForCopyTable.add(new StringTemplate(FIELD)
        .put("name", SQLUtils.escapeIdentifier(entrySet.getKey()))
        .put("type", "")
        .put("constraints", "")
        .build());
    }

    String createTableSql = new StringTemplate(CREATE_TABLE)
      .put("name", SQLUtils.escapeIdentifier(tableName))
      .put("fields", fieldsForCreateTable.stream().collect(Collectors.joining(",")))
      .build();

    try (Connection connection = DriverManager.getConnection(Main.POSTGRES_URL);
         PreparedStatement ps = connection.prepareStatement(createTableSql)) {
      ps.execute();
    }

    String copyTableSql = new StringTemplate(COPY_TABLE)
      .put("name", SQLUtils.escapeIdentifier(tableName))
      .put("fields", fieldsForCopyTable.stream().collect(Collectors.joining(",")))
      .put("constraints", "HEADER DELIMITER ',' QUOTE '\"' ESCAPE E'\\\\' ")
      .build();

    try (Connection connection = DriverManager.getConnection(Main.POSTGRES_URL);
         FileInputStream fileInputStream = new FileInputStream(file.getAbsoluteFile());
         InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, StandardCharsets.UTF_8)) {
      CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
      copyManager.copyIn(copyTableSql, inputStreamReader);
    }

    return tableName;
  }

  public enum FieldType {
    fieldTypeText,
    fieldTypeInteger,
    fieldTypeBigInt,
    fieldTypeFloat,
    fieldTypeOID,
    fieldTypeDate,
    fieldTypePercentage;

    public String toDBFieldType() {
      String dbFieldType;
      switch (this) {
        case fieldTypeOID:
        case fieldTypeInteger:
        case fieldTypePercentage:
        case fieldTypeBigInt:
        case fieldTypeFloat:
          dbFieldType = "numeric(38,8)";
          break;
        case fieldTypeText:
          dbFieldType = "text";
          break;
        case fieldTypeDate:
          dbFieldType = "timestamp without time zone";
          break;
        default:
          dbFieldType = "text";
      }
      return dbFieldType;
    }
  }

}


