package com.cs235.database;

import org.apache.commons.lang3.text.StrSubstitutor;

import java.util.HashMap;

public class StringTemplate {

  private final String template;
  private final HashMap<String, String> map;

  public StringTemplate(String template) {
    this.template = template;
    map = new HashMap<>();
  }

  public static StringTemplate format(String template) {
    return new StringTemplate(template);
  }

  public StringTemplate put(String key, String value) {
    map.put(key, value);
    return this;
  }

  public StringTemplate put(String key, StringBuilder value) {
    return put(key, value.toString());
  }

  public StringTemplate put(String key, int value) {
    return put(key, Integer.toString(value));
  }

  public StringTemplate put(String key, double value) {
    return put(key, Double.toString(value));
  }

  public String build() {
    return new StrSubstitutor(map).replace(template);
  }

  @Override
  public String toString() {
    return build();
  }
}
