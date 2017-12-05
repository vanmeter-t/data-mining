package com.cs235;

public class Attribute {
  public Features feature;
  public String value;

  public Attribute(Features feature, String value) {
    this.feature = feature;
    this.value = value;
  }

  public Attribute copy() {
    return new Attribute(feature, value);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    return feature.equals(((Attribute) obj).feature) && ((value != null && value.equals(((Attribute) obj).value)) || (value == null && ((Attribute) obj).value == null));
  }
}
