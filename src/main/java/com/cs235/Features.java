package com.cs235;

public enum Features {

  SEVERITY_COLUMN("collision_severity"),
  OID_COLUMN("objectid"),
  WEATHER_COLUMN("weather_1"),
  ALCOHOL_COLUMN("alcohol_involved"),
  TIME_CAT_COLUMN("timecat"),
  COL_TYPE_COLUMN("type_of_collision"),
  ROAD_SURF_COLUMN("road_surface"),
  ROAD_COND_COLUMN("road_cond_1"),
  LIGHTING_COLUMN("lighting");

  private final String label;

  Features(String label) {
    this.label = label;
  }

  public String getLabel() {
    return label;
  }
}
