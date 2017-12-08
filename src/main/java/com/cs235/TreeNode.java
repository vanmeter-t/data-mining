package com.cs235;

import java.util.ArrayList;
import java.util.List;

public class TreeNode {

  public Features feature;
  public String value;
  public String parentValue;

  public List<TreeNode> children = new ArrayList<>();

  public TreeNode(String parent, Features feat, String val) {
    parentValue = parent;
    feature = feat;
    value = val;
  }
}


