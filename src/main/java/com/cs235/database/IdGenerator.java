package com.cs235.database;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Random;

public final class IdGenerator {

  private static final Random random = new SecureRandom();
  private static final int NUM_BITS = 130;
  private static final int RADIX = 32;

  IdGenerator() {
    throw new UnsupportedOperationException();
  }

  public static String generate(String prefix) {
    return prefix + new BigInteger(NUM_BITS, random).toString(RADIX);
  }
}
