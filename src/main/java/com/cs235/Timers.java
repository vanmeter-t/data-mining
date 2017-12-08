package com.cs235;

import java.time.Duration;

public final class Timers {

  final long starts;

  private Timers() {
    starts = System.nanoTime();
  }

  public static Timers start() {
    return new Timers();
  }

  public long elapsedMillis() {
    return (System.nanoTime() - starts) / 1_000_000L;
  }

  public String elapsed() {
    return Duration.ofMillis(elapsedMillis()).toString();
  }
}
