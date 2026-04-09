package io.cascadestore.lsm.benchmark;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

public class CascadeBenchmarkRunner {

  @Test
  public void runBenchmarks() throws RunnerException {
    System.out.println("Starting CascadeStore benchmarks...");
    runQuickBenchmark();
    System.out.println("CascadeStore benchmarks completed.");
  }

  public static void main(String[] args) throws RunnerException {
    runDefaultBenchmarks();
  }

  private static void runDefaultBenchmarks() throws RunnerException {
    // Build options for the benchmark runner
    Options options =
        new OptionsBuilder()
            // Include only CascadeBenchmark class
            .include(CascadeBenchmark.class.getSimpleName())
            // Warm up for 3 iterations, 1 second each
            .warmupIterations(3)
            .warmupTime(TimeValue.seconds(1))
            // Measure for 5 iterations, 1 second each
            .measurementIterations(5)
            .measurementTime(TimeValue.seconds(1))
            // Fork 1 JVM with 2GB heap
            .forks(1)
            .jvmArgs("-Xms2G", "-Xmx2G")
            // Output results to console
            .shouldDoGC(true)
            .shouldFailOnError(true)
            .resultFormat(ResultFormatType.TEXT)
            .result("lsm-store-benchmark-results.txt")
            .timeUnit(TimeUnit.MICROSECONDS)
            .build();

    // Run the benchmark
    new Runner(options).run();

    System.out.println("Benchmark completed. Results saved to lsm-store-benchmark-results.txt");
  }

  public static void runBenchmarks(
      int warmupIterations, int measurementIterations, int forks, String resultFile)
      throws RunnerException {
    Options options =
        new OptionsBuilder()
            .include(CascadeBenchmark.class.getSimpleName())
            .warmupIterations(warmupIterations)
            .warmupTime(TimeValue.seconds(1))
            .measurementIterations(measurementIterations)
            .measurementTime(TimeValue.seconds(1))
            .forks(forks)
            .jvmArgs("-Xms2G", "-Xmx2G")
            .shouldDoGC(true)
            .shouldFailOnError(true)
            .resultFormat(ResultFormatType.TEXT)
            .result(resultFile)
            .timeUnit(TimeUnit.MICROSECONDS)
            .build();

    new Runner(options).run();

    System.out.println("Benchmark completed. Results saved to " + resultFile);
  }

  public static void runQuickBenchmark() throws RunnerException {
    System.out.println("Running quick benchmark...");
    Options options =
        new OptionsBuilder()
            .include(CascadeBenchmark.class.getSimpleName())
            .warmupIterations(1)
            .warmupTime(TimeValue.seconds(1))
            .measurementIterations(1)
            .measurementTime(TimeValue.seconds(1))
            .forks(1)
            .jvmArgs("-Xms1G", "-Xmx1G")
            .shouldDoGC(true)
            .shouldFailOnError(true)
            .resultFormat(ResultFormatType.TEXT)
            .result("lsm-store-quick-benchmark-results.txt")
            .timeUnit(TimeUnit.MICROSECONDS)
            .build();

    new Runner(options).run();
    System.out.println(
        "Quick benchmark completed. Results saved to lsm-store-quick-benchmark-results.txt");
  }
}
