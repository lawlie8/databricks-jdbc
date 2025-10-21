package com.jayant;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class TestReporter {
  private final List<ComparisonResult> results = new ArrayList<>();
  private final Path outputPath;

  public TestReporter(Path outputPath) {
    this.outputPath = outputPath;
  }

  public void addResult(ComparisonResult result) {
    results.add(result);
  }

  public void generateReport() throws IOException {
    try (FileWriter writer = new FileWriter(outputPath.toFile())) {
      boolean hasDifferences = false;
      for (ComparisonResult result : results) {
        if (result.hasDifferences()) {
          hasDifferences = true;
          writer.write(result.toString());
          writer.write("\n============================\n\n");
        }
      }

      if (!hasDifferences) {
        writer.write("No differences found between JDBC drivers.\n");
      }
    }
  }
}
