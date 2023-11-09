package org.apache.impala.catalog.iceberg;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.stream.IntStream;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.StructLike;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupedContentFilesTest {

  public static final String BASE_PATH = "hdfs://localhost:20500/test-warehouse/i01/data/a_trunc=%d/placeholder.parq";
  public static final GroupedContentFiles FILES = new GroupedContentFiles();

  private static final Logger LOG = LoggerFactory.getLogger(
      GroupedContentFilesTest.class);


  @BeforeClass
  public static void prepareData(){
    addFiles(FILES.dataFilesWithoutDeletes);
  }
  @Test
  public void testGroupBy() throws InterruptedException {
    Thread.sleep(8000);
    Set<Path> paths = FILES.groupByPartitionPath();
    paths.size();
  }

  private static void addFiles(List<DataFile> collection) {
    IntStream.range(0,5000000).forEach(integerValue -> {
      String path = String.format(BASE_PATH, integerValue / 10);
      collection.add(new TestDataFile(path));
    });
  }

  private static class TestDataFile implements DataFile {

    private CharSequence path;

    public TestDataFile(CharSequence path) {
      this.path = path;
    }

    @Override
    public Long pos() {
      return null;
    }

    @Override
    public int specId() {
      return 0;
    }

    @Override
    public CharSequence path() {
      return path;
    }

    @Override
    public FileFormat format() {
      return null;
    }

    @Override
    public StructLike partition() {
      return null;
    }

    @Override
    public long recordCount() {
      return 0;
    }

    @Override
    public long fileSizeInBytes() {
      return 0;
    }

    @Override
    public Map<Integer, Long> columnSizes() {
      return null;
    }

    @Override
    public Map<Integer, Long> valueCounts() {
      return null;
    }

    @Override
    public Map<Integer, Long> nullValueCounts() {
      return null;
    }

    @Override
    public Map<Integer, Long> nanValueCounts() {
      return null;
    }

    @Override
    public Map<Integer, ByteBuffer> lowerBounds() {
      return null;
    }

    @Override
    public Map<Integer, ByteBuffer> upperBounds() {
      return null;
    }

    @Override
    public ByteBuffer keyMetadata() {
      return null;
    }

    @Override
    public List<Long> splitOffsets() {
      return null;
    }

    @Override
    public DataFile copy() {
      return null;
    }

    @Override
    public DataFile copyWithoutStats() {
      return null;
    }
  }
}