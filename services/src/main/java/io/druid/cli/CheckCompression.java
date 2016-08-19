/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.cli;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.ByteSink;
import com.google.common.io.Files;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.druid.granularity.QueryGranularities;
import io.druid.query.DruidProcessingConfig;
import io.druid.segment.Cursor;
import io.druid.segment.IndexIO;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexStorageAdapter;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.CompressedLongsIndexedSupplier;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.CompressionFactory;
import io.druid.segment.data.IndexedLongs;
import io.druid.segment.data.LongSupplierSerializer;
import io.druid.segment.data.TmpFileIOPeon;
import org.apache.commons.io.FileUtils;
import org.joda.time.chrono.ISOChronology;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Command(
    name = "check-compression",
    description = "Check performance of different compression and encoding on segment"
)
public class CheckCompression extends GuiceRunnable
{
  private static final Logger log = new Logger(CheckCompression.class);

  public static final List<CompressedObjectStrategy.CompressionStrategy> compressions =
      ImmutableList.of(
          CompressedObjectStrategy.CompressionStrategy.LZ4,
          CompressedObjectStrategy.CompressionStrategy.NONE
      );
  public static final List<CompressionFactory.LongEncodingStrategy> encodings =
      ImmutableList.of(CompressionFactory.LongEncodingStrategy.AUTO, CompressionFactory.LongEncodingStrategy.LONGS);

  public CheckCompression()
  {
    super(log);
  }

  @Option(
      name = {"-d", "--directory"},
      title = "directory",
      description = "Directory containing segment data.",
      required = true)
  public String directory;

  @Option(
      name = {"-o", "--out"},
      title = "file",
      description = "File to write to, or omit to write to stdout.",
      required = false)
  public String outputFileName;

  @Option(
      name = {"-k", "--keep"},
      title = "keep data and compressed files, default to false",
      required = false)
  public boolean keepTemp = false;

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/tool");
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(9999);
            binder.bind(DruidProcessingConfig.class).toInstance(
                new DruidProcessingConfig()
                {
                  @Override
                  public String getFormatString()
                  {
                    return "processing-%s";
                  }

                  @Override
                  public int intermediateComputeSizeBytes()
                  {
                    return 100 * 1024 * 1024;
                  }

                  @Override
                  public int getNumThreads()
                  {
                    return 1;
                  }

                  @Override
                  public int columnCacheSizeBytes()
                  {
                    return 25 * 1024 * 1024;
                  }
                }
            );
            binder.bind(ColumnConfig.class).to(DruidProcessingConfig.class);
          }
        }
    );
  }

  @Override
  public void run()
  {
//    final List<String> longColumns = new ArrayList<>();
//    final List<File> tempFiles = new ArrayList<>();
//    final List<CompressionStats> allCompressionStats = new ArrayList<>();
//    final File tempDirectory = new File(directory, "temp");
//
//    final PrintStream out;
//
//    try {
//      out = outputFileName == null ? System.out : new PrintStream(outputFileName);
//      FileUtils.deleteDirectory(tempDirectory);
//    }
//    catch (IOException e) {
//      e.printStackTrace();
//      return;
//    }
//    tempDirectory.mkdir();
//
//    makeColumnDataFiles(longColumns, tempDirectory, tempFiles, out);
//
//    for (String column : longColumns) {
//      makeCompressedFiles(column, tempDirectory, allCompressionStats, tempFiles);
//    }

    //makeCompressedFiles("additions", new File(directory, "temp"), new ArrayList<CompressionStats>(), new ArrayList<File>());
    simpleBenchmark();
//
//    runBenchmark(allCompressionStats, tempDirectory);
//
//    for (CompressionStats compressionStats : allCompressionStats) {
//      out.println(compressionStats.toString());
//    }
//
//    if (!keepTemp) {
//      for (File tempFile : tempFiles) {
//        tempFile.delete();
//      }
//      tempDirectory.delete();
//    }
//
//    if (outputFileName != null) {
//      out.close();
//    }
  }

  private long simpleBenchmark() {

    String directory = "/Users/daveli/gitsegment/2016-08-15T15:00:00.000Z_2016-08-15T16:00:00.000Z/2016-08-15T15:00:15.484Z/1/temp/";
    String fileName = "additions-LZ4-LONGS";

    File dir = new File(directory);
    File compFile = new File(dir, fileName);
    ByteBuffer buffer = null;

    try {
      buffer = Files.map(compFile);
    } catch (Exception e) {
      Throwables.propagate(e);
    }
    IndexedLongs indexedLongs = CompressedLongsIndexedSupplier.fromByteBuffer(buffer, ByteOrder.nativeOrder()).get();

    long sum = 0;
    long startTime = System.nanoTime();
    for (int t = 0; t < 1000; t++) {
      for (int i = 0; i < indexedLongs.size(); i++) {
        sum += indexedLongs.get(i);
      }
    }
    System.out.println(System.nanoTime() - startTime);
    return sum;
  }

  private void makeColumnDataFiles(final List<String> longColumns, final File tempDirectory, final List<File> tempFiles, final PrintStream out) {
    final Injector injector = makeInjector();
    final IndexIO indexIO = injector.getInstance(IndexIO.class);
    try (final QueryableIndex index = indexIO.loadIndex(new File(directory))) {
      for (String columnName : index.getColumnNames()) {
        if (index.getColumn(columnName).getCapabilities().getType() == ValueType.LONG) {
          longColumns.add(columnName);
        }
      }
      final QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(index);
      final Sequence<Cursor> cursors = adapter.makeCursors(
          null,
          index.getDataInterval().withChronology(ISOChronology.getInstanceUTC()),
          QueryGranularities.ALL,
          false
      );

      cursors.accumulate(null, new Accumulator<Object, Cursor>()
      {
        @Override
        public Integer accumulate(Object accumulated, Cursor cursor)
        {
          try {
            List<LongColumnSelector> selectors = new ArrayList<>();
            List<Writer> writers = new ArrayList<>();

            for (String columnName : longColumns) {
              selectors.add(cursor.makeLongColumnSelector(columnName));
              File dataFile = new File(tempDirectory, columnName);
              Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dataFile)));
              writers.add(writer);
              tempFiles.add(dataFile);
            }

            int count = 0;
            while (!cursor.isDone()) {
              cursor.advance();
              for (int i = 0; i < selectors.size(); i++) {
                long value = selectors.get(i).get();
                writers.get(i).write(value + "\n");
              }
              count++;
            }
            out.println(count + " rows");
            out.printf("%-20s%-6s%-6s%10s%32s%32s\n", "column", "", "", "size(b)", "read continuous", "read skipping");

            for (Writer writer : writers) {
              writer.close();
            }
          }
          catch (IOException ex) {
            Throwables.propagate(ex);
          }

          return null;
        }
      });
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private void makeCompressedFiles(String column, File outputDir, List<CompressionStats> allCompressionStats, List<File> tempFiles) {
    for (CompressedObjectStrategy.CompressionStrategy compression : compressions) {
      for (CompressionFactory.LongEncodingStrategy encoding : encodings) {
        CompressionStats compressStats = new CompressionStats(column, compression, encoding);
        allCompressionStats.add(compressStats);
        File compFile = new File(outputDir, compressStats.getFileName());
        File dataFile = new File(outputDir, column);
        tempFiles.add(compFile);

        TmpFileIOPeon iopeon = new TmpFileIOPeon(true);
        LongSupplierSerializer writer = CompressionFactory.getLongSerializer(
            iopeon,
            "long",
            ByteOrder.nativeOrder(),
            encoding,
            compression
        );

        try {
          BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(dataFile)));
          try (FileChannel output = FileChannel.open(
              compFile.toPath(),
              StandardOpenOption.CREATE_NEW,
              StandardOpenOption.WRITE
          )) {
            writer.open();
            String line;
            while ((line = br.readLine()) != null) {
              writer.add(Long.parseLong(line));
            }
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            writer.closeAndConsolidate(
                new ByteSink()
                {
                  @Override
                  public OutputStream openStream() throws IOException
                  {
                    return baos;
                  }
                }
            );
            output.write(ByteBuffer.wrap(baos.toByteArray()));
          }
          finally {
            iopeon.cleanup();
            br.close();
          }
          compressStats.size = compFile.length();
        } catch (IOException ex) {
          throw Throwables.propagate(ex);
        }
      }
    }
  }

  private void runBenchmark(List<CompressionStats> allCompressionStats, File tempDirectory) {
    final List<String> compFiles = Lists.transform(
        allCompressionStats,
        new Function<CompressionStats, String>()
        {
          @Override
          public String apply(CompressionStats input)
          {
            return input.getFileName();
          }
        }
    );

    Options opt = new OptionsBuilder()
        .include(CompBenchmark.class.getName() + ".readContinuous")
        .include(CompBenchmark.class.getName() + ".readSkipping")
        .mode(Mode.AverageTime)
        .timeUnit(TimeUnit.MICROSECONDS)
        .warmupIterations(1)
        .measurementIterations(1)
        .forks(1)
        .param("directory", tempDirectory.getPath())
        .param("fileName", compFiles.toArray(new String[compFiles.size()]))
        .build();

    try {
      Collection<RunResult> results = new Runner(opt).run();
      for (RunResult result : results) {
        for (CompressionStats compStat : allCompressionStats) {
          if (result.getParams().getParam("fileName").equals(compStat.getFileName())) {
            System.out.println(result.getParams().getBenchmark());
            if (result.getParams().getBenchmark().equals(CompBenchmark.class.getName() + ".readContinuous")) {
              compStat.continuousResult = result.getAggregatedResult().getPrimaryResult().toString();
            } else {
              compStat.skippingResult = result.getAggregatedResult().getPrimaryResult().toString();
            }
          }
        }
      }
    }
    catch (RunnerException e) {
      e.printStackTrace();
    }
  }

  private static class CompressionStats
  {
    String column;
    CompressedObjectStrategy.CompressionStrategy compression;
    CompressionFactory.LongEncodingStrategy encoding;
    String fileName;
    long size;
    String continuousResult;
    String skippingResult;

    public CompressionStats(
        String column,
        CompressedObjectStrategy.CompressionStrategy compression,
        CompressionFactory.LongEncodingStrategy encoding
    )
    {
      this.column = column;
      this.compression = compression;
      this.encoding = encoding;
      this.fileName = column + "-" + compression.toString() + "-" + encoding.toString();
    }

    public String getFileName()
    {
      return fileName;
    }

    @Override
    public String toString()
    {
      return String.format("%-20s%-6s%-6s%10s%32s%32s", column, compression.toString(), encoding.toString(),
                           size, continuousResult, skippingResult
      );
    }
  }
}
