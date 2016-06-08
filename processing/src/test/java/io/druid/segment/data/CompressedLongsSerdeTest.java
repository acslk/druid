package io.druid.segment.data;

import com.google.common.base.Supplier;
import com.google.common.io.ByteSink;
import com.google.common.primitives.Longs;
import com.metamx.common.guava.CloseQuietly;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@RunWith(Parameterized.class)
public class CompressedLongsSerdeTest
{
  @Parameterized.Parameters(name="{0} {1}")
  public static Iterable<Object[]> compressionStrategies()
  {
    List<Object[]> data = new ArrayList<>();
    for (CompressionFactory.CompressionFormat format : CompressionFactory.CompressionFormat.values()) {
      data.add(new Object[]{format, ByteOrder.BIG_ENDIAN});
      data.add(new Object[]{format, ByteOrder.LITTLE_ENDIAN});
    }
    return data;
  }

  protected final CompressionFactory.CompressionFormat compressionFormat;
  protected final ByteOrder order;

  private final long values0 [] = {};
  private final long values1 [] = {0, 1, 1, 0, 1, 1, 1, 1, 0, 0, 1, 1};
  private final long values2 [] = {12, 5, 2, 9, 3, 2, 5, 1, 0, 6, 13, 10, 15};
  private final long values3 [] = {1, 1, 1, 1, 1, 11, 11, 11, 11};
  private final long values4 [] = {200, 200, 200, 401, 200, 301, 200, 200, 200, 404, 200, 200, 200, 200};
  private final long values5 [] = {123, 632, 12, 39, 536, 0, 1023, 52, 777, 526, 214, 562, 823, 346};
  private final long values6 [] = {1000000, 1000001, 1000002, 1000003, 1000004, 1000005, 1000006, 1000007, 1000008};
  private final long values7 [] = {Long.MAX_VALUE, Long.MIN_VALUE, 12378, -12718243, -1236213, 12743153, 21364375452L,
                             65487435436632L, -43734526234564L};

  public CompressedLongsSerdeTest(CompressionFactory.CompressionFormat compressionFormat, ByteOrder order)
  {
    this.compressionFormat = compressionFormat;
    this.order = order;
  }

  @Test
  public void testValueSerde() throws Exception
  {
    testWithValues(values0);
    testWithValues(values1);
    testWithValues(values2);
    testWithValues(values3);
    testWithValues(values4);
    testWithValues(values5);
    testWithValues(values6);
    testWithValues(values7);
  }

  @Test
  public void testChunkSerde() throws Exception
  {
    long chunk [] = new long[10000];
    for (int i = 0; i < 10000; i++) {
      chunk[i] = i;
    }
    testWithValues(chunk);
  }

  public void testWithValues(long[] values) throws Exception
  {
    LongSupplierSerializer serializer = compressionFormat.getLongSerializer(new IOPeonForTesting(), "test", order);
    serializer.open();

    for (long value : values) {
      serializer.add(value);
    }
    Assert.assertEquals(values.length, serializer.size());

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    serializer.closeAndConsolidate(
        new ByteSink()
        {
          @Override
          public OutputStream openStream() throws IOException
          {
            return baos;
          }
        }
    );
    Assert.assertEquals(baos.size(), serializer.getSerializedSize());
    CompressedLongsIndexedSupplier supplier = CompressedLongsIndexedSupplier
        .fromByteBuffer(ByteBuffer.wrap(baos.toByteArray()), order);
    IndexedLongs longs = supplier.get();

    assertIndexMatchesVals(longs, values);
    for (int i = 0; i < 10; i++) {
      int a = (int) (Math.random() * values.length);
      int b = (int) (Math.random() * values.length);
      int start = a < b ? a : b;
      int end = a < b ? b : a;
      tryFill(longs, values, start, end - start);
    }
    testSupplierSerde(supplier, values);
    testConcurrentThreadReads(supplier, longs, values);

    longs.close();
  }

  private void tryFill(IndexedLongs indexed, long[] vals, final int startIndex, final int size)
  {
    long[] filled = new long[size];
    indexed.fill(startIndex, filled);

    for (int i = startIndex; i < filled.length; i++) {
      Assert.assertEquals(vals[i + startIndex], filled[i]);
    }
  }

  private void assertIndexMatchesVals(IndexedLongs indexed, long[] vals)
  {
    Assert.assertEquals(vals.length, indexed.size());

    // sequential access
    int[] indices = new int[vals.length];
    for (int i = 0; i < indexed.size(); ++i) {
      Assert.assertEquals(vals[i], indexed.get(i), 0.0);
      indices[i] = i;
    }

    Collections.shuffle(Arrays.asList(indices));
    // random access
    for (int i = 0; i < indexed.size(); ++i) {
      int k = indices[i];
      Assert.assertEquals(vals[k], indexed.get(k), 0.0);
    }
  }

  private void testSupplierSerde(CompressedLongsIndexedSupplier supplier, long[] vals) throws IOException
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    supplier.writeToChannel(Channels.newChannel(baos));

    final byte[] bytes = baos.toByteArray();
    Assert.assertEquals(supplier.getSerializedSize(), bytes.length);
    CompressedLongsIndexedSupplier anotherSupplier = CompressedLongsIndexedSupplier.fromByteBuffer(
        ByteBuffer.wrap(bytes), order
    );
    IndexedLongs indexed = anotherSupplier.get();
    assertIndexMatchesVals(indexed, vals);
  }

  // This test attempts to cause a race condition with the DirectByteBuffers, it's non-deterministic in causing it,
  // which sucks but I can't think of a way to deterministically cause it...
  private void testConcurrentThreadReads(final Supplier<IndexedLongs> supplier,
                                        final IndexedLongs indexed, final long[] vals) throws Exception
  {
    final AtomicReference<String> reason = new AtomicReference<String>("none");

    final int numRuns = 1000;
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch stopLatch = new CountDownLatch(2);
    final AtomicBoolean failureHappened = new AtomicBoolean(false);
    new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        try {
          startLatch.await();
        }
        catch (InterruptedException e) {
          failureHappened.set(true);
          reason.set("interrupt.");
          stopLatch.countDown();
          return;
        }

        try {
          for (int i = 0; i < numRuns; ++i) {
            for (int j = 0; j < indexed.size(); ++j) {
              final long val = vals[j];
              final long indexedVal = indexed.get(j);
              if (Longs.compare(val, indexedVal) != 0) {
                failureHappened.set(true);
                reason.set(String.format("Thread1[%d]: %d != %d", j, val, indexedVal));
                stopLatch.countDown();
                return;
              }
            }
          }
        }
        catch (Exception e) {
          e.printStackTrace();
          failureHappened.set(true);
          reason.set(e.getMessage());
        }

        stopLatch.countDown();
      }
    }).start();

    final IndexedLongs indexed2 = supplier.get();
    try {
      new Thread(new Runnable()
      {
        @Override
        public void run()
        {
          try {
            startLatch.await();
          }
          catch (InterruptedException e) {
            stopLatch.countDown();
            return;
          }

          try {
            for (int i = 0; i < numRuns; ++i) {
              for (int j = indexed2.size() - 1; j >= 0; --j) {
                final long val = vals[j];
                final long indexedVal = indexed2.get(j);
                if (Longs.compare(val, indexedVal) != 0) {
                  failureHappened.set(true);
                  reason.set(String.format("Thread2[%d]: %d != %d", j, val, indexedVal));
                  stopLatch.countDown();
                  return;
                }
              }
            }
          }
          catch (Exception e) {
            e.printStackTrace();
            reason.set(e.getMessage());
            failureHappened.set(true);
          }

          stopLatch.countDown();
        }
      }).start();

      startLatch.countDown();

      stopLatch.await();
    }
    finally {
      CloseQuietly.close(indexed2);
    }

    if (failureHappened.get()) {
      Assert.fail("Failure happened.  Reason: " + reason.get());
    }
  }
}
