/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.orc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.airlift.slice.Slice;
import io.trino.hive.orc.NullMemoryManager;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.Footer;
import io.trino.orc.metadata.OrcColumnId;
import io.trino.orc.metadata.statistics.IntegerStatistics;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Writable;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Map;

import static io.trino.orc.OrcReader.BATCH_SIZE_GROWTH_FACTOR;
import static io.trino.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.trino.orc.OrcReader.MAX_BATCH_SIZE;
import static io.trino.orc.OrcTester.Format.ORC_12;
import static io.trino.orc.OrcTester.READER_OPTIONS;
import static io.trino.orc.OrcTester.createCustomOrcRecordReader;
import static io.trino.orc.OrcTester.createOrcRecordWriter;
import static io.trino.orc.OrcTester.createSettableStructObjectInspector;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hive.ql.io.orc.CompressionKind.SNAPPY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

public class TestOrcReaderPositions
{
    @Test
    public void testEntireFile()
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            createMultiStripeFile(tempFile.getFile());

            try (OrcRecordReader reader = createCustomOrcRecordReader(tempFile, OrcPredicate.TRUE, BIGINT, MAX_BATCH_SIZE)) {
                assertThat(reader.getReaderRowCount()).isEqualTo(100);
                assertThat(reader.getReaderPosition()).isEqualTo(0);
                assertThat(reader.getFileRowCount()).isEqualTo(reader.getReaderRowCount());
                assertThat(reader.getFilePosition()).isEqualTo(reader.getReaderPosition());

                for (int i = 0; i < 5; i++) {
                    Page page = reader.nextPage().getPage();
                    assertThat(page.getPositionCount()).isEqualTo(20);
                    assertThat(reader.getReaderPosition()).isEqualTo(i * 20L);
                    assertThat(reader.getFilePosition()).isEqualTo(reader.getReaderPosition());
                    assertCurrentBatch(page, i);
                }

                assertThat(reader.nextPage()).isNull();
                assertThat(reader.getReaderPosition()).isEqualTo(100);
                assertThat(reader.getFilePosition()).isEqualTo(reader.getReaderPosition());
            }
        }
    }

    @Test
    public void testStripeSkipping()
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            createMultiStripeFile(tempFile.getFile());

            // test reading second and fourth stripes
            OrcPredicate predicate = (numberOfRows, allColumnStatistics) -> {
                if (numberOfRows == 100) {
                    return true;
                }
                IntegerStatistics stats = allColumnStatistics.get(new OrcColumnId(1)).getIntegerStatistics();
                return ((stats.getMin() == 60) && (stats.getMax() == 117)) ||
                        ((stats.getMin() == 180) && (stats.getMax() == 237));
            };

            try (OrcRecordReader reader = createCustomOrcRecordReader(tempFile, predicate, BIGINT, MAX_BATCH_SIZE)) {
                assertThat(reader.getFileRowCount()).isEqualTo(100);
                assertThat(reader.getReaderRowCount()).isEqualTo(40);
                assertThat(reader.getFilePosition()).isEqualTo(0);
                assertThat(reader.getReaderPosition()).isEqualTo(0);

                // second stripe
                Page page = reader.nextPage().getPage();
                assertThat(page.getPositionCount()).isEqualTo(20);
                assertThat(reader.getReaderPosition()).isEqualTo(0);
                assertThat(reader.getFilePosition()).isEqualTo(20);
                assertCurrentBatch(page, 1);

                // fourth stripe
                page = reader.nextPage().getPage();
                assertThat(page.getPositionCount()).isEqualTo(20);
                assertThat(reader.getReaderPosition()).isEqualTo(20);
                assertThat(reader.getFilePosition()).isEqualTo(60);
                assertCurrentBatch(page, 3);

                assertThat(reader.nextPage()).isNull();
                assertThat(reader.getReaderPosition()).isEqualTo(40);
                assertThat(reader.getFilePosition()).isEqualTo(100);
            }
        }
    }

    @Test
    public void testRowGroupSkipping()
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            // create single strip file with multiple row groups
            int rowCount = 142_000;
            createSequentialFile(tempFile.getFile(), rowCount);

            // test reading two row groups from middle of file
            OrcPredicate predicate = (numberOfRows, allColumnStatistics) -> {
                if (numberOfRows == rowCount) {
                    return true;
                }
                IntegerStatistics stats = allColumnStatistics.get(new OrcColumnId(1)).getIntegerStatistics();
                return (stats.getMin() == 50_000) || (stats.getMin() == 60_000);
            };

            try (OrcRecordReader reader = createCustomOrcRecordReader(tempFile, predicate, BIGINT, MAX_BATCH_SIZE)) {
                assertThat(reader.getFileRowCount()).isEqualTo(rowCount);
                assertThat(reader.getReaderRowCount()).isEqualTo(rowCount);
                assertThat(reader.getFilePosition()).isEqualTo(0);
                assertThat(reader.getReaderPosition()).isEqualTo(0);

                long position = 50_000;
                while (true) {
                    SourcePage sourcePage = reader.nextPage();
                    if (sourcePage == null) {
                        break;
                    }
                    Page page = sourcePage.getPage();

                    Block block = page.getBlock(0);
                    for (int i = 0; i < block.getPositionCount(); i++) {
                        assertThat(BIGINT.getLong(block, i)).isEqualTo(position + i);
                    }

                    assertThat(reader.getFilePosition()).isEqualTo(position);
                    assertThat(reader.getReaderPosition()).isEqualTo(position);
                    position += page.getPositionCount();
                }

                assertThat(position).isEqualTo(70_000);
                assertThat(reader.getFilePosition()).isEqualTo(rowCount);
                assertThat(reader.getReaderPosition()).isEqualTo(rowCount);
            }
        }
    }

    @Test
    public void testBatchSizesForVariableWidth()
            throws Exception
    {
        // the test creates a table with one column and 10 row groups (i.e., 100K rows)
        // the 1st row group has strings with each of length 300,
        // the 2nd row group has strings with each of length 600,
        // the 3rd row group has strings with each of length 900, and so on
        // the test is to show when loading those strings,
        // we are first bounded by MAX_BATCH_SIZE = 1024 rows because 1024 X 900B < 1MB
        // then bounded by MAX_BLOCK_SIZE = 1MB because 1024 X 1200B > 1MB
        try (TempFile tempFile = new TempFile()) {
            // create single strip file with multiple row groups
            int rowsInRowGroup = 10000;
            int rowGroupCounts = 10;
            int baseStringBytes = 300;
            int rowCount = rowsInRowGroup * rowGroupCounts;
            createGrowingSequentialFile(tempFile.getFile(), rowCount, rowsInRowGroup, baseStringBytes);

            try (OrcRecordReader reader = createCustomOrcRecordReader(tempFile, OrcPredicate.TRUE, VARCHAR, MAX_BATCH_SIZE)) {
                assertThat(reader.getFileRowCount()).isEqualTo(rowCount);
                assertThat(reader.getReaderRowCount()).isEqualTo(rowCount);
                assertThat(reader.getFilePosition()).isEqualTo(0);
                assertThat(reader.getReaderPosition()).isEqualTo(0);

                // each value's length = original value length + 4 bytes to denote offset + 1 byte to denote if null
                int currentStringBytes = baseStringBytes + Integer.BYTES + Byte.BYTES;
                int rowCountsInCurrentRowGroup = 0;
                while (true) {
                    SourcePage sourcePage = reader.nextPage();
                    if (sourcePage == null) {
                        break;
                    }
                    Page page = sourcePage.getPage();

                    rowCountsInCurrentRowGroup += page.getPositionCount();

                    Block block = page.getBlock(0);
                    if (MAX_BATCH_SIZE * (long) currentStringBytes <= READER_OPTIONS.getMaxBlockSize().toBytes()) {
                        // Either we are bounded by 1024 rows per batch, or it is the last batch in the row group
                        // For the first 3 row groups, the strings are of length 300, 600, and 900 respectively
                        // So the loaded data is bounded by MAX_BATCH_SIZE
                        assertThat(block.getPositionCount() == MAX_BATCH_SIZE || rowCountsInCurrentRowGroup == rowsInRowGroup).isTrue();
                    }
                    else {
                        // Either we are bounded by 1MB per batch, or it is the last batch in the row group
                        // From the 4th row group, the strings are have length > 1200
                        // So the loaded data is bounded by MAX_BLOCK_SIZE
                        if (rowCountsInCurrentRowGroup != rowsInRowGroup) {
                            int maxRows = toIntExact(READER_OPTIONS.getMaxBlockSize().toBytes() / currentStringBytes);
                            assertThat(block.getPositionCount()).isLessThanOrEqualTo(maxRows);
                        }
                    }

                    if (rowCountsInCurrentRowGroup == rowsInRowGroup) {
                        rowCountsInCurrentRowGroup = 0;
                        currentStringBytes += baseStringBytes;
                    }
                    else if (rowCountsInCurrentRowGroup > rowsInRowGroup) {
                        fail("read more rows in the current row group");
                    }
                }
            }
        }
    }

    @Test
    public void testBatchSizesForFixedWidth()
            throws Exception
    {
        // the test creates a table with one column and 10 row groups
        // the each row group has bigints of length 8 in bytes,
        // the test is to show that the loaded data is always bounded by MAX_BATCH_SIZE because 1024 X 8B < 1MB
        try (TempFile tempFile = new TempFile()) {
            // create single strip file with multiple row groups
            int rowsInRowGroup = 10_000;
            int rowGroupCounts = 10;
            int rowCount = rowsInRowGroup * rowGroupCounts;
            createSequentialFile(tempFile.getFile(), rowCount);

            try (OrcRecordReader reader = createCustomOrcRecordReader(tempFile, OrcPredicate.TRUE, BIGINT, MAX_BATCH_SIZE)) {
                assertThat(reader.getFileRowCount()).isEqualTo(rowCount);
                assertThat(reader.getReaderRowCount()).isEqualTo(rowCount);
                assertThat(reader.getFilePosition()).isEqualTo(0);
                assertThat(reader.getReaderPosition()).isEqualTo(0);

                int rowCountsInCurrentRowGroup = 0;
                while (true) {
                    SourcePage sourcePage = reader.nextPage();
                    if (sourcePage == null) {
                        break;
                    }
                    Page page = sourcePage.getPage();
                    rowCountsInCurrentRowGroup += page.getPositionCount();

                    Block block = page.getBlock(0);
                    // 8 bytes per row; 1024 row at most given 1024 X 8B < 1MB
                    assertThat(block.getPositionCount() == MAX_BATCH_SIZE || rowCountsInCurrentRowGroup == rowsInRowGroup).isTrue();

                    if (rowCountsInCurrentRowGroup == rowsInRowGroup) {
                        rowCountsInCurrentRowGroup = 0;
                    }
                    else if (rowCountsInCurrentRowGroup > rowsInRowGroup) {
                        fail("read more rows in the current row group");
                    }
                }
            }
        }
    }

    @Test
    public void testReadUserMetadata()
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            Map<String, String> metadata = ImmutableMap.of(
                    "a", "ala",
                    "b", "ma",
                    "c", "kota");
            createFileWithOnlyUserMetadata(tempFile.getFile(), metadata);

            OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), READER_OPTIONS);
            OrcReader orcReader = OrcReader.createOrcReader(orcDataSource, READER_OPTIONS)
                    .orElseThrow(() -> new RuntimeException("File is empty"));
            Footer footer = orcReader.getFooter();
            Map<String, String> readMetadata = Maps.transformValues(footer.getUserMetadata(), Slice::toStringAscii);
            assertThat(readMetadata).isEqualTo(metadata);
        }
    }

    @Test
    public void testBatchSizeGrowth()
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            // Create a file with 5 stripes of 20 rows each.
            createMultiStripeFile(tempFile.getFile());

            try (OrcRecordReader reader = createCustomOrcRecordReader(tempFile, OrcPredicate.TRUE, BIGINT, INITIAL_BATCH_SIZE)) {
                assertThat(reader.getReaderRowCount()).isEqualTo(100);
                assertThat(reader.getReaderPosition()).isEqualTo(0);
                assertThat(reader.getFileRowCount()).isEqualTo(reader.getReaderRowCount());
                assertThat(reader.getFilePosition()).isEqualTo(reader.getReaderPosition());

                // The batch size should start from INITIAL_BATCH_SIZE and grow by BATCH_SIZE_GROWTH_FACTOR.
                // For INITIAL_BATCH_SIZE = 1 and BATCH_SIZE_GROWTH_FACTOR = 2, the batchSize sequence should be
                // 1, 2, 4, 8, 5, 20, 20, 20, 20
                int totalReadRows = 0;
                int nextBatchSize = INITIAL_BATCH_SIZE;
                int expectedBatchSize = INITIAL_BATCH_SIZE;
                int rowCountsInCurrentRowGroup = 0;
                while (true) {
                    SourcePage sourcePage = reader.nextPage();
                    if (sourcePage == null) {
                        break;
                    }
                    Page page = sourcePage.getPage();

                    assertThat(page.getPositionCount()).isEqualTo(expectedBatchSize);
                    assertThat(reader.getReaderPosition()).isEqualTo(totalReadRows);
                    assertThat(reader.getFilePosition()).isEqualTo(reader.getReaderPosition());
                    assertCurrentBatch(page, (int) reader.getReaderPosition(), page.getPositionCount());

                    if (nextBatchSize > 20 - rowCountsInCurrentRowGroup) {
                        nextBatchSize *= BATCH_SIZE_GROWTH_FACTOR;
                    }
                    else {
                        nextBatchSize = page.getPositionCount() * BATCH_SIZE_GROWTH_FACTOR;
                    }
                    rowCountsInCurrentRowGroup += page.getPositionCount();
                    totalReadRows += page.getPositionCount();
                    if (rowCountsInCurrentRowGroup == 20) {
                        rowCountsInCurrentRowGroup = 0;
                    }
                    else if (rowCountsInCurrentRowGroup > 20) {
                        fail("read more rows in the current row group");
                    }

                    expectedBatchSize = min(min(nextBatchSize, MAX_BATCH_SIZE), 20 - rowCountsInCurrentRowGroup);
                }

                assertThat(reader.getReaderPosition()).isEqualTo(100);
                assertThat(reader.getFilePosition()).isEqualTo(reader.getReaderPosition());
            }
        }
    }

    private static void assertCurrentBatch(Page page, int rowIndex, int batchSize)
    {
        Block block = page.getBlock(0);
        for (int i = 0; i < batchSize; i++) {
            assertThat(BIGINT.getLong(block, i)).isEqualTo((rowIndex + i) * 3L);
        }
    }

    private static void assertCurrentBatch(Page page, int stripe)
    {
        Block block = page.getBlock(0);
        for (int i = 0; i < 20; i++) {
            assertThat(BIGINT.getLong(block, i)).isEqualTo(((stripe * 20L) + i) * 3);
        }
    }

    // write 5 stripes of 20 values each: (0,3,6,..,57), (60,..,117), .., (..297)
    private static void createMultiStripeFile(File file)
            throws IOException, ReflectiveOperationException, SerDeException
    {
        FileSinkOperator.RecordWriter writer = createOrcRecordWriter(file, ORC_12, CompressionKind.NONE, BIGINT);

        Serializer serde = new OrcSerde();
        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector("test", BIGINT);
        Object row = objectInspector.create();
        StructField field = objectInspector.getAllStructFieldRefs().get(0);

        for (int i = 0; i < 300; i += 3) {
            if ((i > 0) && (i % 60 == 0)) {
                flushWriter(writer);
            }

            objectInspector.setStructFieldData(row, field, (long) i);
            Writable record = serde.serialize(row, objectInspector);
            writer.write(record);
        }

        writer.close(false);
    }

    private static void createFileWithOnlyUserMetadata(File file, Map<String, String> metadata)
            throws IOException
    {
        Configuration conf = new Configuration(false);
        OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf)
                .memory(new NullMemoryManager())
                .inspector(createSettableStructObjectInspector("test", BIGINT))
                .compress(SNAPPY);
        Writer writer = OrcFile.createWriter(new Path(file.toURI()), writerOptions);
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            writer.addUserMetadata(entry.getKey(), ByteBuffer.wrap(entry.getValue().getBytes(UTF_8)));
        }
        writer.close();
    }

    private static void flushWriter(FileSinkOperator.RecordWriter writer)
            throws IOException, ReflectiveOperationException
    {
        Field field = OrcOutputFormat.class.getClassLoader()
                .loadClass(OrcOutputFormat.class.getName() + "$OrcRecordWriter")
                .getDeclaredField("writer");
        field.setAccessible(true);
        ((Writer) field.get(writer)).writeIntermediateFooter();
    }

    private static void createSequentialFile(File file, int count)
            throws IOException, SerDeException
    {
        FileSinkOperator.RecordWriter writer = createOrcRecordWriter(file, ORC_12, CompressionKind.NONE, BIGINT);

        Serializer serde = new OrcSerde();
        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector("test", BIGINT);
        Object row = objectInspector.create();
        StructField field = objectInspector.getAllStructFieldRefs().get(0);

        for (int i = 0; i < count; i++) {
            objectInspector.setStructFieldData(row, field, (long) i);
            Writable record = serde.serialize(row, objectInspector);
            writer.write(record);
        }

        writer.close(false);
    }

    private static void createGrowingSequentialFile(File file, int count, int step, int initialLength)
            throws IOException, SerDeException
    {
        FileSinkOperator.RecordWriter writer = createOrcRecordWriter(file, ORC_12, CompressionKind.NONE, VARCHAR);

        Serializer serde = new OrcSerde();
        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector("test", VARCHAR);
        Object row = objectInspector.create();
        StructField field = objectInspector.getAllStructFieldRefs().get(0);

        StringBuilder builder = new StringBuilder();
        builder.append("0".repeat(Math.max(0, initialLength)));
        String seedString = builder.toString();

        // gradually grow the length of a cell
        int previousLength = initialLength;
        for (int i = 0; i < count; i++) {
            if ((i / step + 1) * initialLength > previousLength) {
                previousLength = (i / step + 1) * initialLength;
                builder.append(seedString);
            }
            objectInspector.setStructFieldData(row, field, builder.toString());
            Writable record = serde.serialize(row, objectInspector);
            writer.write(record);
        }

        writer.close(false);
    }
}
