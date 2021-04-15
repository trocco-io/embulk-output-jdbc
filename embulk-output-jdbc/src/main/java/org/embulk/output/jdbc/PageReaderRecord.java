package org.embulk.output.jdbc;

import java.time.Instant;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import com.google.common.base.Function;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

/**
 * Record read by PageReader.
 * The class will save read records for retry.
 */
public class PageReaderRecord implements Record
{
    private static final CSVFormat DEFAULT_FORMAT = CSVFormat.DEFAULT.withNullString("null");
    private final PageReader pageReader;
    protected File readRecordsFile;
    protected CSVPrinter writer;
    private MemoryRecord lastRecord;

    public PageReaderRecord(PageReader pageReader) throws IOException
    {
        this.pageReader = pageReader;
        readRecordsFile = createTempFile();
        writer = openWriter(readRecordsFile);
    }

    public void setPage(Page page)
    {
        pageReader.setPage(page);
    }

    public boolean nextRecord() throws IOException
    {
        writeRow(lastRecord);
        lastRecord = null; // lastRecord will be created in next `save` method execution.
        return pageReader.nextRecord();
    }

    public boolean isNull(Column column)
    {
        return pageReader.isNull(column);
    }

    public boolean getBoolean(Column column)
    {
        return save(column, pageReader.getBoolean(column));
    }

    public long getLong(Column column)
    {
        return save(column, pageReader.getLong(column));
    }

    public double getDouble(Column column)
    {
        return save(column, pageReader.getDouble(column));
    }

    public String getString(Column column)
    {
        return save(column, pageReader.getString(column));
    }

    public Instant getTimestamp(Column column)
    {
        return save(column, pageReader.getTimestamp(column).getInstant());
    }

    public Value getJson(Column column)
    {
        return save(column, pageReader.getJson(column));
    }

    private <T> T save(Column column, T value)
    {
        if (lastRecord == null) {
            lastRecord = new MemoryRecord(pageReader.getSchema().getColumnCount());
        }
        lastRecord.setValue(column, value);
        return value;
    }


    public void clearReadRecords() throws IOException
    {
        close();
        readRecordsFile = createTempFile();
        writer = openWriter(readRecordsFile);
        lastRecord = null;
    }

    private void setReadRecords(File newRecordsFile) throws IOException
    {
        close();
        readRecordsFile = newRecordsFile;
        writer = openWriter(readRecordsFile);
    }

    protected File createTempFile() throws IOException
    {
        File f = File.createTempFile("embulk-output-jdbc-records-", ".csv");
        f.deleteOnExit();
        return f;
    }

    protected CSVParser openReader(File newFile) throws IOException
    {
        return CSVParser.parse(new FileReader(newFile), DEFAULT_FORMAT);
    }

    protected CSVPrinter openWriter(File newFile) throws IOException
    {
        return new CSVPrinter(new FileWriter(newFile, true), DEFAULT_FORMAT);
    }

    private void write(CSVPrinter writer, final Object value)
    {
        try {
            writer.print(value); // CSVPrint.print will check the situation of null, nullString settings, etc.
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected void close() throws IOException
    {
        if (writer != null) {
            writer.close();
            writer = null;
        }
        readRecordsFile = null;
    }

    protected void writeRow(MemoryRecord record) throws IOException
    {
        writeRow(writer, record);
    }

    protected void writeRow(CSVPrinter writer, MemoryRecord record) throws IOException
    {
        if (record == null) {
            return;
        }
        pageReader.getSchema().visitColumns(new ColumnVisitor()
        {
            @Override
            public void booleanColumn(Column column)
            {
                write(writer, record.getBoolean(column));
            }

            @Override
            public void longColumn(Column column)
            {
                write(writer, record.getLong(column));
            }

            @Override
            public void doubleColumn(Column column)
            {
                write(writer, record.getDouble(column));
            }

            @Override
            public void stringColumn(Column column)
            {
                write(writer, record.getString(column));
            }

            @Override
            public void timestampColumn(Column column)
            {
                write(writer, record.getTimestamp(column));
            }

            @Override
            public void jsonColumn(Column column)
            {
                write(writer, record.getJson(column));
            }
        });
        writer.println();
        writer.flush();
    }

    public void foreachRecord(Function<? super Record, Boolean> function) throws IOException
    {
        File tmpFile = createTempFile();
        try (CSVParser reader = openReader(readRecordsFile); CSVPrinter tmpWriter = openWriter(tmpFile)) {
            for (CSVRecord r : reader) {
                MemoryRecord record = new MemoryRecord(pageReader.getSchema().getColumnCount());
                pageReader.getSchema().visitColumns(new ColumnVisitor()
                {
                    @Override
                    public void booleanColumn(Column column)
                    {
                        setValue(record, column, r.get(column.getIndex()), Boolean.class);
                    }

                    @Override
                    public void longColumn(Column column)
                    {
                        setValue(record, column, r.get(column.getIndex()), Long.class);
                    }

                    @Override
                    public void doubleColumn(Column column)
                    {
                        setValue(record, column, r.get(column.getIndex()), Double.class);
                    }

                    @Override
                    public void stringColumn(Column column)
                    {
                        setValue(record, column, r.get(column.getIndex()), String.class);
                    }

                    @Override
                    public void timestampColumn(Column column)
                    {
                        setValue(record, column, r.get(column.getIndex()), Instant.class);
                    }

                    @Override
                    public void jsonColumn(Column column)
                    {
                        setValue(record, column, r.get(column.getIndex()), Value.class);
                    }
                });
                if (function.apply(record)) {
                    writeRow(tmpWriter, record);
                    tmpWriter.flush();
                }
            }
        }
        setReadRecords(tmpFile);
    }

    private void setValue(MemoryRecord record, Column column, String str, Class<?> obj)
    {
        if (str == null) {
            record.setValue(column, null);
            return;
        }
        Object value;
        if (obj == Boolean.class) {
            value = Boolean.valueOf(str);
        } else if (obj == Long.class) {
            value = Long.valueOf(str);
        } else if (obj == Double.class) {
            value = Double.valueOf(str);
        } else if (obj == Instant.class) {
            value = Instant.parse(str);
        } else if (obj == Value.class) {
            value = ValueFactory.newString(str);
        } else {
            value = str;
        }
        record.setValue(column, value);
    }
}
