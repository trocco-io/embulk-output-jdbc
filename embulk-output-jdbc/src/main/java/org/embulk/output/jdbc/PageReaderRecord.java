package org.embulk.output.jdbc;

import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.time.Instant;
import java.util.function.Consumer;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

/**
 * Record read by PageReader.
 * The class will save read records for retry.
 */
public class PageReaderRecord implements Record
{
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

    public void setPage(Page page) {
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

    protected File createTempFile() throws IOException
    {
        File f = File.createTempFile("embulk-output-jdbc-records-", ".csv");
        f.deleteOnExit();
        return f;
    }

    protected CSVParser openReader(File newFile) throws IOException
    {
        return CSVParser.parse(new FileReader(newFile), CSVFormat.DEFAULT);
    }

    protected CSVPrinter openWriter(File newFile) throws IOException
    {
        return new CSVPrinter(new FileWriter(newFile), CSVFormat.DEFAULT);
    }

    private void write(String value)
    {
        try {
            writer.print(value);
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
        readRecordsFile.delete();
    }

    protected void writeRow(MemoryRecord record) throws IOException
    {
        if (record == null) {
            return;
        }
        pageReader.getSchema().visitColumns(new ColumnVisitor() {
            @Override
            public void booleanColumn(Column column) {
                write(Boolean.toString(lastRecord.getBoolean(column)));
            }

            @Override
            public void longColumn(Column column) {
                write(Long.toString(lastRecord.getLong(column)));
            }

            @Override
            public void doubleColumn(Column column) {
                write(Double.toString(lastRecord.getDouble(column)));
            }

            @Override
            public void stringColumn(Column column) {
                write(lastRecord.getString(column));
            }

            @Override
            public void timestampColumn(Column column) {
                write(lastRecord.getTimestamp(column).toString());
            }

            @Override
            public void jsonColumn(Column column) {
                write(lastRecord.getJson(column).toString());
            }
        });
        writer.println();
        writer.flush();
    }

    public void foreachRecord(Consumer<? super Record> comsumer) throws IOException
    {
        try (CSVParser reader = openReader(readRecordsFile)) {
            MemoryRecord record = new MemoryRecord(pageReader.getSchema().getColumnCount());
            for (CSVRecord r : reader) {
                pageReader.getSchema().visitColumns(new ColumnVisitor() {
                    @Override
                    public void booleanColumn(Column column) {
                        record.setValue(column, Boolean.valueOf(r.get(column.getIndex())));
                    }

                    @Override
                    public void longColumn(Column column) {
                        record.setValue(column, Long.valueOf(r.get(column.getIndex())));
                    }

                    @Override
                    public void doubleColumn(Column column) {
                        record.setValue(column, Double.valueOf(r.get(column.getIndex())));
                    }

                    @Override
                    public void stringColumn(Column column) {
                        record.setValue(column, r.get(column.getIndex()));
                    }

                    @Override
                    public void timestampColumn(Column column) {
                        record.setValue(column, Instant.parse(r.get(column.getIndex())));
                    }

                    @Override
                    public void jsonColumn(Column column) {
                        record.setValue(column, ValueFactory.newString(r.get(column.getIndex())));
                    }
                });
            }
        }
    }
}
