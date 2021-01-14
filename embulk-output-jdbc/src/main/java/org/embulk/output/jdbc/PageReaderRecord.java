package org.embulk.output.jdbc;

import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.time.Instant;
import java.util.function.Consumer;

import org.embulk.spi.Column;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

/**
 * Record read by PageReader.
 * The class will save read records for retry.
 */
public class PageReaderRecord implements Record
{
    public static final char ITEM_DELIMITER = ',';
    private final PageReader pageReader;
    protected File readRecordsFile;
    protected BufferedWriter writer;
    protected BufferedReader reader;
    private MemoryRecord lastRecord;

    public PageReaderRecord(PageReader pageReader) throws IOException
    {
        this.pageReader = pageReader;
        readRecordsFile = createTempFile();
        writer = openWriter(readRecordsFile);
    }

    protected File createTempFile() throws IOException
    {
        File f = File.createTempFile("embulk-output-jdbc-records-", ".csv");
        f.deleteOnExit();
        return f;
    }

    protected BufferedWriter openWriter(File newFile) throws IOException
    {
        return new BufferedWriter(new FileWriter(newFile));
    }

    protected BufferedReader openReader(File file) throws IOException
    {
        return new BufferedReader(new FileReader(file));
    }

    protected void close() throws IOException
    {
        if (writer != null) {
            writer.close();
            writer = null;
        }
        if (reader != null) {
            reader.close();
            reader = null;
        }
        readRecordsFile.delete();
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

    protected void writeRow(MemoryRecord record) throws IOException
    {
        if (record == null) {
            return;
        }
        int columnCount = pageReader.getSchema().getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            Column c = pageReader.getSchema().getColumn(i);
            switch (c.getType().getName()) {
                case "boolean":
                    writer.write(Boolean.toString(lastRecord.getBoolean(c)));
                    break;
                case "long":
                    writer.write(Long.toString(lastRecord.getLong(c)));
                    break;
                case "double":
                    writer.write(Double.toString(lastRecord.getDouble(c)));
                    break;
                case "json":
                    writer.write(lastRecord.getJson(c).toString());
                    break;
                case "timestamp":
                    writer.write(lastRecord.getTimestamp(c).toString());
                    break;
                case "string":
                    writer.write(lastRecord.getString(c));
                    break;
            }
            if (i + 1 < columnCount) { // write comma to separate each column from others
                writer.write(ITEM_DELIMITER);
            }
        }
        writer.newLine();
        writer.flush();
    }

    public void foreachRecord(Consumer<? super Record> comsumer) throws IOException
    {
      if (reader != null) {
          reader.close();
          reader = null;
      }
      reader = new BufferedReader(new FileReader(readRecordsFile));
      try {
          int columnCount = pageReader.getSchema().getColumnCount();
          MemoryRecord record = new MemoryRecord(columnCount);
          String row = null;
          while ((row = reader.readLine()) != null) {
              String[] values = row.split(String.valueOf(ITEM_DELIMITER));
              for (int i = 0; i < columnCount; i++) {
                  Column c = pageReader.getSchema().getColumn(i);
                  switch (c.getType().getName()) {
                      case "boolean":
                          record.setValue(c, Boolean.valueOf(values[i]));
                          break;
                      case "long":
                          record.setValue(c, Long.valueOf(values[i]));
                          break;
                      case "double":
                          record.setValue(c, Double.valueOf(values[i]));
                          break;
                      case "json":
                          record.setValue(c, ValueFactory.newString(values[i]));
                          break;
                      case "timestamp":
                          record.setValue(c, Instant.parse(values[i]));
                          break;
                      case "string":
                          record.setValue(c, values[i]);
                          break;
                  }
              }
              comsumer.accept(record);
          }
      } finally {
            reader.close();
      }
    }

    public void clearReadRecords() throws IOException
    {
        close();
        readRecordsFile = createTempFile();
        writer = openWriter(readRecordsFile);
        lastRecord = null;
    }
}
