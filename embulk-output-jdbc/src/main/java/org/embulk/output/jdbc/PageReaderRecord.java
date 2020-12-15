package org.embulk.output.jdbc;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.embulk.spi.Column;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.msgpack.value.Value;


// TODO: weida libs
import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.util.function.Consumer;
import org.embulk.spi.ColumnVisitor;
import org.msgpack.value.ValueFactory;

/**
 * Record read by PageReader.
 * The class will save read records for retry.
 */
public class PageReaderRecord implements Record
{
    public static final char ITEM_DELIMITER = ',';
    public static final int BATCH_ROW_SIZE = 1024;
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

    public int getRecordCount(Page p)
    {
      return pageReader.getRecordCount(p);
    }

    protected File createTempFile() throws IOException
    {
        return File.createTempFile("embulk-output-jdbc-records-", ".csv");
    }

    protected BufferedWriter openWriter(File newFile) throws IOException
    {
        return new BufferedWriter(new FileWriter(newFile));
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
                    writer.write(lastRecord.getJson(c).toJson());
                    break;
                case "timestamp":
                    writer.write(lastRecord.getTimestamp(c).toString());
                    break;
                case "string":
                    writer.write(lastRecord.getString(c));
                    break;
            }
            if (i + 1 < columnCount) {
              writer.write(ITEM_DELIMITER);
            }
        }
        writer.newLine();
        writer.flush();
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
              System.out.printf("record:");
              for (int i = 0; i < columnCount; i++) {
                  Column c = pageReader.getSchema().getColumn(i);
                  switch (c.getType().getName()) {
                      case "boolean":
                          System.out.printf(" %s,", Boolean.valueOf(values[i])); // TODO: weida revert here
                          record.setValue(c, Boolean.valueOf(values[i]));
                          break;
                      case "long":
                          System.out.printf(" %s,", Long.valueOf(values[i]));
                          record.setValue(c, Long.valueOf(values[i]));
                          break;
                      case "double":
                          System.out.printf(" %s,", Double.valueOf(values[i]));
                          record.setValue(c, Double.valueOf(values[i]));
                          break;
                      case "json":
                          System.out.printf(" %s,", ValueFactory.newString(values[i]));
                          record.setValue(c, ValueFactory.newString(values[i]));
                          break;
                      case "timestamp":
                          System.out.printf(" %s,", Instant.parse(values[i]));
                          record.setValue(c, Instant.parse(values[i]));
                          break;
                      case "string":
                          System.out.printf(" %s,", values[i]);
                          record.setValue(c, values[i]);
                          break;
                  }
              }
              System.out.println("");
              comsumer.accept(record);
          }
      } finally {
            reader.close();
      }
    }

    public void clearReadRecords() throws IOException // TODO: weida using file
    {
        close();
        readRecordsFile = createTempFile();
        writer = openWriter(readRecordsFile);
        // readRecords.clear(); // TODO: weida delete here
        lastRecord = null;
    }

    private <T> T save(Column column, T value)
    {
        if (lastRecord == null) {
            lastRecord = new MemoryRecord(pageReader.getSchema().getColumnCount());
            // TODO: weida here is the SPOT that incurred OOM
            // retrieve records should be more efficient
            // readRecords.add(lastRecord); // TODO: weida revert here
        }
        lastRecord.setValue(column, value);
        return value;
    }
}
