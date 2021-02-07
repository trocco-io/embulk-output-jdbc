package org.embulk.output.jdbc;

import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.time.Instant;

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
import com.google.common.base.Function;

/**
 * Record read by PageReader.
 * The class will save read records for retry.
 */
public class PageReaderRecord implements Record {
    private static final CSVFormat DEFAULT_FORMAT = CSVFormat.DEFAULT.withNullString("null");
    private final PageReader pageReader;
    protected File readRecordsFile;
    protected CSVPrinter writer;
    private MemoryRecord lastRecord;

    public PageReaderRecord(PageReader pageReader) throws IOException {
        this.pageReader = pageReader;
        readRecordsFile = createTempFile();
        writer = openWriter(readRecordsFile);
    }

    public void setPage(Page page) {
        pageReader.setPage(page);
    }

    public boolean nextRecord() throws IOException {
        writeRow(lastRecord);
        lastRecord = null; // lastRecord will be created in next `save` method execution.
        return pageReader.nextRecord();
    }

    public boolean isNull(Column column) {
        return pageReader.isNull(column);
    }

    public boolean getBoolean(Column column) {
        return save(column, pageReader.getBoolean(column));
    }

    public long getLong(Column column) {
        return save(column, pageReader.getLong(column));
    }

    public double getDouble(Column column) {
        return save(column, pageReader.getDouble(column));
    }

    public String getString(Column column) {
        return save(column, pageReader.getString(column));
    }

    public Instant getTimestamp(Column column) {
        return save(column, pageReader.getTimestamp(column).getInstant());
    }

    public Value getJson(Column column) {
        return save(column, pageReader.getJson(column));
    }

    private <T> T save(Column column, T value) {
        if (lastRecord == null) {
            lastRecord = new MemoryRecord(pageReader.getSchema().getColumnCount());
        }
        lastRecord.setValue(column, value);
        return value;
    }


    public void clearReadRecords() throws IOException {
        close();
        readRecordsFile = createTempFile();
        writer = openWriter(readRecordsFile);
        lastRecord = null;
    }

    private void setReadRecords(File newRecordsFile) throws IOException {
        close();
        System.out.println("setReadRecords");
        readRecordsFile = newRecordsFile;
        writer = openWriter(readRecordsFile);
    }

    protected File createTempFile() throws IOException {
        File f = File.createTempFile("embulk-output-jdbc-records-", ".csv");
//        f.deleteOnExit();
        return f;
    }

    protected File createTempFile(String s) throws IOException // TODO: weida delete this method
    {
        File f = File.createTempFile(s + "embulk-output-jdbc-records-", ".csv");
//        f.deleteOnExit(); // TODO: weida revert here
        return f;
    }

    protected CSVParser openReader(File newFile) throws IOException {
        return CSVParser.parse(new FileReader(newFile), DEFAULT_FORMAT);
    }

    protected CSVPrinter openWriter(File newFile) throws IOException {
        return new CSVPrinter(new FileWriter(newFile), DEFAULT_FORMAT);
    }

    private void write(CSVPrinter writer, final Object value) {
        try {
            writer.print(value); // CSVPrint.print will check the situation of null, nullString settings, etc.
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected void close() throws IOException {
        if (writer != null) {
            writer.close();
            writer = null;
        }
//        readRecordsFile.delete(); // TODO: weida revert here
    }

    protected void writeRow(MemoryRecord record) throws IOException {
        writeRow(writer, record);
    }

    protected void writeRow(CSVPrinter writer, MemoryRecord record) throws IOException {
        if (record == null) {
            System.out.println("record is null");
            return;
        }
        pageReader.getSchema().visitColumns(new ColumnVisitor() {
            @Override
            public void booleanColumn(Column column) {
                System.out.print(record.getBoolean(column));
                write(writer, record.getBoolean(column));
            }

            @Override
            public void longColumn(Column column) {
                System.out.print(record.getLong(column));
                write(writer, record.getLong(column));
            }

            @Override
            public void doubleColumn(Column column) {
                System.out.print(record.getDouble(column));
                write(writer, record.getDouble(column));
            }

            @Override
            public void stringColumn(Column column) {
                System.out.print(record.getString(column));
                write(writer, record.getString(column));
            }

            @Override
            public void timestampColumn(Column column) {
                System.out.print(record.getTimestamp(column));
                write(writer, record.getTimestamp(column));
            }

            @Override
            public void jsonColumn(Column column) {
                System.out.print(record.getJson(column));
                write(writer, record.getJson(column));
            }
        });
        System.out.print("\n");
        writer.println();
        writer.flush();
    }

    public void foreachRecord(Function<? super Record, Boolean> function) throws IOException {
        File tmpFile = createTempFile("retry");
        try (CSVParser reader = openReader(readRecordsFile); CSVPrinter tmpWriter = openWriter(tmpFile)) {
            MemoryRecord record = new MemoryRecord(pageReader.getSchema().getColumnCount());
            for (CSVRecord r : reader) {
                pageReader.getSchema().visitColumns(new ColumnVisitor() {
                    @Override
                    public void booleanColumn(Column column) {
                        setValue(record, column, r.get(column.getIndex()), Boolean.class);
//                        record.setValue(column, Boolean.valueOf(r.get(column.getIndex())));
                    }

                    @Override
                    public void longColumn(Column column) {
                        setValue(record, column, r.get(column.getIndex()), Long.class);
//                        record.setValue(column, Long.valueOf(r.get(column.getIndex())));
                    }

                    @Override
                    public void doubleColumn(Column column) {
                        setValue(record, column, r.get(column.getIndex()), Double.class);
//                        record.setValue(column, Double.valueOf(r.get(column.getIndex())));
                    }

                    @Override
                    public void stringColumn(Column column) {
                        setValue(record, column, r.get(column.getIndex()), String.class);
//                        record.setValue(column, r.get(column.getIndex()));
                    }

                    @Override
                    public void timestampColumn(Column column) {
                        setValue(record, column, r.get(column.getIndex()), Instant.class);
//                        record.setValue(column, Instant.parse(r.get(column.getIndex())));
                    }

                    @Override
                    public void jsonColumn(Column column) {
                        setValue(record, column, r.get(column.getIndex()), Value.class);
//                        record.setValue(column, ValueFactory.newString(r.get(column.getIndex())));
                    }
                });
//                tmpWriter.printRecord(r);
                showRecord(record);
                writeRow(tmpWriter, record);
//                if (!function.apply(record)) {
//                    System.out.println("write");
//                    System.out.println(tmpWriter);
//                    System.out.println(record);
//                    writeRow(tmpWriter, record);
////                    writeRow(record);
//                } else {
//                    System.out.println("skip writting");
//                }
            }
//            tmpWriter.close();
            setReadRecords(tmpFile);
        }
    }

    private void setValue(MemoryRecord record, Column column, String str, Class<?> obj) {
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

    private void showRecord(MemoryRecord record) {
        System.out.println("showRecord");
        if (record == null) {
            System.out.println("record is null");
            return;
        }
        pageReader.getSchema().visitColumns(new ColumnVisitor() {
            @Override
            public void booleanColumn(Column column) {
                System.out.print(record.getBoolean(column));
            }

            @Override
            public void longColumn(Column column) {
                System.out.print(record.getLong(column));
            }

            @Override
            public void doubleColumn(Column column) {
                System.out.print(record.getDouble(column));
            }

            @Override
            public void stringColumn(Column column) {
                System.out.print(record.getString(column));
            }

            @Override
            public void timestampColumn(Column column) {
                System.out.print(record.getTimestamp(column));
            }

            @Override
            public void jsonColumn(Column column) {
                System.out.print(record.getJson(column));
            }
        });
        System.out.println("");
    }
}
