/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.poi;

import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.util.SAXHelper;
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFComment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;


public class ExcelRecordReader implements RecordReader {

    private static final Logger logger = LoggerFactory.getLogger(ExcelRecordReader.class);
    private volatile CountDownLatch readingLatch = new CountDownLatch(1);
    private volatile CountDownLatch consumingLatch = new CountDownLatch(1);
    private AtomicBoolean closed = new AtomicBoolean(false);

    private volatile Record record = null;

    private class AlreadyClosedException extends RuntimeException {
    }

    private class Reader implements Runnable {

        private final InputStream inputStream;

        private Reader(InputStream inputStream) {
            this.inputStream = inputStream;
        }

        @Override
        public void run() {

            try {
                OPCPackage pkg = OPCPackage.open(inputStream);
                XSSFReader r = new XSSFReader(pkg);
                ReadOnlySharedStringsTable sst = new ReadOnlySharedStringsTable(pkg);
                StylesTable styles = r.getStylesTable();
                XSSFReader.SheetIterator iter = (XSSFReader.SheetIterator) r.getSheetsData();

                //Get the first sheet in the document.
                try (InputStream sheetInputStream = iter.next()) {

                    final DataFormatter formatter = new DataFormatter();
                    final InputSource sheetSource = new InputSource(sheetInputStream);

                    final XSSFSheetXMLHandler.SheetContentsHandler sheetHandler = new XSSFSheetXMLHandler.SheetContentsHandler() {
                        @Override
                        public void startRow(int rowNum) {
                            logger.debug("startRow for {}", rowNum);
                            if (closed.get()) {
                                throw new AlreadyClosedException();
                            }
                            // Wait if previous record is not consumed yet.
                            try {
                                consumingLatch.await();
                            } catch (InterruptedException e) {
                                logger.warn("Reading Excel sheet is interrupted at startRow() due to {}", e);
                            }
                        }

                        @Override
                        public void endRow(int rowNum) {
                            logger.debug("endRow for {}", rowNum);
                            if (closed.get()) {
                                throw new AlreadyClosedException();
                            }

                            Map<String, Object> values = new HashMap<>();
                            values.put("row", rowNum);
                            record = new MapRecord(schema, values);
                            // The row is ready
                            readingLatch.countDown();
                        }

                        @Override
                        public void cell(String cellReference, String formattedValue, XSSFComment comment) {

                        }

                        @Override
                        public void headerFooter(String text, boolean isHeader, String tagName) {

                        }
                    };

                    final XMLReader parser = SAXHelper.newXMLReader();
                    final XSSFSheetXMLHandler handler = new XSSFSheetXMLHandler(
                            styles, null, sst, sheetHandler, formatter, false);

                    parser.setContentHandler(handler);
                    parser.parse(sheetSource);
                }

            } catch (AlreadyClosedException e) {
                logger.info("The file has been closed.");
            } catch (Exception e) {
                throw new RuntimeException("Failed to read Excel sheet, due to " + e, e);
            } finally {
                closed.set(true);
                readingLatch.countDown();
            }
        }
    }

    private final RecordSchema schema;

    public ExcelRecordReader(InputStream inputStream) throws OpenXML4JException, SAXException, IOException {

        List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("row", RecordFieldType.INT.getDataType()));
        schema = new SimpleRecordSchema(fields);

        ExecutorService parseThreadExecutor = Executors.newSingleThreadExecutor();
        Runnable reader = new Reader(inputStream);
        parseThreadExecutor.execute(reader);

        parseThreadExecutor.shutdown();
    }

    @Override
    public Record nextRecord(boolean coerceTypes, boolean dropUnknownFields) throws IOException, MalformedRecordException {
        if (closed.get()) {
            return null;
        }
        try {
            // Wait for the next record.
            consumingLatch.countDown();
            readingLatch.await();
            // Start consuming the record.
        } catch (InterruptedException e) {
            logger.warn("Reading Excel sheet is interrupted at nextRecord() due to {}", e);
        } finally {
            // Reset Latches.
            consumingLatch = new CountDownLatch(1);
            readingLatch = new CountDownLatch(1);
        }
        final Record ret = record;
        record = null;
        return ret;
    }

    @Override
    public RecordSchema getSchema() throws MalformedRecordException {
        return null;
    }

    @Override
    public void close() throws IOException {
        closed.set(true);
        // Release latches so that it won't block the internal reading thread.
        readingLatch.countDown();
        consumingLatch.countDown();
    }
}