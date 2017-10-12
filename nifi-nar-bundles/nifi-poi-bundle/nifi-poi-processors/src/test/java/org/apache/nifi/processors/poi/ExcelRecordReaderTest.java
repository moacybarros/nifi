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

import org.apache.nifi.serialization.record.Record;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ExcelRecordReaderTest {

    private static final Logger logger = LoggerFactory.getLogger(ExcelRecordReaderTest.class);

    @Test
    public void readAllRecords() throws Exception {
        InputStream inputStream = new FileInputStream("src/test/resources/dataformatting.xlsx");
        int numRows = 0;
        try (ExcelRecordReader reader = new ExcelRecordReader(inputStream)) {
            Record record = reader.nextRecord();
            while (record != null) {
                numRows++;
                record = reader.nextRecord();
            }
            // This should not block.
            record = reader.nextRecord();
            assertNull(record);

            assertEquals(10, numRows);
        }
    }

    @Test
    public void alreadyClosed() throws Exception {
        InputStream inputStream = new FileInputStream("src/test/resources/dataformatting.xlsx");
        int numRows = 0;
        try (ExcelRecordReader reader = new ExcelRecordReader(inputStream)) {
            Record record = reader.nextRecord();
            assertNotNull(record);
            numRows++;

            record = reader.nextRecord();
            assertNotNull(record);
            numRows++;

            reader.close();

            // This should not block.
            record = reader.nextRecord();
            assertNull(record);

            assertEquals(2, numRows);
        }
    }

}
