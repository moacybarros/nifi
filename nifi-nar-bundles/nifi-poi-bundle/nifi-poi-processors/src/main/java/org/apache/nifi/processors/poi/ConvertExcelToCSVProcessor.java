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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.util.CellAddress;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.util.SAXHelper;
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFComment;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import javax.xml.parsers.ParserConfigurationException;


@Tags({"excel", "csv", "poi"})
@CapabilityDescription("Consumes a Microsoft Excel document and converts each worksheet to csv. Each sheet from the incoming Excel " +
        "document will generate a new Flowfile that will be output from this processor. Each output Flowfile's contents will be formatted as a csv file " +
        "where the each row from the excel sheet is output as a newline in the csv file. This processor is currently only capable of processing .xlsx " +
        "(XSSF 2007 OOXML file format) Excel documents and not older .xls (HSSF '97(-2007) file format) documents. This processor also expects well formatted " +
        "CSV content and will not escape cell's containing invalid content such as newlines or additional commas.")
@WritesAttributes({@WritesAttribute(attribute="sheetname", description="The name of the Excel sheet that this particular row of data came from in the Excel document"),
        @WritesAttribute(attribute="numrows", description="The number of rows in this Excel Sheet"),
        @WritesAttribute(attribute="sourcefilename", description="The name of the Excel document file that this data originated from"),
        @WritesAttribute(attribute="convertexceltocsvprocessor.error", description="Error message that was encountered on a per Excel sheet basis. This attribute is" +
                " only populated if an error was occured while processing the particular sheet. Having the error present at the sheet level will allow for the end" +
                " user to better understand what syntax errors in their excel doc on a larger scale caused the error.")})
public class ConvertExcelToCSVProcessor
        extends AbstractProcessor {

    private static final String CSV_MIME_TYPE = "text/csv";
    public static final String SHEET_NAME = "sheetname";
    public static final String ROW_NUM = "numrows";
    public static final String SOURCE_FILE_NAME = "sourcefilename";
    private static final String DESIRED_SHEETS_DELIMITER = ",";
    private static final String UNKNOWN_SHEET_NAME = "UNKNOWN";
    private static final String SAX_PARSER = "org.apache.xerces.parsers.SAXParser";

    public static final PropertyDescriptor DESIRED_SHEETS = new PropertyDescriptor
            .Builder().name("extract-sheets")
            .displayName("Sheets to Extract")
            .description("Comma separated list of Excel document sheet names that should be extracted from the excel document. If this property" +
                    " is left blank then all of the sheets will be extracted from the Excel document. The list of names is case in-sensitive. Any sheets not " +
                    "specified in this value will be ignored.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor COLUMN_DELIMITER = new PropertyDescriptor.Builder()
            .name("excel-csv-column-delimiter")
            .displayName("Column Delimiter")
            .description("Character(s) used to separate columns of data in the CSV file. Special characters should use the '\\u' notation.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(",")
            .build();

    static final PropertyDescriptor RECORD_DELIMITER = new PropertyDescriptor.Builder()
            .name("excel-csv-record-delimiter")
            .displayName("Record Delimiter")
            .description("Character(s) used to separate rows of data in the CSV file. For line return enter \\n in this field. Special characters should use the '\\u' notation.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("\\n")
            .build();

    public static final Relationship ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Original Excel document received by this processor")
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Excel data converted to csv")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to parse the Excel document")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(DESIRED_SHEETS);
        descriptors.add(COLUMN_DELIMITER);
        descriptors.add(RECORD_DELIMITER);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(ORIGINAL);
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        final String desiredSheetsDelimited = context.getProperty(DESIRED_SHEETS).evaluateAttributeExpressions().getValue();

        //Delimiter's are frequently special characters. Allow users to enter special characters using \ u notation and then translate.
        final String columnDelimiter = org.apache.commons.lang3.StringEscapeUtils.unescapeJava(context.getProperty(COLUMN_DELIMITER).getValue());
        final String recordDelimiter = org.apache.commons.lang3.StringEscapeUtils.unescapeJava(context.getProperty(RECORD_DELIMITER).getValue());

        try {
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream inputStream) throws IOException {

                    try {
                        OPCPackage pkg = OPCPackage.open(inputStream);
                        XSSFReader r = new XSSFReader(pkg);
                        ReadOnlySharedStringsTable sst = new ReadOnlySharedStringsTable(pkg);
                        StylesTable styles = r.getStylesTable();
                        XSSFReader.SheetIterator iter = (XSSFReader.SheetIterator) r.getSheetsData();

                        if (desiredSheetsDelimited != null) {
                            String[] desiredSheets = StringUtils
                                    .split(desiredSheetsDelimited, DESIRED_SHEETS_DELIMITER);

                            if (desiredSheets != null) {
                                while (iter.hasNext()) {
                                    InputStream sheet = iter.next();
                                    String sheetName = iter.getSheetName();

                                    for (int i = 0; i < desiredSheets.length; i++) {
                                        //If the sheetName is a desired one parse it
                                        if (sheetName.equalsIgnoreCase(desiredSheets[i])) {
                                            handleExcelSheet(session, flowFile, sst, styles, sheet, sheetName,
                                                    columnDelimiter, recordDelimiter);
                                            break;
                                        }
                                    }
                                }
                            } else {
                                getLogger().debug("Excel document was parsed but no sheets with the specified desired names were found.");
                            }

                        } else {
                            //Get all of the sheets in the document.
                            while (iter.hasNext()) {
                                handleExcelSheet(session, flowFile, sst, styles, iter.next(), iter.getSheetName(),
                                        columnDelimiter, recordDelimiter);
                            }
                        }
                    } catch (InvalidFormatException ife) {
                        getLogger().error("Only .xlsx Excel 2007 OOXML files are supported", ife);
                        throw new UnsupportedOperationException("Only .xlsx Excel 2007 OOXML files are supported", ife);
                    } catch (OpenXML4JException | SAXException e) {
                        getLogger().error("Error occurred while processing Excel document metadata", e);
                    }
                }
            });

            session.transfer(flowFile, ORIGINAL);

        } catch (RuntimeException ex) {
            getLogger().error("Failed to process incoming Excel document", ex);
            FlowFile failedFlowFile = session.putAttribute(flowFile,
                    ConvertExcelToCSVProcessor.class.getName() + ".error", ex.getMessage());
            session.transfer(failedFlowFile, FAILURE);
        }
    }


    /**
     * Handles an individual Excel sheet from the entire Excel document. Each sheet will result in an individual flowfile.
     *
     * @param session
     *  The NiFi ProcessSession instance for the current invocation.
     */
    private void handleExcelSheet(ProcessSession session, FlowFile originalParentFF,
                                  ReadOnlySharedStringsTable sst, StylesTable styles, final InputStream sheetInputStream, String sName,
                                  String columnDelimiter, String recordDelimiter) throws IOException {

        FlowFile ff = session.create();
        try {
            final DataFormatter formatter = new DataFormatter();
            final InputSource sheetSource = new InputSource(sheetInputStream);

            final SheetToCSV sheetHandler = new SheetToCSV();
            sheetHandler.setColumnDelimiter(columnDelimiter);
            sheetHandler.setRecordDelimiter(recordDelimiter);

            final XMLReader parser = SAXHelper.newXMLReader();
            final XSSFSheetXMLHandler handler = new XSSFSheetXMLHandler(
                    styles, null, sst, sheetHandler, formatter, false);

            parser.setContentHandler(handler);

            ff = session.write(ff, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    PrintStream outPrint = new PrintStream(out);
                    sheetHandler.setOutput(outPrint);

                    try {
                        parser.parse(sheetSource);

                        sheetInputStream.close();
                        outPrint.close();
                    } catch (SAXException se) {
                        getLogger().error("Error occurred while processing Excel sheet {}", new Object[]{sName}, se);
                    }
                }
            });

            ff = session.putAttribute(ff, SHEET_NAME, sName);
            ff = session.putAttribute(ff, ROW_NUM, new Long(sheetHandler.getRowCount()).toString());

            if (StringUtils.isNotEmpty(originalParentFF.getAttribute(CoreAttributes.FILENAME.key()))) {
                ff = session.putAttribute(ff, SOURCE_FILE_NAME, originalParentFF.getAttribute(CoreAttributes.FILENAME.key()));
            } else {
                ff = session.putAttribute(ff, SOURCE_FILE_NAME, UNKNOWN_SHEET_NAME);
            }

            //Update the CoreAttributes.FILENAME to have the .csv extension now. Also update MIME.TYPE
            ff = session.putAttribute(ff, CoreAttributes.FILENAME.key(), updateFilenameToCSVExtension(ff.getAttribute(CoreAttributes.UUID.key()),
                    ff.getAttribute(CoreAttributes.FILENAME.key()), sName));
            ff = session.putAttribute(ff, CoreAttributes.MIME_TYPE.key(), CSV_MIME_TYPE);

            session.transfer(ff, SUCCESS);

        } catch (SAXException | ParserConfigurationException saxE) {
            getLogger().error("Failed to create instance of SAXParser {}", new Object[]{SAX_PARSER}, saxE);
            ff = session.putAttribute(ff,
                    ConvertExcelToCSVProcessor.class.getName() + ".error", saxE.getMessage());
            session.transfer(ff, FAILURE);
        } finally {
            sheetInputStream.close();
        }
    }

    /**
     * Uses the XSSF Event SAX helpers to do most of the work
     *  of parsing the Sheet XML, and outputs the contents
     *  as a (basic) CSV.
     */
    private class SheetToCSV implements XSSFSheetXMLHandler.SheetContentsHandler {
        private String delimiter = ",";
        private String recordDelimiter = "\n";
        private boolean firstCellOfRow;
        private int currentRow = -1;
        private int currentCol = -1;
        private int rowCount = 0;
        private boolean rowHasValues=false;
        private PrintStream output;

        private boolean firstRow=false;
        private int firstCol;
        private int lastCol;

        private StringBuilder sbRow;

        public String getColumnDelimiter(){
            return delimiter;
        }

        public String getRecordDelimiter(){
            return recordDelimiter;
        }

        public void setColumnDelimiter(String delimiter){
            this.delimiter = delimiter;
        }

        public void setRecordDelimiter(String recordDelimiter){
            this.recordDelimiter = recordDelimiter;
        }

        public int getRowCount(){
            return rowCount;
        }

        public void setOutput(PrintStream output){
            this.output = output;
        }

        @Override
        public void startRow(int rowNum) {
            // Prepare for this row
            firstCellOfRow = true;
            firstRow = currentRow==-1;
            currentRow = rowNum;
            currentCol = -1;
            rowHasValues = false;

            sbRow = new StringBuilder();
        }

        @Override
        public void endRow(int rowNum) {
            if(firstRow){
                lastCol = currentCol;
            }

            //if there was no data in this row, don't write it
            if(!rowHasValues) {
                return;
            }

            // Ensure the correct number of columns
            for (int i=currentCol; i<lastCol; i++) {
                sbRow.append(delimiter);
            }
            sbRow.append(recordDelimiter);

            output.append(sbRow.toString());
            rowCount++;
        }

        @Override
        public void cell(String cellReference, String formattedValue,
                         XSSFComment comment) {

            // gracefully handle missing CellRef here in a similar way as XSSFCell does
            if(cellReference == null) {
                cellReference = new CellAddress(currentRow, currentCol).formatAsString();
            }

            // Did we miss any cells?
            int thisCol = (new CellReference(cellReference)).getCol();

            //Use the first row of the file to decide on the area of data to export
            if(firstRow && firstCellOfRow){
                firstCol = thisCol;
            }

            //if this cell falls outside our area, return and don't write it out.
            if(!firstRow && (thisCol < firstCol || thisCol > lastCol)){
                return;
            }

            int missedCols = (thisCol - firstCol) - (currentCol - firstCol) - 1;
            if(firstCellOfRow){
                missedCols = (thisCol - firstCol);
            }

            if (firstCellOfRow) {
                firstCellOfRow = false;
            } else {
                sbRow.append(delimiter);
            }

            for (int i=0; i<missedCols; i++) {
                sbRow.append(delimiter);
            }
            currentCol = thisCol;

            sbRow.append(formattedValue);

            rowHasValues = true;
        }

        @Override
        public void headerFooter(String s, boolean b, String s1) {

        }
    }

        /**
     * Takes the original input filename and updates it by removing the file extension and replacing it with
     * the .csv extension.
     *
     * @param origFileName
     *  Original filename from the input file.
     *
     * @return
     *  The new filename with the .csv extension that should be place in the output flowfile's attributes
     */
    private String updateFilenameToCSVExtension(String nifiUUID, String origFileName, String sheetName) {

        StringBuilder stringBuilder = new StringBuilder();

        if (StringUtils.isNotEmpty(origFileName)) {
            String ext = FilenameUtils.getExtension(origFileName);
            if (StringUtils.isNotEmpty(ext)) {
                stringBuilder.append(StringUtils.replace(origFileName, ("." + ext), ""));
            } else {
                stringBuilder.append(origFileName);
            }
        } else {
            stringBuilder.append(nifiUUID);
        }

        stringBuilder.append("_");
        stringBuilder.append(sheetName);
        stringBuilder.append(".");
        stringBuilder.append("csv");

        return stringBuilder.toString();
    }

}