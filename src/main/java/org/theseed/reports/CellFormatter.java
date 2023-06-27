/**
 *
 */
package org.theseed.reports;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.java.erdb.DbQuery;
import org.theseed.java.erdb.DbRecord;
import org.theseed.java.erdb.DbValue;

import j2html.tags.ContainerTag;
import j2html.tags.DomContent;
import static j2html.TagCreator.*;

import java.sql.SQLException;
import java.util.Collection;

/**
 * This method formats a cell for an HTML table used in an RNA report.  The formatter knows the appropriate style
 * and how to convert the database field to the cell contents.
 *
 * @author Bruce Parrello
 *
 */
public abstract class CellFormatter {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(CellFormatter.class);
    /** column name for table header */
    private String heading;
    /** database field name */
    private String fieldName;
    /** constant to use for error cells */
    private static final ContainerTag ERROR_CELL = td();
    /** constant to use for empty cell content */
    private static final DomContent EMPTY_CELL = rawHtml("&nbsp;");

    /**
     * Update a query to select all of the fields in a list of formatters
     *
     * @param query		query to update
     * @param list		collection of cell formatters
     *
     * @throws SQLException
     */
    public static void getSelectInfo(DbQuery query, Collection<CellFormatter> list) throws SQLException {
        for (var formatter : list) {
            String[] parts = StringUtils.split(formatter.fieldName, '.');
            if (parts.length != 2)
                throw new IllegalArgumentException("Invalid field specification \"" + formatter.fieldName + " in cell formatter.");
            query.select(parts[0], parts[1]);
        }
    }

    /**
     *  Construct a cell formatter.
     *
     *  @param header	heading name for this column
     *  @param field	database field name (fully-qualified)
     */
    public CellFormatter(String header, String field) {
        this.heading = header;
        this.fieldName = field;
    }

    /**
     * @return the table cell HTML for this column and this database record
     *
     * @param record	database record for this row
     * @param field		name of field to extract from the record
     *
     * @throws SQLException
     */
    protected abstract DomContent getContent(DbRecord record, String field) throws SQLException;

    /**
     * @return the style for this column
     */
    protected abstract String getStyle();

    /**
     * @return the header cell for this column
     */
    public ContainerTag getHeader() {
        return th().with(text(this.heading)).withStyle(this.getStyle());
    }

    /**
     * @return a data cell for this column
     *
     * @param record	database record for this row
     */
    public ContainerTag getCell(DbRecord record) {
        ContainerTag retVal = ERROR_CELL;
        try {
            retVal = td().with(this.getContent(record, this.fieldName)).withStyle(this.getStyle());
        } catch (SQLException e) {
            log.error("SQL error formatting table field {}:  {}", this.fieldName, e.toString());
        }
        return retVal;
    }

    /**
     * @return the database field name used to populate this column
     */
    public String getFieldName() {
        return this.fieldName;
    }

    /**
     * This is a basic text cell.  The style is "text" (left-justified), with no fancy formatting.
     */
    public static class Text extends CellFormatter {

        public Text(String header, String field) {
            super(header, field);
        }

        @Override
        protected DomContent getContent(DbRecord record, String fieldName) throws SQLException {
            // We use "getReportString" so that null comes back as an empty string.
            String content = record.getReportString(fieldName);
            DomContent retVal;
            if (content.isBlank())
                retVal = EMPTY_CELL;
            else
                retVal = text(content);
            return retVal;
        }

        @Override
        protected String getStyle() {
            return "text";
        }

    }

    /**
     * This is an integer cell.  The style is "num" (right-justified).
     */
    public static class Count extends CellFormatter {

        public Count(String header, String field) {
            super(header, field);
        }

        @Override
        protected DomContent getContent(DbRecord record, String field) throws SQLException {
            String content = Integer.toString(record.getInt(field));
            return text(content);
        }

        @Override
        protected String getStyle() {
            return "num";
        }

    }

    /**
     * This is a percent cell.  The style is "num" (right-justified), and it is formatted to two decimal places.
     */
    public static class Percent extends CellFormatter {

        public Percent(String header, String field) {
            super(header, field);
        }

        @Override
        protected DomContent getContent(DbRecord record, String field) throws SQLException {
            String content = String.format("%1.2f", record.getDouble(field));
            return text(content);
        }

        @Override
        protected String getStyle() {
            return "num";
        }

    }

    /**
     * This is a pubmed link cell.  The style is "text" (left-justified), but it links to a PUBMED article.
     */
    public static class PubMed extends CellFormatter {

        /** URL format for pubmed IDs */
        protected static String PUBMED_URL = "https://pubmed.ncbi.nlm.nih.gov/%s/";

        public PubMed(String header, String field) {
            super(header, field);
        }

        @Override
        protected DomContent getContent(DbRecord record, String field) throws SQLException {
            DbValue value = record.getValue(field);
            DomContent retVal;
            if (value.isNull())
                retVal = EMPTY_CELL;
            else {
                String pubmedId = value.getString();
                retVal = a(pubmedId).withHref(String.format(PUBMED_URL, pubmedId)).withTarget("_blank");
            }
            return retVal;
        }

        @Override
        protected String getStyle() {
            return "text";
        }

    }

}
