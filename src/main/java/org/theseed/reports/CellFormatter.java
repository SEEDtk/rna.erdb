/**
 *
 */
package org.theseed.reports;

import org.theseed.java.erdb.DbRecord;

import j2html.tags.ContainerTag;
import j2html.tags.DomContent;
import static j2html.TagCreator.*;

/**
 * This method formats a cell for an HTML table used in an RNA report.  The formatter knows the appropriate style
 * and how to convert the database field to the cell contents.
 *
 * @author Bruce Parrello
 *
 */
public abstract class CellFormatter {

    // FIELDS
    /** column name for table header */
    private String heading;
    /** database field name */
    private String fieldName;

    /**
     *  Construct a cell formatter.
     *
     *  @param header	heading name for this column
     *  @param field	database field name
     */
    public CellFormatter(String header, String field) {
        this.heading = header;
        this.fieldName = field;
    }

    /**
     * @return the table cell HTML for this column and this database record
     *
     * @param record	database record for this row
     */
    protected abstract DomContent getContent(DbRecord record);

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
        return td().with(this.getContent(record)).withStyle(this.getStyle());
    }

    /**
     * @return the database field name used to populate this column
     */
    public String getFieldName() {
        return this.fieldName;
    }

}
