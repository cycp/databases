package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

public class BNLJOperator extends JoinOperator {

  protected int numBuffers;

  public BNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
            rightSource,
            leftColumnName,
            rightColumnName,
            transaction,
            JoinType.BNLJ);

    this.numBuffers = transaction.getNumMemoryPages();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new BNLJIterator();
  }


  /**
   * BNLJ: Block Nested Loop Join
   *  See lecture slides.
   *
   * An implementation of Iterator that provides an iterator interface for this operator.
   *
   * Before proceeding, you should read and understand SNLJOperator.java
   *    You can find it in the same directory as this file.
   *
   * Word of advice: try to decompose the problem into distinguishable sub-problems.
   *    This means you'll probably want to add more methods than those given (Once again,
   *    SNLJOperator.java might prove to be a useful reference).
   */
  private class BNLJIterator extends JoinIterator {
    /**
     * Some member variables are provided for guidance, but there are many possible solutions.
     * You should implement the solution that's best for you, using any member variables you need.
     * You're free to use these member variables, but you're not obligated to.
     */

    private Iterator<Page> leftPageIterator = null; // left/outer relation page iter
    private Page currLeftPage = null;
    private Iterator<Page> rightPageIterator = null; // right relation page iter
    private Page currRightPage = null;

    private BacktrackingIterator<Record> leftRecordIterator = null;
    private BacktrackingIterator<Record> rightRecordIterator = null;

    private Record leftRecord = null;
    private Record rightRecord = null;
    private Record nextRecord = null;

    private boolean newPage = true;

    private int blockSize = numBuffers - 2;

    //private Iterator<Page> leftIterator = null;
    //private Iterator<Page> rightIterator = null;
    //private BacktrackingIterator<Record> leftRecordIterator = null;
    //private BacktrackingIterator<Record> rightRecordIterator = null;
    //private Record leftRecord = null;
    //private Record nextRecord = null;

    public BNLJIterator() throws QueryPlanException, DatabaseException {
      super();
      this.leftPageIterator = getPageIterator(this.getLeftTableName());
      leftPageIterator.next();
//      this.currLeftPage = leftPageIterator.next();

//      rightPageIterator = null;
      leftRecordIterator = null;
      rightRecordIterator = null;
      leftRecord = null;
      nextRecord = null;
//      throw new UnsupportedOperationException("hw3: TODO");
    }

    public void nextRightPage() throws DatabaseException {
      this.currRightPage = this.rightPageIterator.next(); // what if no next?
      this.rightRecordIterator = getBlockIterator(this.getRightTableName(), new Page[]{this.currRightPage});
      this.newPage = true;
    }

    public void resetLeftRecord() {
      this.leftRecordIterator.reset();
      this.leftRecord = this.leftRecordIterator.next(); // should always be true??
    }

    public void markLeftRecord() {
      this.leftRecord = this.leftRecordIterator.next();
      leftRecordIterator.mark();
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      if (nextRecord != null) {
        return true;
      }
      while (true) {
        // if no next record, we must calculate the next record...
        if (this.leftRecord == null) {
          // if we're not done with R (there are more pages)
          if (this.rightPageIterator != null &&
                  this.rightPageIterator.hasNext()) {
            try {
              nextRightPage();
            } catch (DatabaseException e) { // Database Exception or No Other Element
              return false;
            }
            // reset to beginning of L page (with next R page)
            resetLeftRecord();

            // else, we've reached the end of R with current L page, so we should start over from first R page with next L page
          } else if (//this.leftPageIterator != null &&
                  this.leftPageIterator.hasNext()) {
            // if L has another page, get that page and mark the first record so we can outer loop over it for every rec in R
            try {
//              this.currLeftPage = this.leftPageIterator.next();
              this.leftRecordIterator = getBlockIterator(this.getLeftTableName(), this.leftPageIterator, this.blockSize);
              // if the new page has records, update current left record and mark it
              if (this.leftRecordIterator.hasNext()) {
                markLeftRecord();
              } else return false; // else, outer loop is done so ret false

              rightPageIterator = getPageIterator(getRightTableName());
              rightPageIterator.next();
              nextRightPage();
              // catch exception from getBlockIterator
            } catch (DatabaseException e) {
              return false;
            }
          } else return false; // necessary?
        }
        while (this.rightRecordIterator != null && this.rightRecordIterator.hasNext()) {
          this.rightRecord = this.rightRecordIterator.next();
          // if this is a new page, then we want to mark the first record so we can loop over it for every record in L
          if (this.newPage) {
            this.rightRecordIterator.mark();
            this.newPage = false;
          }
//          DataBox leftVal = this.leftRecord.getValues().get(getLeftColumnIndex());
          if (this.leftRecord.getValues().get(getLeftColumnIndex()).equals(this.rightRecord.getValues().get(getRightColumnIndex()))) {
            // add the matching L/R records to res
            List<DataBox> res = new ArrayList<>(this.leftRecord.getValues());
            res.addAll(this.rightRecord.getValues());
            this.nextRecord = new Record(res);
            return true;
          }
        }
        while (//this.leftRecordIterator != null &&
                this.leftRecordIterator.hasNext()) {
          // iterate over every R record for every L page
          // for every L record, we iterate through all R records in the page so we should reset R rec iterator
          this.leftRecord = this.leftRecordIterator.next();
          this.rightRecordIterator.reset();
          return this.hasNext();
        }
        // if we've looped over all of L page and still don't have a match, then we must get new R page and reset L
        // set leftRec to null to indicate this
        this.leftRecord = null;

      }

//      throw new UnsupportedOperationException("hw3: TODO");
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      if (this.hasNext()) {
        Record next = this.nextRecord;
        this.nextRecord = null;
        return next;
      }
      throw new NoSuchElementException("No more records to yield");
//      throw new UnsupportedOperationException("hw3: TODO");
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
