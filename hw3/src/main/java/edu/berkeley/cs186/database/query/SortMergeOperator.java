package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordIterator;
import edu.berkeley.cs186.database.table.Schema;

public class SortMergeOperator extends JoinOperator {

  public SortMergeOperator(QueryOperator leftSource,
           QueryOperator rightSource,
           String leftColumnName,
           String rightColumnName,
           Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new SortMergeOperator.SortMergeIterator();
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   *
   * Before proceeding, you should read and understand SNLJOperator.java
   *    You can find it in the same directory as this file.
   *
   * Word of advice: try to decompose the problem into distinguishable sub-problems.
   *    This means you'll probably want to add more methods than those given (Once again,
   *    SNLJOperator.java might be a useful reference).
   *
   * Also, see discussion slides for week 7.
   */
  private class SortMergeIterator extends JoinIterator {
    /**
    * Some member variables are provided for guidance, but there are many possible solutions.
    * You should implement the solution that's best for you, using any member variables you need.
    * You're free to use these member variables, but you're not obligated to.
    */

    private RecordIterator leftIterator;
    private RecordIterator rightIterator;
    private Record leftRecord;
    private Record nextRecord;
    private Record rightRecord;
    private boolean marked;
    private boolean newRound;
    private LR_RecordComparator comparator;

    public SortMergeIterator() throws QueryPlanException, DatabaseException {
      super();
      this.leftIterator = getRecordIterator(new SortOperator(getTransaction(), getLeftTableName(), new LeftRecordComparator()).sort());
      this.rightIterator = getRecordIterator(new SortOperator(getTransaction(), getRightTableName(), new RightRecordComparator()).sort());;

      this.marked = false;
      this.newRound = true;
      this.comparator = new LR_RecordComparator();
//      throw new UnsupportedOperationException("hw3: TODO");
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      if (this.nextRecord != null) {
        return true;
      }
      // otherwise, we need to find next record


      // if we're done with L and we're not currently iterating on a L record,
      // we've gone through the whole relation without finding anything --> return false
      if (!this.leftIterator.hasNext() && this.leftRecord == null) {
        return false;
      }

      if (!this.marked && !this.newRound) {
        this.rightIterator.mark();
        this.marked = true;
      }

      // initialize L record
      if (this.leftRecord == null) {
        this.leftRecord = this.leftIterator.next();
      }

      // if we're done with R, then we have to reset R and get new L record
      if (!this.rightIterator.hasNext()) {
        if (this.leftIterator.hasNext()) {
          this.leftRecord = this.leftIterator.next();
          this.rightIterator.reset();
          // recurse
          return this.hasNext();
        } else {
          return false;
        }

      // otherwise we want to just keep iterating through R until we're done
      } else {
        this.rightRecord = this.rightIterator.next();
      }
      if (!this.marked && this.newRound) {
        this.marked = true;
        this.newRound = false;
        this.rightIterator.mark();
      }
      // compare the L and R until equality
      while (this.comparator.compare(this.leftRecord, this.rightRecord) != 0) {
        // L < R --> advance L
//        while (this.comparator.compare(this.leftRecord, this.rightRecord) < 0) {
//          if (!this.leftIterator.hasNext()) {
//            return false;
//          }
//          this.leftRecord = this.leftIterator.next();
//          this.marked = false;
//          this.rightIterator.reset();
//        }
//        // L > R --> advance R
//        while (this.comparator.compare(this.leftRecord, this.rightRecord) > 0) {
//          if (!this.rightIterator.hasNext()) {
//            return false;
//          }
//          this.rightRecord = this.rightIterator.next();
//        }
//      }
        if (this.comparator.compare(this.leftRecord, this.rightRecord) < 0) {
          if (!this.leftIterator.hasNext()) {
            return false;
          }
          this.leftRecord = this.leftIterator.next();
          this.rightIterator.reset();
          this.rightRecord = this.rightIterator.next();
        } else {
          if (!this.rightIterator.hasNext()) {
            return false;
          }

          this.rightRecord = this.rightIterator.next();
          this.marked = false;
        }
      }

      if (!this.marked) {
        this.rightIterator.mark();
        this.marked = true;
      }

      List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
      leftValues.addAll(this.rightRecord.getValues());
      this.nextRecord = new Record(leftValues);
      return true;
    }
//      throw new UnsupportedOperationException("hw3: TODO");


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


    private class LeftRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
      }
    }

    private class RightRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }

    /**
    * Left-Right Record comparator
    * o1 : leftRecord
    * o2: rightRecord
    */
    private class LR_RecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }
  }
}
