package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.io.Page;

import java.util.*;

public class SortOperator {
    private Database.Transaction transaction;
    private String tableName;
    private Comparator<Record> comparator;
    private Schema operatorSchema;
    private int numBuffers;
    private String sortedTableName = null;

    public SortOperator(Database.Transaction transaction, String tableName,
                        Comparator<Record> comparator) throws DatabaseException, QueryPlanException {
        this.transaction = transaction;
        this.tableName = tableName;
        this.comparator = comparator;
        this.operatorSchema = this.computeSchema();
        this.numBuffers = this.transaction.getNumMemoryPages();
    }

    public Schema computeSchema() throws QueryPlanException {
        try {
            return this.transaction.getFullyQualifiedSchema(this.tableName);
        } catch (DatabaseException de) {
            throw new QueryPlanException(de);
        }
    }

    public class Run {
        String tempTableName;

        public Run() throws DatabaseException {
            this.tempTableName = SortOperator.this.transaction.createTempTable(
                                     SortOperator.this.operatorSchema);
        }

        public void addRecord(List<DataBox> values) throws DatabaseException {
            SortOperator.this.transaction.addRecord(this.tempTableName, values);
        }

        public void addRecords(List<Record> records) throws DatabaseException {
            for (Record r : records) {
                this.addRecord(r.getValues());
            }
        }

        public Iterator<Record> iterator() throws DatabaseException {
            return SortOperator.this.transaction.getRecordIterator(this.tempTableName);
        }

        public String tableName() {
            return this.tempTableName;
        }
    }

    /**
     * Returns a NEW run that is the sorted version of the input run.
     * Can do an in memory sort over all the records in this run
     * using one of Java's built-in sorting methods.
     * Note: Don't worry about modifying the original run.
     * Returning a new run would bring one extra page in memory beyond the
     * size of the buffer, but it is done this way for ease.
     */
    public Run sortRun(Run run) throws DatabaseException {
        //throw new UnsupportedOperationException("TODO(hw3): implement");
        List<Record> elements = new ArrayList<>();
        Iterator<Record> recordIter = run.iterator();
        while (recordIter.hasNext()){
            elements.add(recordIter.next());
        }
        Run toRetrun = new Run();
        elements.sort(SortOperator.this.comparator);
        toRetrun.addRecords(elements);
        return toRetrun;
    }

    /**
     * Given a list of sorted runs, returns a new run that is the result
     * of merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be should be added to the output run next.
     * It is recommended that your Priority Queue hold Pair<Record, Integer> objects
     * where a Pair (r, i) is the Record r with the smallest value you are
     * sorting on currently unmerged from run i.
     */
    public Run mergeSortedRuns(List<Run> runs) throws DatabaseException {
        //throw new UnsupportedOperationException("TODO(hw3): implement");
        PriorityQueue<Pair<Record, Integer>> nextElem = new PriorityQueue<>(new RecordPairComparator());
        List<Iterator<Record>> runIters = new ArrayList<>();
        Run toReturn = new Run();
        int i = 0;
        while (!runs.isEmpty()) {
            runIters.add(i, runs.remove(0).iterator());
            if (runIters.get(i).hasNext()) {
                nextElem.add(new Pair<>(runIters.get(i).next(), i));
            }
            i++;
        }
        while (!nextElem.isEmpty()) {
            Pair<Record, Integer> next = nextElem.poll();
            int j = next.getSecond();
            toReturn.addRecord(next.getFirst().getValues());
            if (runIters.get(j).hasNext()) {
                nextElem.add(new Pair<>(runIters.get(j).next(), j));
            }
        }

        return toReturn;
    }

    /**
     * Given a list of N sorted runs, returns a list of
     * sorted runs that is the result of merging (numBuffers - 1)
     * of the input runs at a time.
     */
    public List<Run> mergePass(List<Run> runs) throws DatabaseException {
        //throw new UnsupportedOperationException("TODO(hw3): implement");
        List<Run> toReturn = new LinkedList<>();
        while (runs.size() > 0) {
            List<Run> toMerge = new ArrayList<>();
            while (toMerge.size() < this.numBuffers - 1 & runs.size() > 0) {
                toMerge.add(runs.remove(0));
            }
            toReturn.add(mergeSortedRuns(toMerge));
        }

        return toReturn;
    }

    /**
     * Does an external merge sort on the table with name tableName
     * using numBuffers.
     * Returns the name of the table that backs the final run.
     */
    public String sort() throws DatabaseException {
        //throw new UnsupportedOperationException("TODO(hw3): implement");

        Iterator<Record> allRecords = transaction.getRecordIterator(tableName);
        int pageSize = transaction.getNumEntriesPerPage(tableName);
        List<Run> sortedRuns = new LinkedList<>();
        while (allRecords.hasNext()) {
            Run run = new Run();
            int i = 0;
            while (i < numBuffers * pageSize & allRecords.hasNext()) {
                run.addRecord(allRecords.next().getValues());
                i++;
            }
            sortedRuns.add(sortRun(run));
        }

        sortedRuns = mergePass(sortedRuns);
        while (sortedRuns.size() > 1) {
            sortedRuns = mergePass(sortedRuns);
        }
        return sortedRuns.get(0).tempTableName;
    }


    public Iterator<Record> iterator() throws DatabaseException {
        if (sortedTableName == null) {
            sortedTableName = sort();
        }
        return this.transaction.getRecordIterator(sortedTableName);
    }

    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }

    public Run createRun() throws DatabaseException {
        return new Run();
    }
}

