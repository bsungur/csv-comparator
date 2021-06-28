package com.github.ngoanh2n.asserts.csv;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.RowProcessor;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

public class ParallelRowProcessor implements RowProcessor {

    private final ConcurrentMap<String, String[]> thisMap;
    private final ConcurrentMap<String, String[]> otherMap;
    private final List<CsvComparisonVisitor> visitors;
    private final String[] headers;
    private final CsvComparisonOptions options;

    public ParallelRowProcessor(ConcurrentMap<String, String[]> thisMap, ConcurrentMap<String, String[]> otherMap, List<CsvComparisonVisitor> visitors, String[] headers, CsvComparisonOptions options) {
        this.thisMap = thisMap;
        this.otherMap = otherMap;
        this.visitors = visitors;
        this.headers = headers;
        this.options = options;
    }

    @Override
    public void processStarted(ParsingContext context) {
    }

    @Override
    public void rowProcessed(String[] thisRow, ParsingContext context) {
        String thisKey = thisRow[0];
        String[] otherRow = otherMap.get(thisKey);

        if (otherRow == null) {
            thisMap.putIfAbsent(thisKey, thisRow);
        } else {
            if (Arrays.equals(thisRow, otherRow)) {
                visitors.forEach(v -> v.rowKept(thisRow, headers, options));
            } else {
                visitors.forEach(v -> v.rowModified(thisRow, headers, options));
            }
            otherMap.remove(thisKey);
        }
    }

    @Override
    public void processEnded(ParsingContext context) {
    }
}
