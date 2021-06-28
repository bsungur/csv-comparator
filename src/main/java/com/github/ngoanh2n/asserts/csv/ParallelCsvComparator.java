package com.github.ngoanh2n.asserts.csv;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.ConcurrentRowProcessor;
import com.univocity.parsers.common.processor.RowProcessor;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <h3>csv-comparator<h3>
 * <a href="https://github.com/ngoanh2n/csv-comparator">https://github.com/ngoanh2n/csv-comparator<a>
 * <br>
 *
 * @author Ho Huu Ngoan (ngoanh2n@gmail.com)
 * @since 1.0.0
 */
public class ParallelCsvComparator {

    private final CsvComparisonSource<File> source;
    private final CsvComparisonOptions options;
    private final CsvComparisonVisitor visitor;

    public ParallelCsvComparator(@Nonnull CsvComparisonSource<File> source,
                                 @Nonnull CsvComparisonOptions options) {
        this(source, options, new DefaultCsvComparisonVisitor());
    }

    public ParallelCsvComparator(@Nonnull CsvComparisonSource<File> source,
                                 @Nonnull CsvComparisonOptions options,
                                 @Nonnull CsvComparisonVisitor visitor) {
        this.source = checkNotNull(source, "source cannot be null");
        this.options = checkNotNull(options, "source cannot be null");
        this.visitor = checkNotNull(visitor, "source cannot be null");
    }

    @Nonnull
    public CsvComparisonResult compare() throws ExecutionException, InterruptedException {
        visitor.visitStarted(source);
        Collector collector = new Collector();
        CsvParserSettings settings = getSettings();

        String[] headers = getHeaders(settings);
        ConcurrentMap<String, String[]> expMap = new ConcurrentHashMap<>();
        ConcurrentMap<String, String[]> actMap = new ConcurrentHashMap<>();

        settings.setProcessor(new ConcurrentRowProcessor(new ParallelRowProcessor(expMap, actMap, Arrays.asList(collector, visitor), headers, options)));
        CsvParser expectParser = new CsvParser(settings);

        settings.setProcessor(new ConcurrentRowProcessor(new ParallelRowProcessor(actMap, expMap, Arrays.asList(collector, visitor), headers, options)));
        CsvParser actualParser = new CsvParser(settings);

        final CompletableFuture<Void> parseExpect = CompletableFuture.runAsync(() -> expectParser.parse(source.exp(), getEncoding(source.exp())));
        final CompletableFuture<Void> parseActual = CompletableFuture.runAsync(() -> actualParser.parse(source.act(), getEncoding(source.act())));

        final CompletableFuture<Void> allCompleted = CompletableFuture.allOf(parseExpect, parseActual);
        allCompleted.thenRun(() -> {
            if (expMap.size() > 0) {
                for (Map.Entry<String, String[]> left : expMap.entrySet()) {
                    String[] actRow = actMap.get(left.getKey());

                    if (actRow == null) {
                        visitor.rowDeleted(left.getValue(), headers, options);
                        collector.rowDeleted(left.getValue(), headers, options);
                    } else {
                        if (Arrays.equals(left.getValue(), actRow)) {
                            visitor.rowKept(left.getValue(), headers, options);
                            collector.rowKept(left.getValue(), headers, options);
                        } else {
                            visitor.rowModified(left.getValue(), headers, options);
                            collector.rowModified(left.getValue(), headers, options);
                        }
                        actMap.remove(left.getKey());
                    }
                }
            }
            if (actMap.size() > 0) {
                for (Map.Entry<String, String[]> right : actMap.entrySet()) {
                    String[] expRow = expMap.get(right.getKey());

                    if (expRow == null) {
                        visitor.rowInserted(right.getValue(), headers, options);
                        collector.rowInserted(right.getValue(), headers, options);
                    } else {
                        if (Arrays.equals(right.getValue(), expRow)) {
                            visitor.rowKept(right.getValue(), headers, options);
                            collector.rowKept(right.getValue(), headers, options);
                        } else {
                            visitor.rowModified(right.getValue(), headers, options);
                            collector.rowModified(right.getValue(), headers, options);
                        }
                        expMap.remove(right.getKey());
                    }
                }
            }
        });
        allCompleted.get();

        visitor.visitEnded(source);
        return new Result(collector);
    }

    private CsvParserSettings getSettings() {
        Utils.createsDirectory(options.resultOptions().location());
        return options.parserSettings();
    }

    private Charset getEncoding(File file) {
        try {
            return options.encoding() != null
                    ? options.encoding()
                    : Charset.forName(Utils.charsetOf(file));
        } catch (IOException ignored) {
            // Can't happen
            return StandardCharsets.UTF_8;
        }
    }

    private String[] getHeaders(CsvParserSettings settings) {
        if (settings.isHeaderExtractionEnabled()) {
            String[] headers = new String[0];
            settings.setHeaderExtractionEnabled(false);
            List<String[]> expRows = Utils.read(source.exp(), getEncoding(source.exp()), settings);
            if (expRows.size() > 1) headers = expRows.get(0);
            settings.setHeaderExtractionEnabled(true);
            return headers;
        }
        return new String[0];
    }

    private final static class Result implements CsvComparisonResult {

        private final Collector collector;

        private Result(Collector collector) {
            this.collector = collector;
        }

        @Override
        public boolean hasDeleted() {
            return collector.hasDeleted;
        }

        @Override
        public boolean hasInserted() {
            return collector.hasInserted;
        }

        @Override
        public boolean hasModified() {
            return collector.hasModified;
        }

        @Override
        public List<String[]> rowsKept() {
            return collector.rowsKept;
        }

        @Override
        public List<String[]> rowsDeleted() {
            return collector.rowsDeleted;
        }

        @Override
        public List<String[]> rowsInserted() {
            return collector.rowsInserted;
        }

        @Override
        public List<String[]> rowsModified() {
            return collector.rowsModified;
        }

        @Override
        public boolean hasDiff() {
            return hasDeleted() || hasInserted() || hasModified();
        }
    }

    private final static class Collector implements CsvComparisonVisitor {

        private boolean hasDeleted = false;
        private boolean hasInserted = false;
        private boolean hasModified = false;

        private final List<String[]> rowsKept = new ArrayList<>();
        private final List<String[]> rowsDeleted = new ArrayList<>();
        private final List<String[]> rowsInserted = new ArrayList<>();
        private final List<String[]> rowsModified = new ArrayList<>();

        @Override
        public void rowKept(String[] row, String[] headers, CsvComparisonOptions options) {
            rowsKept.add(row);
        }

        @Override
        public void rowDeleted(String[] row, String[] headers, CsvComparisonOptions options) {
            hasDeleted = true;
            rowsDeleted.add(row);
        }

        @Override
        public void rowInserted(String[] row, String[] headers, CsvComparisonOptions options) {
            hasInserted = true;
            rowsInserted.add(row);
        }

        @Override
        public void rowModified(String[] row, String[] headers, CsvComparisonOptions options) {
            hasModified = true;
            rowsModified.add(row);
        }

        @Override
        public void visitStarted(CsvComparisonSource<?> source) {

        }

        @Override
        public void visitEnded(CsvComparisonSource<?> source) {

        }
    }
}
