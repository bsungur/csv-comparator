package com.github.ngoanh2n.asserts.csv;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.File;
import java.net.URL;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * <h3>csv-comparator<h3>
 * <a href="https://github.com/ngoanh2n/csv-comparator">https://github.com/ngoanh2n/csv-comparator<a>
 * <br>
 *
 * @author Ho Huu Ngoan (ngoanh2n@gmail.com)
 * @since 1.0.0
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ParallelCsvComparatorTest {

    private static final int COLUMNS = 50;

    @Test
    @Order(1)
    void kept() throws ExecutionException, InterruptedException {
        CsvComparisonSource<File> source = CsvComparisonSource.create(
                resource("com/github/ngoanh2n/asserts/csv/exp/people.csv"),
                resource("com/github/ngoanh2n/asserts/csv/exp/people.csv")
        );
        CsvComparisonOptions options = CsvComparisonOptions
                .builder()
                .setColumns(IntStream.range(0, COLUMNS).boxed().toArray(Integer[]::new))
                .setIdentityColumn(0)
                .build();
        CsvComparisonResult result = new ParallelCsvComparator(source, options).compare();

        assertFalse(result.hasDeleted());
        assertFalse(result.hasInserted());
        assertFalse(result.hasModified());
        assertFalse(result.hasDiff());
        assertEquals(100000, result.rowsKept().size());
    }

    @Test
    @Order(2)
    void deleted() throws ExecutionException, InterruptedException {
        CsvComparisonSource<File> source = CsvComparisonSource.create(
                resource("com/github/ngoanh2n/asserts/csv/exp/people.csv"),
                resource("com/github/ngoanh2n/asserts/csv/act/people_deleted.csv")
        );
        CsvComparisonOptions options = CsvComparisonOptions
                .builder()
                .setColumns(IntStream.range(0, COLUMNS).boxed().toArray(Integer[]::new))
                .setIdentityColumn(0)
                .build();
        CsvComparisonResult result = new ParallelCsvComparator(source, options).compare();

        assertTrue(result.hasDeleted());
        assertTrue(result.hasDiff());
        assertEquals(99995, result.rowsKept().size());
        assertEquals(5, result.rowsDeleted().size());
    }

    @Test
    @Order(3)
    void inserted() throws ExecutionException, InterruptedException {
        CsvComparisonSource<File> source = CsvComparisonSource.create(
                resource("com/github/ngoanh2n/asserts/csv/exp/people.csv"),
                resource("com/github/ngoanh2n/asserts/csv/act/people_inserted.csv")
        );
        CsvComparisonOptions options = CsvComparisonOptions
                .builder()
                .setColumns(IntStream.range(0, COLUMNS).boxed().toArray(Integer[]::new))
                .setIdentityColumn(0)
                .build();
        CsvComparisonResult result = new ParallelCsvComparator(source, options).compare();

        assertTrue(result.hasInserted());
        assertTrue(result.hasDiff());
        assertEquals(100000, result.rowsKept().size());
        assertEquals(5, result.rowsInserted().size());
    }

    @Test
    @Order(4)
    void modified() throws ExecutionException, InterruptedException {
        CsvComparisonSource<File> source = CsvComparisonSource.create(
                resource("com/github/ngoanh2n/asserts/csv/exp/people.csv"),
                resource("com/github/ngoanh2n/asserts/csv/act/people_modified.csv")
        );
        CsvComparisonOptions options = CsvComparisonOptions
                .builder()
                .setColumns(IntStream.range(0, COLUMNS).boxed().toArray(Integer[]::new))
                .setIdentityColumn(0)
                .build();
        CsvComparisonResult result = new ParallelCsvComparator(source, options).compare();

        assertTrue(result.hasModified());
        assertTrue(result.hasDiff());
        assertEquals(99995, result.rowsKept().size());
        assertEquals(5, result.rowsModified().size());
    }

    @Test
    void generateUUID() {
        for (int i = 0; i < 5; i++) {
            System.out.println(UUID.randomUUID().toString());
        }
    }

    static File resource(String name) {
        ClassLoader classLoader = Utils.class.getClassLoader();
        URL resource = classLoader.getResource(name);
        if (resource == null) throw new IllegalArgumentException("File not found!");
        else return new File(resource.getFile());
    }
}
