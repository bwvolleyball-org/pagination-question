package com.erwolff.pagination;

import com.erwolff.data.ArchivedDrive;
import com.erwolff.data.DriveType;
import com.erwolff.data.LiveDrive;
import com.erwolff.data.Translator;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import static org.fest.assertions.Assertions.assertThat;
import static org.fest.assertions.Fail.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class PagerTest {
    private static final Logger log = LoggerFactory.getLogger(PagerTest.class.getSimpleName());

    private Pager pager = new Pager();
    private Random random = new Random();
    private List<LiveDrive> liveDrives;
    private List<ArchivedDrive> archivedDrives;

    /**
     * Verifies that a pageSize of zero is rejected with an IllegalArgumentException
     */
    @Test(description = "Verifies that a pageSize of zero is rejected with an IllegalArgumentException",
            expectedExceptions = IllegalArgumentException.class)
    public void test_pageAndMerge_pageSizeZero() {
        // mock this because otherwise we can't construct this class with page size 0
        PageRequest pageRequest = mock(PageRequest.class);
        doReturn(new Sort(Pager.DEFAULT_SORT)).when(pageRequest).getSort();
        doReturn(0).when(pageRequest).getPageSize();
        liveDrives = generateLiveDrives(pageRequest, 1);
        archivedDrives = generateArchivedDrives(pageRequest, 1);

        pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                pageRequest);
    }

    @Test(description = "Verifies that a negative page number is rejected with an IllegalArgumentException",
            expectedExceptions = IllegalArgumentException.class)
    public void test_pageAndMerge_pageNumberNegative() {
        // mock this because otherwise we can't construct this class with page size 0
        PageRequest pageRequest = mock(PageRequest.class);
        doReturn(new Sort(Pager.DEFAULT_SORT)).when(pageRequest).getSort();
        doReturn(5).when(pageRequest).getPageSize();
        doReturn(-1).when(pageRequest).getPageNumber();
        liveDrives = generateLiveDrives(pageRequest, 1);
        archivedDrives = generateArchivedDrives(pageRequest, 1);

        pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                pageRequest);
    }

    @Test(description = "Verifies the correct elements are returned with a DESC sort with ONLY live drives")
    public void test_pageAndMerge_descSort_noArchivedDrives() {
        // pageSize of 5, only 3 live drives, and 0 archived drives
        PageRequest pageRequest = new PageRequest(0, 5, Sort.Direction.DESC, "timestamp");
        liveDrives = generateLiveDrives(pageRequest, 3);
        archivedDrives = new ArrayList<>();

        Page<LiveDrive> results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                pageRequest);

        assertThat(results).isNotNull();
        assertThat(results.getNumberOfElements()).isEqualTo(3);
        verifyOrder(results.getContent(), Sort.Direction.DESC);
        verifyAllLive(results.getContent());
    }

    @Test(description = "Verifies the correct elements are returned with an ASC sort with ONLY live drives")
    public void test_pageAndMerge_ascSort_noArchivedDrives() {
        // pageSize of 5, only 3 live drives, and 0 archived drives
        PageRequest pageRequest = new PageRequest(0, 5, Sort.Direction.ASC, "timestamp");
        liveDrives = generateLiveDrives(pageRequest, 3);
        archivedDrives = new ArrayList<>();

        Page<LiveDrive> results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                pageRequest);

        assertThat(results).isNotNull();
        assertThat(results.getNumberOfElements()).isEqualTo(3);
        verifyOrder(results.getContent(), Sort.Direction.ASC);
        verifyAllLive(results.getContent());
    }

    @Test(description = "Verifies the correct elements are returned with a DESC sort with ONLY archived drives")
    public void test_pageAndMerge_descSort_noLiveDrives() {
        // pageSize of 5, only 3 archived drives, and 0 live drives
        PageRequest pageRequest = new PageRequest(0, 5, Sort.Direction.DESC, "timestamp");
        liveDrives = new ArrayList<>();
        archivedDrives = generateArchivedDrives(pageRequest, 3);

        Page<LiveDrive> results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                pageRequest);

        assertThat(results).isNotNull();
        assertThat(results.getNumberOfElements()).isEqualTo(3);
        verifyOrder(results.getContent(), Sort.Direction.DESC);
        verifyAllArchived(results.getContent());
    }

    @Test(description = "Verifies the correct elements are returned with an ASC sort with ONLY archived drives")
    public void test_pageAndMerge_ascSort_noLiveDrives() {
        // pageSize of 5, only 3 archived drives, and 0 live drives
        PageRequest pageRequest = new PageRequest(0, 5, Sort.Direction.ASC, "timestamp");
        liveDrives = new ArrayList<>();
        archivedDrives = generateArchivedDrives(pageRequest, 3);

        Page<LiveDrive> results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                pageRequest);

        assertThat(results).isNotNull();
        assertThat(results.getNumberOfElements()).isEqualTo(3);
        verifyOrder(results.getContent(), Sort.Direction.ASC);
        verifyAllArchived(results.getContent());
    }

    @Test(description = "Verifies the correct elements are returned with a DESC sort with 8 live drives, 8 archived drives, and a pageSize of 6")
    public void test_pageAndMerge_descSort_liveAndArchivedDrives() {
        // pageSize of 6, 8 live drives, 8 archived drives
        PageRequest pageRequest = new PageRequest(0, 6, Sort.Direction.DESC, "timestamp");
        liveDrives = generateLiveDrives(pageRequest, 8);
        archivedDrives = generateArchivedDrives(pageRequest, 8);

        Page<LiveDrive> results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                pageRequest);

        assertThat(results).isNotNull();
        assertThat(results.getNumberOfElements()).isEqualTo(6);
        assertThat(results.hasNext()).isTrue();
        verifyOrder(results.getContent(), Sort.Direction.DESC);
        verifyAllLive(results.getContent());

        // get the next page of results
        results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                results.nextPageable());

        assertThat(results).isNotNull();
        assertThat(results.getNumberOfElements()).isEqualTo(6);
        assertThat(results.hasNext()).isTrue();

        List<LiveDrive> liveResults = results.getContent().subList(0, 2);
        List<LiveDrive> archivedResults = results.getContent().subList(2, results.getNumberOfElements());

        assertThat(liveResults).hasSize(2);
        assertThat(archivedResults).hasSize(4);

        verifyOrder(liveResults, Sort.Direction.DESC);
        verifyOrder(archivedResults, Sort.Direction.DESC);

        verifyAllLive(liveResults);
        verifyAllArchived(archivedResults);

        // get the final page of results
        results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                results.nextPageable());

        assertThat(results).isNotNull();
        assertThat(results.getNumberOfElements()).isEqualTo(4);
        assertThat(results.hasNext()).isFalse();

        verifyOrder(results.getContent(), Sort.Direction.DESC);
        verifyAllArchived(results.getContent());
    }

    @Test(description = "Verifies the correct elements are returned with an ASC sort with 8 live drives, 8 archived drives, and a pageSize of 6")
    public void test_pageAndMerge_ascSort_liveAndArchivedDrives() {
        // pageSize of 6, 8 live drives, 8 archived drives
        PageRequest pageRequest = new PageRequest(0, 6, Sort.Direction.ASC, "timestamp");
        liveDrives = generateLiveDrives(pageRequest, 8);
        archivedDrives = generateArchivedDrives(pageRequest, 8);

        Page<LiveDrive> results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                pageRequest);

        assertThat(results).isNotNull();
        assertThat(results.getNumberOfElements()).isEqualTo(6);
        assertThat(results.hasNext()).isTrue();
        verifyOrder(results.getContent(), Sort.Direction.ASC);
        verifyAllArchived(results.getContent());

        // get the next page of results
        results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                results.nextPageable());

        assertThat(results).isNotNull();
        assertThat(results.getNumberOfElements()).isEqualTo(6);
        assertThat(results.hasNext()).isTrue();

        List<LiveDrive> archivedResults = results.getContent().subList(0, 2);
        List<LiveDrive> liveResults = results.getContent().subList(2, results.getNumberOfElements());

        assertThat(archivedResults).hasSize(2);
        assertThat(liveResults).hasSize(4);

        verifyOrder(archivedResults, Sort.Direction.ASC);
        verifyOrder(liveResults, Sort.Direction.ASC);

        verifyAllArchived(archivedResults);
        verifyAllLive(liveResults);

        // get the final page of results
        results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                results.nextPageable());

        assertThat(results).isNotNull();
        assertThat(results.getNumberOfElements()).isEqualTo(4);
        assertThat(results.hasNext()).isFalse();

        verifyOrder(results.getContent(), Sort.Direction.DESC);
        verifyAllLive(results.getContent());
    }

    @Test(description = "Verifies the correct elements are returned with a DESC sort with a random amount of live and archived drives, and a random pageSize", invocationCount = 500)
    public void test_pageAndMerge_descSort_randomData() {
        int pageSize = Math.abs(random.nextInt(20) + 1);
        log.debug("pageSize: {}", pageSize);
        PageRequest pageRequest = new PageRequest(0, pageSize, Sort.Direction.DESC, "timestamp");
        int numLiveDrives = Math.abs(random.nextInt(100));
        int numArchivedDrives = Math.abs(random.nextInt(100));
        liveDrives = generateLiveDrives(pageRequest, numLiveDrives);
        archivedDrives = generateArchivedDrives(pageRequest, numArchivedDrives);
        int totalDrives = numLiveDrives + numArchivedDrives;

        Page<LiveDrive> results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                pageRequest);

        assertThat(results).isNotNull();
        if (totalDrives != 0) {
            assertThat(results.hasContent()).isTrue();
        }
        if (totalDrives >= pageSize) {
            assertThat(results.getNumberOfElements()).isEqualTo(pageSize);
        }
        if (totalDrives > pageSize) {
            assertThat(results.hasNext()).isTrue();
        }

        int splitElement = numLiveDrives >= pageSize ? pageSize : numLiveDrives;
        List<LiveDrive> liveResults = results.getContent().subList(0, splitElement);
        verifyAllLive(liveResults);
        verifyOrder(liveResults, Sort.Direction.DESC);

        List<LiveDrive> archivedResults = liveResults.size() >= pageSize ? new ArrayList<>() : results.getContent().subList(splitElement, results.getContent().size());
        verifyAllArchived(archivedResults);
        verifyOrder(archivedResults, Sort.Direction.DESC);

        while (results.hasNext()) {
            results = pager.pageAndMerge(liveQuery,
                    ld -> ld,
                    archivedQuery,
                    ad -> Translator.translate(ad).orElse(null),
                    results.nextPageable());

            int pageNumber = results.getNumber();
            if (numLiveDrives >= (pageSize * (pageNumber + 1))) {
                splitElement = pageSize;
            } else if (numLiveDrives > (pageSize * pageNumber)) {
                splitElement = numLiveDrives % (pageSize * pageNumber);
            } else {
                splitElement = 0;
            }
            liveResults = results.getContent().subList(0, splitElement);
            verifyAllLive(liveResults);
            verifyOrder(liveResults, Sort.Direction.DESC);

            archivedResults = liveResults.size() >= pageSize ? new ArrayList<>() : results.getContent().subList(splitElement, results.getContent().size());
            verifyAllArchived(archivedResults);
            verifyOrder(archivedResults, Sort.Direction.DESC);
        }
    }

    @Test(description = "Verifies the correct elements are returned with an ASC sort with a random amount of live and archived drives, and a random pageSize", invocationCount = 500)
    public void test_pageAndMerge_ascSort_randomData() {
        int pageSize = Math.abs(random.nextInt(20) + 1);
        log.debug("pageSize: {}", pageSize);
        PageRequest pageRequest = new PageRequest(0, pageSize, Sort.Direction.ASC, "timestamp");
        int numLiveDrives = Math.abs(random.nextInt(100));
        int numArchivedDrives = Math.abs(random.nextInt(100));
        liveDrives = generateLiveDrives(pageRequest, numLiveDrives);
        archivedDrives = generateArchivedDrives(pageRequest, numArchivedDrives);
        int totalDrives = numLiveDrives + numArchivedDrives;

        Page<LiveDrive> results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                pageRequest);

        assertThat(results).isNotNull();
        if (totalDrives != 0) {
            assertThat(results.hasContent()).isTrue();
        }
        if (totalDrives >= pageSize) {
            assertThat(results.getNumberOfElements()).isEqualTo(pageSize);
        }
        if (totalDrives > pageSize) {
            assertThat(results.hasNext()).isTrue();
        }

        int splitElement = numArchivedDrives >= pageSize ? pageSize : numArchivedDrives;
        List<LiveDrive> archivedResults = results.getContent().subList(0, splitElement);
        verifyAllArchived(archivedResults);
        verifyOrder(archivedResults, Sort.Direction.ASC);

        List<LiveDrive> liveResults = archivedResults.size() >= pageSize ? new ArrayList<>() : results.getContent().subList(splitElement, results.getContent().size());
        verifyAllLive(liveResults);
        verifyOrder(liveResults, Sort.Direction.ASC);

        while (results.hasNext()) {
            results = pager.pageAndMerge(liveQuery,
                    ld -> ld,
                    archivedQuery,
                    ad -> Translator.translate(ad).orElse(null),
                    results.nextPageable());

            int pageNumber = results.getNumber();
            if (numArchivedDrives >= (pageSize * (pageNumber + 1))) {
                splitElement = pageSize;
            } else if (numArchivedDrives > (pageSize * pageNumber)) {
                splitElement = numArchivedDrives % (pageSize * pageNumber);
            } else {
                splitElement = 0;
            }
            archivedResults = results.getContent().subList(0, splitElement);
            verifyAllArchived(archivedResults);
            verifyOrder(archivedResults, Sort.Direction.ASC);

            liveResults = archivedResults.size() >= pageSize ? new ArrayList<>() : results.getContent().subList(splitElement, results.getContent().size());
            verifyAllLive(liveResults);
            verifyOrder(liveResults, Sort.Direction.ASC);
        }
    }

    private Function<Pageable, Page<LiveDrive>> liveQuery = new Function<Pageable, Page<LiveDrive>>() {
        @Override
        public Page<LiveDrive> apply(Pageable pageable) {
            /** based on the findings in
             * {@link org.springframework.data.mongodb.core.query.Query#with(Pageable)} and
             * {@link org.springframework.data.mongodb.repository.support.QueryDslMongoRepository#applyPagination(com.mysema.query.mongodb.MongodbQuery<T>, org.springframework.data.domain.Pageable)} ,
             *  the offset of the {@link Pageable} should be used exclusively to determine the starting point.
             *  The default implementation is what was here, however a specialized Pageable with an offset adjust proves invaluable for performance tweaks*/
            int startingElement = pageable.getOffset();
            int endingElement = startingElement + (pageable.getPageSize() - 1);
            log.debug("LIVE: startingElement: {}  endingElement: {}  liveDrives.size(): {}", startingElement, endingElement, liveDrives.size());
            if (liveDrives.size() >= (endingElement + 1)) {
                return new PageImpl<>(liveDrives.subList(startingElement, (endingElement + 1)), pageable, liveDrives.size());
            }
            if (liveDrives.size() <= startingElement) {
                return new PageImpl<>(new ArrayList<>(), pageable, liveDrives.size());
            }
            return new PageImpl<>(liveDrives.subList(startingElement, liveDrives.size()), pageable, liveDrives.size());
        }
    };

    private Function<Pageable, Page<ArchivedDrive>> archivedQuery = new Function<Pageable, Page<ArchivedDrive>>() {
        @Override
        public Page<ArchivedDrive> apply(Pageable pageable) {
            /** based on the findings in
             * {@link org.springframework.data.mongodb.core.query.Query#with(Pageable)} and
             * {@link org.springframework.data.mongodb.repository.support.QueryDslMongoRepository#applyPagination(com.mysema.query.mongodb.MongodbQuery<T>, org.springframework.data.domain.Pageable)} ,
             *  the offset of the {@link Pageable} should be used exclusively to determine the starting point.
             *  The default implementation is what was here, however a specialized Pageable with an offset adjust proves invaluable for performance tweaks*/
            int startingElement = pageable.getOffset();
            int endingElement = startingElement + (pageable.getPageSize() - 1);
            log.debug("ARCHIVED: startingElement: {}  endingElement: {}  archivedDrives.size(): {}", startingElement, endingElement, archivedDrives.size());
            if (archivedDrives.size() >= (endingElement + 1)) {
                return new PageImpl<>(archivedDrives.subList(startingElement, (endingElement + 1)), pageable, archivedDrives.size());
            }
            if (archivedDrives.size() <= startingElement) {
                return new PageImpl<>(new ArrayList<>(), pageable, archivedDrives.size());
            }
            return new PageImpl<>(archivedDrives.subList(startingElement, archivedDrives.size()), pageable, archivedDrives.size());
        }
    };

    private List<LiveDrive> generateLiveDrives(Pageable p, int numDrives) {
        log.debug("Generating {} LIVE drives", numDrives);
        List<LiveDrive> drives = new ArrayList<>(numDrives);
        Sort.Order sort = Iterators.get(p.getSort().iterator(), 0, Pager.DEFAULT_SORT);
        if (Sort.Direction.ASC == sort.getDirection()) {
            for (int i = 0; i < numDrives; i++) {
                drives.add(new LiveDrive(i));
            }
        } else {
            for (int i = (numDrives - 1); i >= 0; i--) {
                drives.add(new LiveDrive(i));
            }
        }
        return drives;
    }

    private List<ArchivedDrive> generateArchivedDrives(Pageable p, int numDrives) {
        log.debug("Generating {} ARCHIVED drives", numDrives);
        List<ArchivedDrive> drives = new ArrayList<>(numDrives);
        Sort.Order sort = Iterators.get(p.getSort().iterator(), 0, Pager.DEFAULT_SORT);
        if (Sort.Direction.ASC == sort.getDirection()) {
            for (int i = 0; i < numDrives; i++) {
                drives.add(new ArchivedDrive(i));
            }
        } else {
            for (int i = (numDrives - 1); i >= 0; i--) {
                drives.add(new ArchivedDrive(i));
            }
        }
        return drives;
    }


    /**
     * Verifies that all supplied drives have type LIVE
     *
     * @param drives
     */
    private void verifyAllLive(List<LiveDrive> drives) {
        if (drives == null) {
            return;
        }
        for (LiveDrive drive : drives) {
            assertThat(drive.getType()).isEqualTo(DriveType.LIVE);
        }
    }

    /**
     * Verifies that all supplied drives have type ARCHIVED
     * Note: Expects LiveDrives to be supplied due to translation by function
     *
     * @param drives
     */
    private void verifyAllArchived(List<LiveDrive> drives) {
        if (drives == null) {
            fail("Expected drives not to be null");
        }
        for (LiveDrive drive : drives) {
            assertThat(drive.getType()).isEqualTo(DriveType.ARCHIVED);
        }
    }

    private void verifyOrder(List<LiveDrive> drives, Sort.Direction sortDirection) {
        if (drives == null) {
            fail("Expected drives not to be null");
        }
        long counter = Sort.Direction.ASC == sortDirection ? -1 : 1000;
        for (LiveDrive drive : drives) {
            if (Sort.Direction.ASC == sortDirection) {
                assertThat(drive.getTimestamp() > counter);
            } else {
                assertThat(drive.getTimestamp() < counter);
            }
            counter = drive.getTimestamp();
        }
    }
// ADDITIONAL TESTS

    @Test(description = "Verifies the correct elements are returned with an ASC sort with a random amount of live and archived drives, and a random pageSize, with extra assertions", dataProvider = "getDrivesOfBothTypesNum")
    public void test_pageAndMerge_ascSort_bothDrives_multiPage(final int pageSize, final int numLiveDrives, final int numArchivedDrives) {
        log.debug("pageSize: {}", pageSize);
        PageRequest pageRequest = new PageRequest(0, pageSize, Sort.Direction.ASC, "timestamp");
        liveDrives = generateLiveDrives(pageRequest, numLiveDrives);
        archivedDrives = generateArchivedDrives(pageRequest, numArchivedDrives);
        int totalDrives = numLiveDrives + numArchivedDrives;

        Page<LiveDrive> results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                pageRequest);

        assertThat(results).isNotNull();

        if (totalDrives != 0) {
            assertThat(results.hasContent()).isTrue();
        }
        int totalFoundDrives = results.getNumberOfElements();

        if (totalDrives >= pageSize) {
            assertThat(results.getNumberOfElements()).isEqualTo(pageSize);
        }
        if (totalDrives > pageSize) {
            assertThat(results.hasNext()).isTrue();
        }

        int splitElement = numArchivedDrives >= pageSize ? pageSize : numArchivedDrives;
        List<LiveDrive> archivedResults = results.getContent().subList(0, splitElement);
        verifyAllArchived(archivedResults);
        verifyOrder(archivedResults, Sort.Direction.ASC);

        List<LiveDrive> liveResults = archivedResults.size() >= pageSize ? new ArrayList<>() : results.getContent().subList(splitElement, results.getContent().size());
        verifyAllLive(liveResults);
        verifyOrder(liveResults, Sort.Direction.ASC);

        while (results.hasNext()) {
            results = pager.pageAndMerge(liveQuery,
                    ld -> ld,
                    archivedQuery,
                    ad -> Translator.translate(ad).orElse(null),
                    results.nextPageable());

            int pageNumber = results.getNumber();
            if (numArchivedDrives >= (pageSize * (pageNumber + 1))) {
                splitElement = pageSize;
            } else if (numArchivedDrives > (pageSize * pageNumber)) {
                splitElement = numArchivedDrives % (pageSize * pageNumber);
            } else {
                splitElement = 0;
            }
            archivedResults = results.getContent().subList(0, splitElement);
            verifyAllArchived(archivedResults);
            verifyOrder(archivedResults, Sort.Direction.ASC);

            liveResults = archivedResults.size() >= pageSize ? new ArrayList<>() : results.getContent().subList(splitElement, results.getContent().size());
            verifyAllLive(liveResults);
            verifyOrder(liveResults, Sort.Direction.ASC);

            totalFoundDrives += results.getNumberOfElements();
        }

        assertThat(totalFoundDrives).isEqualTo(numLiveDrives + numArchivedDrives);
    }

    @Test(description = "Verifies the correct elements are returned with an DESC sort with a random amount of live and archived drives, and a random pageSize, with extra assertions", dataProvider = "getDrivesOfBothTypesNum")
    public void test_pageAndMerge_descSort_bothDrives_multiPage(final int pageSize, final int numLiveDrives, final int numArchivedDrives) {
        log.debug("pageSize: {}", pageSize);
        PageRequest pageRequest = new PageRequest(0, pageSize, Sort.Direction.DESC, "timestamp");
        liveDrives = generateLiveDrives(pageRequest, numLiveDrives);
        archivedDrives = generateArchivedDrives(pageRequest, numArchivedDrives);
        int totalDrives = numLiveDrives + numArchivedDrives;

        Page<LiveDrive> results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                pageRequest);

        assertThat(results).isNotNull();

        if (totalDrives != 0) {
            assertThat(results.hasContent()).isTrue();
        }
        int totalFoundDrives = results.getNumberOfElements();

        if (totalDrives >= pageSize) {
            assertThat(results.getNumberOfElements()).isEqualTo(pageSize);
        }
        if (totalDrives > pageSize) {
            assertThat(results.hasNext()).isTrue();
        }

        int splitElement = numLiveDrives >= pageSize ? pageSize : numLiveDrives;
        List<LiveDrive> liveResults = results.getContent().subList(0, splitElement);
        verifyAllLive(liveResults);
        verifyOrder(liveResults, Sort.Direction.DESC);

        List<LiveDrive> archivedResults = liveResults.size() >= pageSize ? new ArrayList<>() : results.getContent().subList(splitElement, results.getContent().size());
        verifyAllArchived(archivedResults);
        verifyOrder(archivedResults, Sort.Direction.DESC);

        while (results.hasNext()) {
            results = pager.pageAndMerge(liveQuery,
                    ld -> ld,
                    archivedQuery,
                    ad -> Translator.translate(ad).orElse(null),
                    results.nextPageable());

            int pageNumber = results.getNumber();
            if (numLiveDrives >= (pageSize * (pageNumber + 1))) {
                splitElement = pageSize;
            } else if (numLiveDrives > (pageSize * pageNumber)) {
                splitElement = numLiveDrives % (pageSize * pageNumber);
            } else {
                splitElement = 0;
            }
            liveResults = results.getContent().subList(0, splitElement);
            verifyAllLive(liveResults);
            verifyOrder(liveResults, Sort.Direction.DESC);

            archivedResults = liveResults.size() >= pageSize ? new ArrayList<>() : results.getContent().subList(splitElement, results.getContent().size());
            verifyAllArchived(archivedResults);
            verifyOrder(archivedResults, Sort.Direction.DESC);

            totalFoundDrives += results.getNumberOfElements();
        }

        assertThat(totalFoundDrives).isEqualTo(numLiveDrives + numArchivedDrives);
    }

    @Test(description = "Verifies the correct elements are returned with a DESC sort with ONLY live drives, multiple pages worth", dataProvider = "getDrivesOfSingleTypeNum")
    public void test_pageAndMerge_descSort_noArchivedDrives_multiPage(final int pageSize, final int totalDrives) {
        PageRequest pageRequest = new PageRequest(0, pageSize, Sort.Direction.DESC, "timestamp");
        liveDrives = generateLiveDrives(pageRequest, totalDrives);
        archivedDrives = new ArrayList<>();


        Page<LiveDrive> results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                pageRequest);

        assertThat(results).isNotNull();
        assertThat(results.getNumberOfElements()).isEqualTo(pageSize);
        verifyOrder(results.getContent(), Sort.Direction.DESC);
        verifyAllLive(results.getContent());

        int returnedDrives = results.getContent().size();
        while (results.hasNext()) {

            results = pager.pageAndMerge(liveQuery,
                    ld -> ld,
                    archivedQuery,
                    ad -> Translator.translate(ad).orElse(null),
                    results.nextPageable());

            assertThat(results).isNotNull();

            int lastPageSize = totalDrives % pageSize == 0 ? pageSize : totalDrives % pageSize;
            int expectedSize = results.isLast() ? lastPageSize : pageSize;

            assertThat(results.getNumberOfElements()).isEqualTo(expectedSize);
            verifyOrder(results.getContent(), Sort.Direction.DESC);
            verifyAllLive(results.getContent());

            returnedDrives += results.getContent().size();
        }

        assertThat(returnedDrives).isEqualTo(totalDrives);
    }

    @Test(description = "Verifies the correct elements are returned with a DESC sort with ONLY live drives, multiple pages worth", dataProvider = "getDrivesOfSingleTypeNum")
    public void test_pageAndMerge_ascSort_noArchivedDrives_multiPage(final int pageSize, final int totalDrives) {
        PageRequest pageRequest = new PageRequest(0, pageSize, Sort.Direction.ASC, "timestamp");
        liveDrives = generateLiveDrives(pageRequest, totalDrives);
        archivedDrives = new ArrayList<>();


        Page<LiveDrive> results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                pageRequest);

        assertThat(results).isNotNull();
        assertThat(results.getNumberOfElements()).isEqualTo(pageSize);
        verifyOrder(results.getContent(), Sort.Direction.ASC);
        verifyAllLive(results.getContent());

        int returnedDrives = results.getContent().size();
        while (results.hasNext()) {

            results = pager.pageAndMerge(liveQuery,
                    ld -> ld,
                    archivedQuery,
                    ad -> Translator.translate(ad).orElse(null),
                    results.nextPageable());

            assertThat(results).isNotNull();
            int lastPageSize = totalDrives % pageSize == 0 ? pageSize : totalDrives % pageSize;
            int expectedSize = results.isLast() ? lastPageSize : pageSize;

            assertThat(results.getNumberOfElements()).isEqualTo(expectedSize);
            verifyOrder(results.getContent(), Sort.Direction.ASC);
            verifyAllLive(results.getContent());

            returnedDrives += results.getContent().size();
        }

        assertThat(returnedDrives).isEqualTo(totalDrives);
    }

    @Test(description = "Verifies the correct elements are returned with a DESC sort with ONLY archived drives, multiple pages worth", dataProvider = "getDrivesOfSingleTypeNum")
    public void test_pageAndMerge_descSort_noLiveDrives_multiPage(final int pageSize, final int totalDrives) {
        PageRequest pageRequest = new PageRequest(0, pageSize, Sort.Direction.DESC, "timestamp");
        archivedDrives = generateArchivedDrives(pageRequest, totalDrives);
        liveDrives = new ArrayList<>();


        Page<LiveDrive> results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                pageRequest);

        assertThat(results).isNotNull();
        assertThat(results.getNumberOfElements()).isEqualTo(pageSize);
        verifyOrder(results.getContent(), Sort.Direction.DESC);
        verifyAllArchived(results.getContent());

        int returnedDrives = results.getContent().size();
        while (results.hasNext()) {

            results = pager.pageAndMerge(liveQuery,
                    ld -> ld,
                    archivedQuery,
                    ad -> Translator.translate(ad).orElse(null),
                    results.nextPageable());

            assertThat(results).isNotNull();

            int lastPageSize = totalDrives % pageSize == 0 ? pageSize : totalDrives % pageSize;
            int expectedSize = results.isLast() ? lastPageSize : pageSize;

            assertThat(results.getNumberOfElements()).isEqualTo(expectedSize);
            verifyOrder(results.getContent(), Sort.Direction.DESC);
            verifyAllArchived(results.getContent());

            returnedDrives += results.getContent().size();
        }

        assertThat(returnedDrives).isEqualTo(totalDrives);
    }

    @Test(description = "Verifies the correct elements are returned with a DESC sort with ONLY live drives, multiple pages worth", dataProvider = "getDrivesOfSingleTypeNum")
    public void test_pageAndMerge_ascSort_noLiveDrives_multiPage(final int pageSize, final int totalDrives) {
        PageRequest pageRequest = new PageRequest(0, pageSize, Sort.Direction.ASC, "timestamp");
        archivedDrives = generateArchivedDrives(pageRequest, totalDrives);
        liveDrives = new ArrayList<>();


        Page<LiveDrive> results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                pageRequest);

        assertThat(results).isNotNull();
        assertThat(results.getNumberOfElements()).isEqualTo(pageSize);
        verifyOrder(results.getContent(), Sort.Direction.ASC);
        verifyAllArchived(results.getContent());

        int returnedDrives = results.getContent().size();
        while (results.hasNext()) {

            results = pager.pageAndMerge(liveQuery,
                    ld -> ld,
                    archivedQuery,
                    ad -> Translator.translate(ad).orElse(null),
                    results.nextPageable());

            assertThat(results).isNotNull();
            int lastPageSize = totalDrives % pageSize == 0 ? pageSize : totalDrives % pageSize;
            int expectedSize = results.isLast() ? lastPageSize : pageSize;

            assertThat(results.getNumberOfElements()).isEqualTo(expectedSize);
            verifyOrder(results.getContent(), Sort.Direction.ASC);
            verifyAllArchived(results.getContent());

            returnedDrives += results.getContent().size();
        }

        assertThat(returnedDrives).isEqualTo(totalDrives);
    }

    /**
     * A data provider to return 2 integers.
     * <p>
     * First integer is pageSize
     * <p>
     * Second integer is totalNumberOfDrives
     *
     * @return the data provider {pageSize, totalNumberOfDrives}
     */
    @DataProvider
    public Object[][] getDrivesOfSingleTypeNum() {

        return new Object[][]{{5, 17}, {5, 44}, {5, 70}, {7, 49}, {12, 143}};
    }

    /**
     * A data provider to return 3 integers
     * <p>
     * First integer is pageSize
     * <p>
     * Second integer is totalLiveDrives
     * <p>
     * Third integer is totalArchivedDrives
     *
     * @return the data provider {pageSize, totalLiveDrives, totalArchivedDrives}
     */
    @DataProvider
    public Object[][] getDrivesOfBothTypesNum() {
        return new Object[][]{{19, 100, 100}, {4, 38, 69}, {7, 82, 31}, {12, 4, 4}, {10, 9, 1}, {17, 0, 23}, {17, 23, 0}, {42, 178, 382}, {11, 11, 12}};
    }

    @Test(description = "Verifies the correct elements are returned with an ASC sort with a truly random amount of live and archived drives, and a random pageSize, with extra assertions", dataProvider = "getDrivesOfBothTypesNumRandom", invocationCount = 500)
    public void test_pageAndMerge_ascSort_bothDrives_multiPage_randomData(final int pageSize, final int numLiveDrives, final int numArchivedDrives) {
        log.debug("pageSize: {}", pageSize);
        PageRequest pageRequest = new PageRequest(0, pageSize, Sort.Direction.ASC, "timestamp");
        liveDrives = generateLiveDrives(pageRequest, numLiveDrives);
        archivedDrives = generateArchivedDrives(pageRequest, numArchivedDrives);
        int totalDrives = numLiveDrives + numArchivedDrives;

        Page<LiveDrive> results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                pageRequest);

        assertThat(results).isNotNull();

        if (totalDrives != 0) {
            assertThat(results.hasContent()).isTrue();
        }
        int totalFoundDrives = results.getNumberOfElements();

        if (totalDrives >= pageSize) {
            assertThat(results.getNumberOfElements()).isEqualTo(pageSize);
        }
        if (totalDrives > pageSize) {
            assertThat(results.hasNext()).isTrue();
        }

        int splitElement = numArchivedDrives >= pageSize ? pageSize : numArchivedDrives;
        List<LiveDrive> archivedResults = results.getContent().subList(0, splitElement);
        verifyAllArchived(archivedResults);
        verifyOrder(archivedResults, Sort.Direction.ASC);

        List<LiveDrive> liveResults = archivedResults.size() >= pageSize ? new ArrayList<>() : results.getContent().subList(splitElement, results.getContent().size());
        verifyAllLive(liveResults);
        verifyOrder(liveResults, Sort.Direction.ASC);

        while (results.hasNext()) {
            results = pager.pageAndMerge(liveQuery,
                    ld -> ld,
                    archivedQuery,
                    ad -> Translator.translate(ad).orElse(null),
                    results.nextPageable());

            int pageNumber = results.getNumber();
            if (numArchivedDrives >= (pageSize * (pageNumber + 1))) {
                splitElement = pageSize;
            } else if (numArchivedDrives > (pageSize * pageNumber)) {
                splitElement = numArchivedDrives % (pageSize * pageNumber);
            } else {
                splitElement = 0;
            }
            archivedResults = results.getContent().subList(0, splitElement);
            verifyAllArchived(archivedResults);
            verifyOrder(archivedResults, Sort.Direction.ASC);

            liveResults = archivedResults.size() >= pageSize ? new ArrayList<>() : results.getContent().subList(splitElement, results.getContent().size());
            verifyAllLive(liveResults);
            verifyOrder(liveResults, Sort.Direction.ASC);

            totalFoundDrives += results.getNumberOfElements();
        }

        assertThat(totalFoundDrives).isEqualTo(numLiveDrives + numArchivedDrives);
    }

    @Test(description = "Verifies the correct elements are returned with an DESC sort with a truly random amount of live and archived drives, and a random pageSize, with extra assertions", dataProvider = "getDrivesOfBothTypesNumRandom", invocationCount = 500)
    public void test_pageAndMerge_descSort_bothDrives_multiPage_random(final int pageSize, final int numLiveDrives, final int numArchivedDrives) {
        log.debug("pageSize: {}", pageSize);
        PageRequest pageRequest = new PageRequest(0, pageSize, Sort.Direction.DESC, "timestamp");
        liveDrives = generateLiveDrives(pageRequest, numLiveDrives);
        archivedDrives = generateArchivedDrives(pageRequest, numArchivedDrives);
        int totalDrives = numLiveDrives + numArchivedDrives;

        Page<LiveDrive> results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                pageRequest);

        assertThat(results).isNotNull();

        if (totalDrives != 0) {
            assertThat(results.hasContent()).isTrue();
        }
        int totalFoundDrives = results.getNumberOfElements();

        if (totalDrives >= pageSize) {
            assertThat(results.getNumberOfElements()).isEqualTo(pageSize);
        }
        if (totalDrives > pageSize) {
            assertThat(results.hasNext()).isTrue();
        }

        int splitElement = numLiveDrives >= pageSize ? pageSize : numLiveDrives;
        List<LiveDrive> liveResults = results.getContent().subList(0, splitElement);
        verifyAllLive(liveResults);
        verifyOrder(liveResults, Sort.Direction.DESC);

        List<LiveDrive> archivedResults = liveResults.size() >= pageSize ? new ArrayList<>() : results.getContent().subList(splitElement, results.getContent().size());
        verifyAllArchived(archivedResults);
        verifyOrder(archivedResults, Sort.Direction.DESC);

        while (results.hasNext()) {
            results = pager.pageAndMerge(liveQuery,
                    ld -> ld,
                    archivedQuery,
                    ad -> Translator.translate(ad).orElse(null),
                    results.nextPageable());

            int pageNumber = results.getNumber();
            if (numLiveDrives >= (pageSize * (pageNumber + 1))) {
                splitElement = pageSize;
            } else if (numLiveDrives > (pageSize * pageNumber)) {
                splitElement = numLiveDrives % (pageSize * pageNumber);
            } else {
                splitElement = 0;
            }
            liveResults = results.getContent().subList(0, splitElement);
            verifyAllLive(liveResults);
            verifyOrder(liveResults, Sort.Direction.DESC);

            archivedResults = liveResults.size() >= pageSize ? new ArrayList<>() : results.getContent().subList(splitElement, results.getContent().size());
            verifyAllArchived(archivedResults);
            verifyOrder(archivedResults, Sort.Direction.DESC);

            totalFoundDrives += results.getNumberOfElements();
        }

        assertThat(totalFoundDrives).isEqualTo(numLiveDrives + numArchivedDrives);
    }

    @Test(description = "Verifies the correct elements are returned with a DESC sort with ONLY live drives, multiple pages worth", dataProvider = "getDrivesOfSingleTypeNumRandom", invocationCount = 500)
    public void test_pageAndMerge_descSort_noArchivedDrives_multiPage_random(final int pageSize, final int totalDrives) {
        PageRequest pageRequest = new PageRequest(0, pageSize, Sort.Direction.DESC, "timestamp");
        liveDrives = generateLiveDrives(pageRequest, totalDrives);
        archivedDrives = new ArrayList<>();


        Page<LiveDrive> results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                pageRequest);

        assertThat(results).isNotNull();
        int expectedResults = totalDrives < pageSize ? totalDrives : pageSize;
        assertThat(results.getNumberOfElements()).isEqualTo(expectedResults);
        verifyOrder(results.getContent(), Sort.Direction.DESC);
        verifyAllLive(results.getContent());

        int returnedDrives = results.getContent().size();
        while (results.hasNext()) {

            results = pager.pageAndMerge(liveQuery,
                    ld -> ld,
                    archivedQuery,
                    ad -> Translator.translate(ad).orElse(null),
                    results.nextPageable());

            assertThat(results).isNotNull();

            int lastPageSize = totalDrives % pageSize == 0 ? pageSize : totalDrives % pageSize;
            int expectedSize = results.isLast() ? lastPageSize : pageSize;

            assertThat(results.getNumberOfElements()).isEqualTo(expectedSize);
            verifyOrder(results.getContent(), Sort.Direction.DESC);
            verifyAllLive(results.getContent());

            returnedDrives += results.getContent().size();
        }

        assertThat(returnedDrives).isEqualTo(totalDrives);
    }

    @Test(description = "Verifies the correct elements are returned with a DESC sort with ONLY live drives, multiple pages worth", dataProvider = "getDrivesOfSingleTypeNumRandom", invocationCount = 500)
    public void test_pageAndMerge_ascSort_noArchivedDrives_multiPage_random(final int pageSize, final int totalDrives) {
        PageRequest pageRequest = new PageRequest(0, pageSize, Sort.Direction.ASC, "timestamp");
        liveDrives = generateLiveDrives(pageRequest, totalDrives);
        archivedDrives = new ArrayList<>();


        Page<LiveDrive> results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                pageRequest);

        assertThat(results).isNotNull();
        int expectedResults = totalDrives < pageSize ? totalDrives : pageSize;
        assertThat(results.getNumberOfElements()).isEqualTo(expectedResults);
        verifyOrder(results.getContent(), Sort.Direction.ASC);
        verifyAllLive(results.getContent());

        int returnedDrives = results.getContent().size();
        while (results.hasNext()) {

            results = pager.pageAndMerge(liveQuery,
                    ld -> ld,
                    archivedQuery,
                    ad -> Translator.translate(ad).orElse(null),
                    results.nextPageable());

            assertThat(results).isNotNull();
            int lastPageSize = totalDrives % pageSize == 0 ? pageSize : totalDrives % pageSize;
            int expectedSize = results.isLast() ? lastPageSize : pageSize;

            assertThat(results.getNumberOfElements()).isEqualTo(expectedSize);
            verifyOrder(results.getContent(), Sort.Direction.ASC);
            verifyAllLive(results.getContent());

            returnedDrives += results.getContent().size();
        }

        assertThat(returnedDrives).isEqualTo(totalDrives);
    }

    @Test(description = "Verifies the correct elements are returned with a DESC sort with ONLY archived drives, multiple pages worth", dataProvider = "getDrivesOfSingleTypeNumRandom", invocationCount = 500)
    public void test_pageAndMerge_descSort_noLiveDrives_multiPage_random(final int pageSize, final int totalDrives) {
        PageRequest pageRequest = new PageRequest(0, pageSize, Sort.Direction.DESC, "timestamp");
        archivedDrives = generateArchivedDrives(pageRequest, totalDrives);
        liveDrives = new ArrayList<>();


        Page<LiveDrive> results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                pageRequest);

        assertThat(results).isNotNull();
        int expectedResults = totalDrives < pageSize ? totalDrives : pageSize;
        assertThat(results.getNumberOfElements()).isEqualTo(expectedResults);
        verifyOrder(results.getContent(), Sort.Direction.DESC);
        verifyAllArchived(results.getContent());

        int returnedDrives = results.getContent().size();
        while (results.hasNext()) {

            results = pager.pageAndMerge(liveQuery,
                    ld -> ld,
                    archivedQuery,
                    ad -> Translator.translate(ad).orElse(null),
                    results.nextPageable());

            assertThat(results).isNotNull();

            int lastPageSize = totalDrives % pageSize == 0 ? pageSize : totalDrives % pageSize;
            int expectedSize = results.isLast() ? lastPageSize : pageSize;

            assertThat(results.getNumberOfElements()).isEqualTo(expectedSize);
            verifyOrder(results.getContent(), Sort.Direction.DESC);
            verifyAllArchived(results.getContent());

            returnedDrives += results.getContent().size();
        }

        assertThat(returnedDrives).isEqualTo(totalDrives);
    }

    @Test(description = "Verifies the correct elements are returned with a DESC sort with ONLY live drives, multiple pages worth", dataProvider = "getDrivesOfSingleTypeNumRandom", invocationCount = 500)
    public void test_pageAndMerge_ascSort_noLiveDrives_multiPage_random(final int pageSize, final int totalDrives) {
        PageRequest pageRequest = new PageRequest(0, pageSize, Sort.Direction.ASC, "timestamp");
        archivedDrives = generateArchivedDrives(pageRequest, totalDrives);
        liveDrives = new ArrayList<>();


        Page<LiveDrive> results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                pageRequest);

        assertThat(results).isNotNull();
        int expectedResults = totalDrives < pageSize ? totalDrives : pageSize;
        assertThat(results.getNumberOfElements()).isEqualTo(expectedResults);
        verifyOrder(results.getContent(), Sort.Direction.ASC);
        verifyAllArchived(results.getContent());

        int returnedDrives = results.getContent().size();
        while (results.hasNext()) {

            results = pager.pageAndMerge(liveQuery,
                    ld -> ld,
                    archivedQuery,
                    ad -> Translator.translate(ad).orElse(null),
                    results.nextPageable());

            assertThat(results).isNotNull();
            int lastPageSize = totalDrives % pageSize == 0 ? pageSize : totalDrives % pageSize;
            int expectedSize = results.isLast() ? lastPageSize : pageSize;

            assertThat(results.getNumberOfElements()).isEqualTo(expectedSize);
            verifyOrder(results.getContent(), Sort.Direction.ASC);
            verifyAllArchived(results.getContent());

            returnedDrives += results.getContent().size();
        }

        assertThat(returnedDrives).isEqualTo(totalDrives);
    }

    /**
     * A data provider to return 2 integers.
     * <p>
     * First integer is pageSize
     * <p>
     * Second integer is totalNumberOfDrives
     * <p>
     * These results are completely randomized.
     *
     * @return the data provider {pageSize, totalNumberOfDrives}
     */
    @DataProvider
    public Object[][] getDrivesOfSingleTypeNumRandom() {
        return new Object[][]{{Math.abs(random.nextInt(100) + 1), Math.abs(random.nextInt(1000) + 1)}};
    }

    /**
     * A data provider to return 3 integers
     * <p>
     * First integer is pageSize
     * <p>
     * Second integer is totalLiveDrives
     * <p>
     * Third integer is totalArchivedDrives
     * <p>
     * These results are completely randomized.
     *
     * @return the data provider {pageSize, totalLiveDrives, totalArchivedDrives}
     */
    @DataProvider
    public Object[][] getDrivesOfBothTypesNumRandom() {
        return new Object[][]{
                {Math.abs(random.nextInt(100) + 1), Math.abs(random.nextInt(500) + 1), Math.abs(random.nextInt(500) + 1)},
                {Math.abs(random.nextInt(100) + 1), Math.abs(random.nextInt(700) + 1), Math.abs(random.nextInt(300) + 1)},
                {Math.abs(random.nextInt(100) + 1), Math.abs(random.nextInt(200) + 1), Math.abs(random.nextInt(800) + 1)},
                {Math.abs(random.nextInt(100) + 1), Math.abs(random.nextInt(900) + 1), Math.abs(random.nextInt(100) + 1)}
        };
    }

    @Test(description = "paging and merging with mixed drive types to verify we can page forwards and backwards")
    public void test_pageAndMerge_mixedDrives_forwardAndBackwardPaging(){
        final int pageSize = 5;
        final int numLiveDrives = 8;
        final int numArchivedDrives = 7;

        log.debug("pageSize: {}", pageSize);
        PageRequest pageRequest = new PageRequest(0, pageSize, Sort.Direction.DESC, "timestamp");
        liveDrives = generateLiveDrives(pageRequest, numLiveDrives);
        archivedDrives = generateArchivedDrives(pageRequest, numArchivedDrives);

        // first request, all live
        Page<LiveDrive> results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                pageRequest);

        assertThat(results).isNotNull();
        verifyAllLive(results.getContent());

        assertThat(results.hasNext()).isTrue();

        results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                results.nextPageable());

        List<LiveDrive> mixedContent = results.getContent();
        verifyAllLive(mixedContent.subList(0, 3));
        verifyAllArchived(mixedContent.subList(3, 5));

        assertThat(results.hasNext()).isTrue();

        results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                results.nextPageable());

        assertThat(results.hasNext()).isFalse();
        assertThat(results.hasPrevious()).isTrue();
        assertThat(results.isLast()).isTrue();

        verifyAllArchived(results.getContent());

        // go back a page to the mixed results
        results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                results.previousPageable());

        mixedContent = results.getContent();
        verifyAllLive(mixedContent.subList(0, 3));
        verifyAllArchived(mixedContent.subList(3, 5));

        assertThat(results.hasNext()).isTrue();
        assertThat(results.hasPrevious()).isTrue();


        results = pager.pageAndMerge(liveQuery,
                ld -> ld,
                archivedQuery,
                ad -> Translator.translate(ad).orElse(null),
                results.previousPageable());

        verifyAllLive(results.getContent());

        assertThat(results.hasNext()).isTrue();
        assertThat(results.hasPrevious()).isFalse();
        assertThat(results.isFirst()).isTrue();
    }
}
