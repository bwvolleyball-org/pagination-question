package com.erwolff.pagination;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.erwolff.data.Timestamped;
import com.google.common.collect.Iterators;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.comparators.ComparatorChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.*;
import org.springframework.util.Assert;

/**
 * Provides helper utilities for paging results from the DB
 */
public class Pager {
    private static final Logger log = LoggerFactory.getLogger(Pager.class.getSimpleName());

    static final String DEFAULT_SORT_FIELD = "timestamp";
    static final Sort.Direction DEFAULT_SORT_DIRECTION = Sort.Direction.DESC;
    static final Sort.Order DEFAULT_SORT = new Sort.Order(DEFAULT_SORT_DIRECTION, DEFAULT_SORT_FIELD);

    /**
     * Performs pagination over the two collections using the supplied queries and mapping the results to the specified RESULT object
     *
     * @param liveQuery               - the query against the live collection
     * @param archivedQuery           - the query against the archived collection
     * @param liveMappingFunction     - the function which maps live collection results to the RESULT object
     * @param archivedMappingFunction - the function which maps archived collection results to the RESULT object
     * @param pageable                - the page request
     * @return - an org.springframework.data.Page of type RESULT
     */
    public <LIVE extends Timestamped, ARCHIVED extends Timestamped, RESULT> Page<RESULT> pageAndMerge(Function<Pageable, Page<LIVE>> liveQuery,
                                                              Function<LIVE, RESULT> liveMappingFunction,
                                                              Function<Pageable, Page<ARCHIVED>> archivedQuery,
                                                              Function<ARCHIVED, RESULT> archivedMappingFunction,
                                                              Pageable pageable) {
        if (pageable.getPageSize() <= 0) {
            String message = "Page size must be greater than 0";
            log.error(message);
            // Let's assume that we have exception handling which maps these IllegalArgumentExceptions into HttpStatus 400s
            throw new IllegalArgumentException(message);
        }

        // COMPLETED HINT: What are some other error cases we should handle?
        if (pageable.getPageNumber() < 0) {
            String message = "Page Number must be non-negative!";
            log.error(message);
            throw new IllegalArgumentException(message);
        }


        // Here's some initial help to get the ball rolling. We're going to perform the queries against the two
        // collections, then we'll calculate the total elements that were returned, determine the sort that was supplied,
        // and call a helper function which will perform the complex logic. We're doing these steps first so that the
        // helper function doesn't need to know the difference between archived vs live, and instead can just perform its logic
        // agnostic of that concept


        // perform both queries in order to be able to calculate the total number of results between collections
        Page<LIVE> liveResults = liveQuery.apply(pageable);
        Page<ARCHIVED> archivedResults = archivedQuery.apply(pageable);

        // calculate total elements
        long totalElements = liveResults.getTotalElements() + archivedResults.getTotalElements();

        // determine the sort requested (we're only going to worry about a single sort - multiple sorting is too complex for this exercise)
        Sort.Order sort = Iterators.get(pageable.getSort().iterator(), 0);

        if (sort == null) {
            // a sort was not supplied, we'll use our default sort
            sort = DEFAULT_SORT;
        }

        if (Sort.Direction.ASC == sort.getDirection()) {
            // sort is ASC: archivedResults are initial, liveResults are secondary
            return pageAndMerge(archivedResults, archivedMappingFunction, liveResults, liveQuery, liveMappingFunction, totalElements, pageable, sort);
        }

        // sort is DESC: liveResults are initial, archivedResults are secondary
        return pageAndMerge(liveResults, liveMappingFunction, archivedResults, archivedQuery, archivedMappingFunction, totalElements, pageable, sort);
    }

    /**
     * Performs pagination over the two collections using the supplied queries and mapping the results to the specified RESULT object
     * <p>
     * Reference the README.md for example output of this function
     *
     * @param initialResults           - the results from the first query performed based on the supplied sort (ASC: initial = archived, DESC: initial = live)
     * @param initialMappingFunction   = the function which maps the first query results to the RESULT object
     * @param secondaryResults         - the results from the second query performed based on the supplied sort (ASC: secondary = live, DESC: secondary = archived)
     * @param secondaryQuery           - the query to perform to retrieve secondary results based on the supplied sort (ASC: live query, DESC: archived query)
     * @param secondaryMappingFunction - the function which maps the second query results to the RESULT object
     * @param totalElements            - the combined total number of elements from both queries
     * @param pageable                 - the page request
     * @param sort                     - the sort field and direction
     * @return - an org.springframework.data.Page of type RESULT
     */
    private <RESULT, INITIAL extends Timestamped, SECONDARY extends Timestamped> Page<RESULT> pageAndMerge(Page<INITIAL> initialResults,
                                                                   Function<INITIAL, RESULT> initialMappingFunction,
                                                                   Page<SECONDARY> secondaryResults,
                                                                   Function<Pageable, Page<SECONDARY>> secondaryQuery,
                                                                   Function<SECONDARY, RESULT> secondaryMappingFunction,
                                                                   long totalElements,
                                                                   Pageable pageable,
                                                                   Sort.Order sort) {
        // if we only have results of one type, we can short circuit and return a binary result(page of one type)
        if(secondaryResults.getTotalElements() == 0){
            return binaryResult(initialResults, initialMappingFunction, pageable, totalElements, sort);
        }
        // if the initial results have no elements, everything is in the secondary results.
        if (initialResults.getTotalElements() == 0){
            return binaryResult(secondaryResults, secondaryMappingFunction, pageable, totalElements, sort);
        }
        // results are not of one type exclusively, so we'll need to merge as we go.
        return dualResult(initialResults, initialMappingFunction, secondaryResults, secondaryMappingFunction, secondaryQuery, pageable, totalElements, sort);
    }

    /**
     * Determines whether the supplied Page has a full set of results
     *
     * @param page - the supplied page
     * @return true IFF the supplied page has a full set of results
     */
    private boolean isFullPage(Page<?> page) {
        // COMPLETED HINT: write this logic - consider page.hasContent(), page.getSize(), page.getNumberOfElements()
        // case of size = 0 is illegal and will not happen, so page size is > 0
        // ensure page has content & the size is equivalent to the number of elements.
        return page.hasContent() && (page.getSize() == page.getNumberOfElements());
    }

    /**
     * @param <RESULT> a generic type, can be anything, intended to be used on 'RESULT' types.
     * @return a {@link Page<RESULT>} from the stream termination.
     */
    private <RESULT> Collector<RESULT, List<RESULT>, Page<RESULT>> toPage(Pageable pageable, long numberOfItems) {
        return Collector.of(
                // start with a new list
                ArrayList::new,
                // add new items into this list
                List::add,
                // if we end up with multiple lists (parallel streams), add everything to the (arbitrary) left list
                (left, right) -> {
                    left.addAll(right);
                    return left;
                },
                // finalize the collection by passing the list to a new PageImpl.
                (results) -> new PageImpl<>(results, pageable, numberOfItems)
        );
    }

    /**
     *
     * @param page to stream the contents of
     * @param <ANY> the generic type of the page
     * @return a stream of the page
     */
    private <ANY> Stream<ANY> streamOf(Page<ANY> page){
        return StreamSupport.stream(page.spliterator(), false);
    }

    /**
     * A short circuit helper function that will convert results of one type to the final paged results.
     *
     * @param anyResults any initial starting type
     * @param mappingFunction a mapping function to transform from initial type to result type
     * @param pageable the original pageable - we don't need to modify since it's calculations are correct for results of one starting type
     * @param numberOfItems the total number of items (which are all of one type).
     * @param sort the sort declaration
     * @param <ANY> any original type
     * @param <RESULT> the final result type
     * @return a transformed page of results.
     */
    private <ANY extends Timestamped,RESULT> Page<RESULT> binaryResult(Page<ANY> anyResults, Function<ANY, RESULT> mappingFunction, Pageable pageable, long numberOfItems, Sort.Order sort){
        Comparator<ANY> comparator = comparator(sort);
            return streamOf(anyResults).sorted(comparator).map(mappingFunction).collect(toPage(pageable, numberOfItems));
    }

    /**
     * A helper function that handles the merging of paging objects when we have results of multiple types.
     *
     * We only need to re-query the secondary objects when we've reached them and are trying to merge them into the final paging object.
     *
     * @param initialResults the initial objects
     * @param initialMappingFunction a mapping function to translate from initial to result
     * @param secondaryResults the secondary objects
     * @param secondaryMappingFunction a mapping function to translate from secondary to result
     * @param secondaryQuery a query that can return the secondary objects again from any starting point.
     * @param pageable the pageable object which tells us where we are at
     * @param numberOfItems the number of total items
     * @param sort the sort
     * @param <INITIAL> the first starting type
     * @param <SECONDARY> the second starting type
     * @param <RESULT> the final mapped result type
     * @return a page of the result type
     */
    private <INITIAL extends Timestamped, SECONDARY extends Timestamped, RESULT> Page<RESULT> dualResult(Page<INITIAL> initialResults,
                                                                                                         Function<INITIAL, RESULT> initialMappingFunction,
                                                                                                         Page<SECONDARY> secondaryResults,
                                                                                                         Function<SECONDARY, RESULT> secondaryMappingFunction,
                                                                                                         Function<Pageable, Page<SECONDARY>> secondaryQuery,
                                                                                                         Pageable pageable,
                                                                                                         long numberOfItems,
                                                                                                         Sort.Order sort){
        Comparator<Timestamped> comparator = comparator(sort);
        int pageSize = pageable.getPageSize();
        if(isFullPage(initialResults)){
            Pageable mergedPageable = new PageRequest(pageable.getPageNumber(), pageSize, sort.getDirection(), DEFAULT_SORT_FIELD);
            return streamOf(initialResults).limit(pageable.getPageSize()).sorted(comparator).map(initialMappingFunction).collect(toPage(mergedPageable, numberOfItems));
        } else if (initialResults.getNumberOfElements() != 0){
            Pageable mergedPageable = new PageRequest(pageable.getPageNumber(), pageSize, sort.getDirection(), DEFAULT_SORT_FIELD);
            int initialElements = initialResults.getNumberOfElements();
            int secondaryElements = pageable.getPageSize() - initialElements;

            Page<SECONDARY> newSecondary = secondaryQuery.apply(pageable.first());

            Stream<RESULT> resultInitial = streamOf(initialResults).sorted(comparator).map(initialMappingFunction);
            Stream<RESULT> resultSecondary = streamOf(newSecondary).limit(secondaryElements).sorted(comparator).map(secondaryMappingFunction);
            return concatStreamsToPage(resultInitial, resultSecondary, mergedPageable, numberOfItems);
        } else {
            Page<SECONDARY> newSecondary = secondaryQuery.apply(pageable.first());

            long offset = (initialResults.getTotalElements() % pageSize) * (secondaryResults.getTotalPages() - 1);

            int page = pageable.getPageNumber() + initialResults.getNumberOfElements() / initialResults.getTotalPages();
            Pageable mergedPageable = new PageRequest(page, pageSize, sort.getDirection(), DEFAULT_SORT_FIELD);
            return streamOf(newSecondary).skip(offset).limit(pageSize).sorted(comparator).map(secondaryMappingFunction).collect(toPage(mergedPageable, numberOfItems));
        }
    }

    private <ANY extends Timestamped> Comparator<ANY> comparator(Sort.Order sort){
        Comparator<ANY> comparator = Comparator.comparingLong(Timestamped::getTimestamp);
        return new ComparatorChain<>(comparator, sort.isAscending());
    }

    /**
     * A helper function that can concatenate streams to a final merged stream, collected into a page.
     *
     * @param first the first stream
     * @param second the second stream
     * @param pageable the pageable which keeps track of our place
     * @param numberOfItems the total number of items
     * @param <RESULT> the result type
     * @return the page of merged / concatenated results.
     */
    private <RESULT> Page<RESULT> concatStreamsToPage(Stream<RESULT> first, Stream<RESULT> second, Pageable pageable, long numberOfItems){
        return Stream.concat(first, second).collect(toPage(pageable, numberOfItems));
    }
}
