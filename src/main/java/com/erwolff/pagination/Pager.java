package com.erwolff.pagination;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
    public <LIVE, ARCHIVED, RESULT> Page<RESULT> pageAndMerge(Function<Pageable, Page<LIVE>> liveQuery,
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
    private <RESULT, INITIAL, SECONDARY> Page<RESULT> pageAndMerge(Page<INITIAL> initialResults,
                                                                   Function<INITIAL, RESULT> initialMappingFunction,
                                                                   Page<SECONDARY> secondaryResults,
                                                                   Function<Pageable, Page<SECONDARY>> secondaryQuery,
                                                                   Function<SECONDARY, RESULT> secondaryMappingFunction,
                                                                   long totalElements,
                                                                   Pageable pageable,
                                                                   Sort.Order sort) {
        // if we only have results of one type, we can short circuit and return a binary result [page(s) of one type]
        if(secondaryResults.getTotalElements() == 0){
            // the results are all of the initial type, so we'll craft a page of this and return it.
            return binaryResult(initialResults, initialMappingFunction, pageable, totalElements);
        }
        // if the initial results have no elements, everything is in the secondary results.
        if (initialResults.getTotalElements() == 0){
            // the results are all of the secondary type, so we'll craft a page of this and return it.
            return binaryResult(secondaryResults, secondaryMappingFunction, pageable, totalElements);
        }
        // results are not of one type exclusively, so we'll need to merge as we go.
        return dualResult(initialResults, initialMappingFunction, secondaryMappingFunction, secondaryQuery, pageable, totalElements, sort);
    }

    /**
     * Determines whether the supplied Page has a full set of results
     *
     * @param page - the supplied page
     * @return true IFF the supplied page has a full set of results
     */
    private boolean isFullPage(Page<?> page) {
        // case of size = 0 is illegal and will not happen, so page size is > 0
        // ensure page has content & the size is equivalent to the number of elements.
        return page.hasContent() && (page.getSize() == page.getNumberOfElements());
    }

    /**
     * Creates a collector that allows terminating a stream into a Page of whatever type the stream is of.
     *
     * Builds a list of the items in the stream, and upon finalization takes the completed list and returns a new
     * {@link PageImpl} with the list of results, the provided pageable, and the number of items provided.
     *
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
     * Allow us to turn any page into a non-parallel stream (to preserve order)
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
     * @param <ANY> any original type
     * @param <RESULT> the final result type
     * @return a transformed page of results.
     */
    private <ANY,RESULT> Page<RESULT> binaryResult(Page<ANY> anyResults, Function<ANY, RESULT> mappingFunction, Pageable pageable, long numberOfItems){
            return streamOf(anyResults)
                    .map(mappingFunction)
                    .collect(toPage(pageable, numberOfItems));
    }

    /**
     * A helper function that handles the merging of paging objects when we have results of multiple types.
     *
     * We only need to re-query the secondary objects when we've reached them and are trying to merge them into the final paging object.
     *
     * @param initialResults the initial objects
     * @param initialMappingFunction a mapping function to translate from initial to result
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
    private <INITIAL, SECONDARY, RESULT> Page<RESULT> dualResult(Page<INITIAL> initialResults,
                                                                 Function<INITIAL, RESULT> initialMappingFunction,
                                                                 Function<SECONDARY, RESULT> secondaryMappingFunction,
                                                                 Function<Pageable, Page<SECONDARY>> secondaryQuery,
                                                                 Pageable pageable,
                                                                 long numberOfItems,
                                                                 Sort.Order sort){
        // we'll need this in multiple paths, so we'll extract it once for re-use and readability.
        int pageSize = pageable.getPageSize();

        if(isFullPage(initialResults)){
            return streamOf(initialResults).
                    // only take as many as our page size
                    limit(pageSize)
                    // transform to the results type
                    .map(initialMappingFunction)
                    // collect it to a page
                    .collect(toPage(pageable, numberOfItems));
        } else if (initialResults.getNumberOfElements() != 0){
            int initialElements = initialResults.getNumberOfElements();
            int secondaryElements = pageSize - initialElements;

            // re-query the secondary results to get back to the start
            Page<SECONDARY> newSecondary = secondaryQuery.apply(pageable.first());

            // drain the rest of the initial results
            Stream<RESULT> resultInitial = streamOf(initialResults)
                    // apply the transformation and don't terminate the stream
                    .map(initialMappingFunction);
            // take enough from the re-queried secondary results to fill the page
            Stream<RESULT> resultSecondary = streamOf(newSecondary)
                    // limit to the difference needed to fill a page
                    .limit(secondaryElements)
                    // transform to a result and don't terminate the stream
                    .map(secondaryMappingFunction);
            // return the concatenated results.
            return concatStreamsToPage(resultInitial, resultSecondary, pageable, numberOfItems);
        } else {
            // re-query the secondary results to get back to the start
            Page<SECONDARY> newSecondary = secondaryQuery.apply(pageable.first());

            // calculate the offset which is the number of items on the partial page of initial results
            // these were accounted for in the page of mixed results.
            long offset = initialResults.getTotalElements() % pageSize;

            // calculate the new page number, which is the page number of results we are on
            // + the total number of pages in the initial results, which will account for the partial page (offset)
            // we are skipping from this result.
            int page = pageable.getPageNumber() + initialResults.getTotalPages();
            // since we are exclusively onto secondary results, we need to fast-forward the page number of the pageable
            // by the number of initialResults's total pages
            Pageable mergedPageable = new PageRequest(page, pageSize, sort.getDirection(), DEFAULT_SORT_FIELD);

            return streamOf(newSecondary)
                    // skip what was included in the mixed result type page
                    .skip(offset)
                    // keep enough to fill the page
                    .limit(pageSize)
                    // transform to the correct result type
                    .map(secondaryMappingFunction)
                    // collect it to a page
                    .collect(toPage(mergedPageable, numberOfItems));
        }
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
        // append the second stream to the first one, then terminate to a page.
        return Stream.concat(first, second).collect(toPage(pageable, numberOfItems));
    }
}
