package com.erwolff.pagination;

import java.util.function.Function;
import java.util.stream.StreamSupport;

import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.*;

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

        //TODO: What are some other error cases we should handle?

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
        // check if the initialResults page is already full
        if (isFullPage(initialResults)) {
            StreamSupport.stream(initialResults.spliterator(), false).map(initialMappingFunction)
            // TODO: initial results contained a full page, what should we do?
            return null;
        }

        // TODO: check if all total results reside in the secondary collection - then we can short-circuit and return the results immediately

        //TODO: The rest is up to you! Some things to consider:
        // Remember, we have the FULL results of the query from both collections - we need to whittle down those results to the correct results to be returned.
        // Maybe keep track of total size vs the remaining size needed to fill out the results page.
        // Consider a cursor or an offset calculation.
        // Remember to apply the mapping functions!

        return null;
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
}
