package com.erwolff.pagination;

import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.*;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Provides helper utilities for paging results from the DB
 */
public class Pager {
    private static final Logger log = LoggerFactory.getLogger(Pager.class.getSimpleName());

    private static final String DEFAULT_SORT_FIELD = "timestamp";
    private static final Sort.Direction DEFAULT_SORT_DIRECTION = Sort.Direction.DESC;
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
        // check both paths of short-circuiting, and if not proceed to a merge of the dual result types.
        if (secondaryResults.getTotalElements() == 0) {
            // the results are all of the initial type, so we'll craft a page of this and return it.
            return binaryResult(initialResults, initialMappingFunction);
        } else if (initialResults.getTotalElements() == 0) {
            // if the initial results have no elements, everything is in the secondary results.
            // the results are all of the secondary type, so we'll craft a page of this and return it.
            return binaryResult(secondaryResults, secondaryMappingFunction);
        } else {
            // results are not of one type exclusively, so we'll need to merge as we go.
            return dualResult(initialResults, initialMappingFunction,secondaryResults, secondaryMappingFunction, secondaryQuery, pageable, totalElements, sort);
        }
    }

    /**
     * A short circuit helper function that will convert results of one type to the final paged results.
     *
     * @param anyResults      any initial starting type
     * @param mappingFunction a mapping function to transform from initial type to result type
     * @param <ANY>           any original type
     * @param <RESULT>        the final result type
     * @return a transformed page of results.
     */
    private <ANY, RESULT> Page<RESULT> binaryResult(Page<ANY> anyResults, Function<ANY, RESULT> mappingFunction) {
        // just convert to the result type and return
        return anyResults.map(mappingFunction::apply);
    }

    /**
     * A helper function that handles the merging of paging objects when we have results of multiple types.
     * <p>
     * We only need to re-query the secondary objects when we've reached them and are trying to merge them into the final paging object.
     *
     * @param initialResults           the initial objects
     * @param initialMappingFunction   a mapping function to translate from initial to result
     * @param secondaryMappingFunction a mapping function to translate from secondary to result
     * @param secondaryQuery           a query that can return the secondary objects again from any starting point.
     * @param pageable                 the pageable object which tells us where we are at
     * @param numberOfItems            the number of total items
     * @param sort                     the sort
     * @param <INITIAL>                the first starting type
     * @param <SECONDARY>              the second starting type
     * @param <RESULT>                 the final mapped result type
     * @return a page of the result type
     */
    private <INITIAL, SECONDARY, RESULT> Page<RESULT> dualResult(Page<INITIAL> initialResults,
                                                                 Function<INITIAL, RESULT> initialMappingFunction,
                                                                 Page<SECONDARY> secondaryResults,
                                                                 Function<SECONDARY, RESULT> secondaryMappingFunction,
                                                                 Function<Pageable, Page<SECONDARY>> secondaryQuery,
                                                                 Pageable pageable,
                                                                 long numberOfItems,
                                                                 Sort.Order sort) {
        // we'll need this in multiple paths, so we'll extract it once for re-use and readability.
        int pageSize = pageable.getPageSize();

        // there is one of 3 states,
        // 1) we are on a page of only initial results
        // 2) we are on a page of merged initial results & secondary results
        // 3) we are on a page of only secondary results of which we need to throw away results that were on the merged page
        if (isFullPage(initialResults)) {
            // convert and return this page taking into account that that we have a merged count.
            return new PageImpl<>(initialResults.map(initialMappingFunction::apply).getContent(), pageable, numberOfItems);
        } else if (initialResults.getNumberOfElements() != 0) {

            // Take the initial results, map to results, get the content,
            // re-query the secondary results, map to results,
            // add this content to the original content
            List<RESULT> resultsList = toMutableList(initialResults.map(initialMappingFunction::apply));

            // calculate how many items we need to fill the page
            int secondarySizeNeeded = pageSize - resultsList.size();

            // make this page request so we only pull back what we need from secondary
            Pageable secondaryPageRequest = new PageRequest(0, secondarySizeNeeded, sort.getDirection(), DEFAULT_SORT_FIELD);
            // Add these secondary items to our results list
            resultsList.addAll(secondaryQuery.apply(secondaryPageRequest)
                    .map(secondaryMappingFunction::apply)
                    .getContent());

            // return our merged results in a page.
            return new PageImpl<>(resultsList, pageable, numberOfItems);
        } else {
            // calculate the offset which is the number of items on the partial page of initial results
            // these were accounted for in the page of mixed results.
            // we then take the positive difference of page size - the number of items accounted for in the merged page
            // after we calculate our page size, we ensure it's less than the page size by modding it again by pageSize.
            int offset = (pageSize - Math.toIntExact(initialResults.getTotalElements() % pageSize)) % pageSize;
            // calculate the page number we need to be on for this request by getting the page of the secondary results,
            // and subtracting the total number of pages in the first set of results
            int pageNumber = pageable.getPageNumber() - initialResults.getTotalPages();
            // Create a new, secondary page request that takes the offset from the initial results into account.
            Pageable secondaryPageRequest = new OffsetAdjustedPageRequest(offset, pageNumber, pageable.getPageSize(), sort.getDirection(), DEFAULT_SORT_FIELD);
            // re-query the secondary results to get back to the start
            Page<RESULT> newSecondary = secondaryQuery.apply(secondaryPageRequest).map(secondaryMappingFunction::apply);
            return new PageImpl<>(newSecondary.getContent(), pageable, numberOfItems);
        }
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
     * Get the contents of a page and return it to a mutable list
     * <p>
     * Useful if there is a need to append content (say, from a second source)
     *
     * @param page  the page to get content from
     * @param <ANY> type param of the page and returned list
     * @return the mutable list from the page content.
     */
    private <ANY> List<ANY> toMutableList(Page<ANY> page) {
        return new ArrayList<>(page.getContent());
    }
}
