package com.erwolff.pagination;

import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

public class OffsetAdjustedPageRequest extends PageRequest {

    private final int skipped;

    /**
     * The first page of this Pageable is the number of items passed in as {@link OffsetAdjustedPageRequest#skipped}, s
     * o in effect, we only have a page 0 for this implementation if skipped = 0
     * @param skipped the number of results we skip. i.e. these first n results cannot be returned from this page request
     * @param page the page number we are on
     * @param size the size of pages to return
     * @param direction the direction to sort items
     * @param properties to sort by.
     */
    public OffsetAdjustedPageRequest(int skipped, int page, int size, Sort.Direction direction, String... properties) {
        super(page, size, direction, properties);
        this.skipped = skipped;
    }

    /**
     * tweak offset functionality so that we have granular control
     * over requests to never need to request more results than are required at a time.
     * @return the offset, which is always the number of skipped results + the default offset.
     */
    @Override
    public int getOffset(){
        return skipped + super.getOffset();
    }



}
