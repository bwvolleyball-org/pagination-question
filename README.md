# pagination-question
This is an exercise designed to be taken home and worked on as part of a developer interview process.
The exercise revolves around the principles of pagination. There are many useful libraries for paginating results (e.g. from a database), however, these libraries are designed to paginate from only a single "collection".

**What if we have multiple collections that we need to paginate from?**

 This question is the heart of the exercise.

 This exercise is designed to test a few specific things:

  1. Can you work with pre-existing libraries and understand their functionality (and shortcomings)?
  2. Can you handle edge cases and determine potential error scenarios?
  3. Do you know how to work with some decently advanced aspects of the Java language?
  4. Are you able to write a complex algorithm with an elegant solution?

 This question is challenging. We're **not** necessarily looking for a perfect solution (though if you solve it, we will shower you with praise and many bonus points)!

 What we **are** looking for is _clean, efficient, well-documented code_.

 Read our in-line comments, take your time, and have FUN :)

 ## The Scenario

 We have two collections in a database. One is for LIVE data (i.e. data that belongs to an ongoing operation), one is for ARCHIVED data (i.e. data relating to an operation that has already finished). Let's use a real-world example to illustrate the situation.

 We have a service which tracks delivery drivers as they are driving to and from their destinations. At any one time, thousands of delivery drivers may be driving around the country. It would be extremely inefficient of us to query a database for an ongoing drive and have to query against a collection which contains all drives that have ever happened. Therefore, we make the decision to use two separate collections: a LIVE collection (storing all currently ongoing drives), and an ARCHIVED collection (storing all drives which have ended). When a driver reaches a destination and turns off his/her car, our service will mark the drive as "finished", then transfer it from the LIVE collection into the ARCHIVED collection.

 This all seems simple enough, and we're happy with the efficiency. Now, however, we've encountered a difficulty: our service must support a Web UI which shows all drives for a specific account in a sortable order. Naturally, our Web UI must remain quick and snappy, therefore it cannot perform any complex logic. It simply makes a request to our server asking for a page of results (and the sort order) to display.

 **It's up to our server to supply the correct results based on the page and sort requested**.

 For simplicity's sake, we'll skip all of the external aspects (REST endpoints, DAO layer, etc). We'll just tackle the algorithm that our server needs to implement in order to paginate over two separate collections while maintaining a sort.
 
 _Why Generics?_
 
 We'd like to use Java Generics because this isn't the only type of data that we're planning on storing a Live version and an Archived version of. We want to be able to use this same function for any time we'd like to iterate over two collections, no matter the type of data in those collections.

 Oh, and one more thing: a `LiveDrive` object, is not the same as an `ArchivedDrive` object. Our Web UI only knows how to handle `LiveDrive` objects. Therefore, we'll need this function to be able to map results to the required object type. We'll add this as a parameter to the function so it's up to the caller to specify HOW to do the mapping - but we can't forget to actually perform the mapping!  

 ## Example

 Let's demonstrate an example of what we're looking for in terms of inputs and outputs for this function.

 **Given**: requested page size is 6  
 **Given**: sort is DESC (by timestamp)  
 **Given**: total archived results for this query is 20  
 **Given**: total live results for this query is 20  

**Expected Results**  
 **Page 0**: LIVE results: [39,38,37,36,35,34]

 _Note: Assume that these numbers are timestamps - it's much easier to think about them as small numbers though. Because the sort is DESC, the most recent results must be returned first. This means that the first page (page 0) must start with the LIVE collection's results. In this case, the page size is 6, the live results contained 20 elements (after running the query against the live collection) so all results returned will be solely from the live collection. The elements returned are elements 39,38,37,36,35,34. Remember, the greater the timestamp, the more "recent" it is, hence 39 is more recent than 34._

 **Page 1**: LIVE results: [33,32,31,30,29,28]  
 **Page 2**: LIVE results: [27,26,25,24,23,22]  
 **Page 3**: LIVE results: [21,20]  ARCHIVED results: [19,18,17,16]

 _Note: In this case, the live results did not contain enough elements to fill the requested page size. Therefore we needed to supplement the page with archived results. Remember in this case that the archived results need to be mapped to the same object type as the live results - this was supplied as an argument, so just perform the mapping!_

**Page 4**: ARCHIVED results: [15,14,13,12,11,10]  
**Page 5**: ARCHIVED results: [9,8,7,6,5,4]  
**Page 6**: ARCHIVED results: [3,2,1,0]  

_Note: This last page isn't a full page, but we ran out of results so that's fine._


## Goals

1. Complete the `Pager` class
2. Verify that all tests pass in the `PagerTest` class (without altering the tests)
3. Enjoy :)

## Pit Mutation Testing

If you are curious to see the strength of the solution, and which lines are adequately covered by the tests,
you can use mutation testing to achieve this.
For an explanation of what mutation testing is, head to [Pitest's official website](http://pitest.org/)!

To run mutation testing against the Pager test, simply run

`mvn org.pitest:pitest-maven:mutationCoverage`

and then open [the report](./target/pit-reports/index.html) in your favorite browser!

Note, due to classpath errors, this project uses `v1.2.5` of the maven pitest plugin, however the latest version is `v1.4.5`.
`v1.3.x` & `v1.4.x` are seemingly incompatible with maven `v3.6.x`, so to experiment with higher versions of pitest, 
the maven wrapper could assist with versioning down maven.
