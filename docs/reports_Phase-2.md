# DATA SYSTEMS

## PROJECT PHASE 2

<p align = 'right'>
Souvik Karfa (2020102051)</br>
Trisha Kaore (2020111021)</br>
Anish Mathur (2020102044)</br>
</p>

## Task 1: SORT

### 1. `sortTable()`

-   This function is a driver function that calls the two phases of external sorting: <br>
    1. `sortingPhase`
    2. `mergingPhase`

### 2. `getColumnIndex()`

-   This is an overloaded version of the already existing function, to give indices of multiple columns at the same time in a more efficient manner.

### 2. `sortingPhase()`

-   This functions reads `<BLOCK_COUNT>` pages at a time, sorts them in memory and writes them back to temporary pages, thus creating the first run of our sorting algorithm.

-   To sort the rows with respect to multiple columns, we use a priority queue, with a custom lambda function as the comparator.

-   The name of each temporary page follows the following convention: `<tableName_run_subfile_pageIndex>`

### 3. `mergingPhase()`

-   This function merges the sorted subfiles created in the previous run and creates a new sorted subfile.

-   This is done by bringing in `<BLOCK_COUNT>` subfiles into the memory, and merging them to create a new subfile. As the existing pages of the subfiles are merged, new pages of the same subfile are brought into the memory.

-   After each merge operation, the number of subfiles is descreased by a factor of `<BLOCK_COUNT>`, and number of pages per subfile increases by a factor of `<BLOCK_COUNT>`.

-   The merge operation is done until the number of subfiles become `1`.

-   After that the original pages are rewritten with the sorted pages.

-   After starting the merge of the current run, all the temporary pages of the previous run are deleted.

-   This function also uses a priority queue, with the same custom lambda function as the comparator to merge the subfiles.

## Task 2: JOIN

-   The join function lists all rows of from two tables which satisfy a certain specified condition.

### 1. `executeJOIN()`

-   This function is a driver function that calls the two types of join algorithms based on the predicate: <br>
    1. `sortedMergeJoin` (in case of $==$ predicate)
    2. `nestedBlockJoin` (otherwise)

### 2. `sortedMergeJoin()`

-   This function is used to Join on the $==$ operator.
-   In this function we sort both the tables according to the given columns.
-   Then we traverse the rows of `Table 1` comparing the values in rows of `Table 2`, using 2 pointers (cursor2 and cursor3) to store the last relation which satisfied the equality to make the join more efficient.
-   The overall average block accesses are $m+n$, excluding those needed for the sorting process.

### 3. `nestedBlockJoin()`

-   This function is used to join every other operator. In this function we do not have to sort the tables, we merely choose the table with the lesser number of blocks as the outer loop and the other as the inner loop.
-   Then for every record in the outer loop we traverse the inner loop and concatenate the relations which match the predicate conditions.

## Task 2: ORDER BY

-   The order by function sorts the table based on the given column and in the specified order into a new table.

### 1. `executeORDERBY()`

-   This function can be understood as an extension of the external sort function implemented in Task - 1. We break down this task by creating a new table and copying all the rows individually from the previous table.

-   Then we store the new table in the Table Catalogue and sort it according to the column specified in an ascending or descending manner.

## Task 2: GROUP BY

### 1. `executeGROUPBY()`

-   This function executes the `GROUP BY` command, by first sorting the table with respect to the `<grouping_column>`.
-   This is done to calculate the aggregate functions of each group together at once, instead of searching the whole table to find the rows of each group.
-   Then it iterates through the rows of each group and calculates the aggregates for the provided columns.
-   If a group satisfies the `HAVING` clause, it's return attribute is added to the resultant table.

## ASSUMPTIONS

1. $1 \leq rows \leq 1000$
2. $1 \leq cols \leq 50$
3. All elements of the table are integers, lying in the range $[1, 1000]$
4. For `JOIN` and `GROUP BY`, `NOT EQUAL` is not a valid predicate.

## LEARNINGS

1. Understanding how to use lambda functions and custom comparators in C++.
2. Understanding the complexities of external sorting and join algorithms.

## CONTRIBUTIONS

1. Souvik Karfa - SORT, GROUP BY
2. Anish Mathur - JOIN, ORDER BY
3. Trisha Kaore -
