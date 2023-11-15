# DATA SYSTEMS
## PROJECT PHASE 1
<p align = 'right'>
Souvik Karfa (2020102051)</br>
Trisha Kaore (2020111021)</br>
Anish Mathur (2020102044)</br>
</p>

### 1. Page Design: </br>
The constraints given to us were:
- The input matrices would only be square matrices.
- The size of the matrices (N $\le$ $10^{4}$)

However we know from ```(BLOCK_SIZE * 1000) / (sizeof(int))``` that we can only store 250 numbers in a page, which can be stored in any form (for example square or rectangular matrix).

This means at maximum we can store a 15 x 15 matrix in a single page.

Hence in our page design we have decided to break down the matrix into smaller square matrices of size M x M, where 0 $\le$ M $\le$ 15.

### 2. Functions implemented: </br>

1. **LOAD MATRIX M**: This function loads a .csv file into the memory and stores the matrix in the M x M submatrices in the `temp` folder inside the `data` folder. The `blockifyMatrix` function in `table.cpp` is where the matrix is read row by row and the `appendRowToPage` writes to `subMatrixSize` number of pages at once.

2. **TRANSPOSE MATRIX M**: The transpose function runs `table->blockCount` number of times, and for each pageIndex, calculates the appropriate submatrix whose transpose must be taken in order to transpose the whole matrix. For example, imagine a matrix of size 20 x 20, which is broken into the following submatrices:


    | $M0_{15X15}$  | $M1_{15X5}$  |
    | ------------- | ------------- |
    | $M2_{5X15}$  | $M3_{5X5}$  |

For all the submatrices along the diagonal we take them and read them using the `cursor.getNext()` function and read their values and swap them along the diagonal to write them back using the `bufferManager.writePage()` function to update the submatrix.

For the submatrices not along the diagonal we find the submatrix which will be transposed to replace the original submatrix, read and store the values in two 2D matrices and after swapping the values both are written back to the pages similarly.

Hence in this function at a time at maximum only 2 blocks are being accessed and read from and written to.

3. **PRINT MATRIX M**: In this function we print the matrix by not traversing a single page however reading a single row from all of the `subMatrixSize` rows one at a time. This is done by the `getMatrixNext` function in `table.cpp` and the `getNextPointer` function fetches the next row pointer to read from (same in case of the row being accessed is not the last in pages, else 0).

The `printMatrix` function also checks if the matrix has more than 20 rows and columns and if yes than only prints the first 20 rows and columns.

4. **EXPORT MATRIX M**: This function is similar in implementation to the `printMatrix` function except instead of writing to the terminal we write to another location which is the location of the .csv file in the `../data/temp` folder.

5. **RENAME MATRIX M NEW_M**: In this function we need to rename a matrix which mainly has to be done at 2 places. In the `tableCatalogue` which is done by the `RenameTable` function implemented in the `RenameMatrix` function in `table.cpp` which traverses through all the pages stored in the pool and individually changes the name to the string given as input.

6. **CHECKSYMMETRY M**: In this function, similar to the transpose function we read the block(s) as required to check if they are symmetric. If at any point the values did not match we return a `false`, else return `true`.

7. **COMPUTE M**: The compute function is also similar in implementation to the transpose function. How we calculated the transpose we read the block(s), depending on whether we are on the diagonal submatrices or not, and then compute the matrix `A_RESULT` in the following fashion:
```
    for (int r = 0; r < m; r++)
    {
        for (int c = 0; c < n; c++)
        {
            mat1[r][c] -= mat2[c][r];
            mat2[c][r] = -1 * mat1[r][c];
        }
    }
```
Then we simply use the `writePage` function in `bufferManager.cpp` to store the resultant matrix.

8. **BLOCKS COUNT**: Two global variables `blocksRead` and `blocksWritten` were added to `global.h`. `blocksRead` is incremneted in `Page()`, whenever a new page is read. `blocksWritten` is incremented in `writePage()` and `appendRowToPage()`, whenever a page is written into. These values are printed in the `main()` function.

### LEARNINGS:
1. There were multiple learnings to take away from this phase, such as how to go through large codebases to understand the codeflow of the project.

2. Understanding how to design, allocate and modify blocks/pages at once.

### CONTRIBUTIONS:
1. Souvik Karfa - Load Matrix, CheckSymmetry, Print Matrix, and Export.
2. Anish Mathur - Transpose Matrix, Report, Compute, Rename
3. Trisha Kaore - 
