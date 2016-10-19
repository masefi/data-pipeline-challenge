# data-pipeline-challenge
This project applies map reduce programing framework using java to find the top 100 flight routes with highest average arrival delays.
# Getting started
* We started by implementing a single node Hadoop cluster, to develope and test our code. The fist version, was a naive version implemented using map reduce. The map function, takes each row of 
flight information and produces a key in the form "source-destination" and value is arrival delay. The reduce step sums up the arrival delays for each key and divides by number of occurance of the key to compute the average arrival delays. This does not yet extract the top 100. We used the Hadoop Unix command to sort by second column (average arrival delay) and select the first 100 and save it to a file.

* We worked on improving the algorithm to tackle possible integer problem that may occue if the size of the data is very large. One usual solution is to do calculation using data structures such long or double which have higher precision. Particularly, we will check the improvement we apply on the code to cope with integer problem should not change the result dervied from previouse simpler version of the code and hence it is numerically accurate. The approach is that we incrementally compute the cumulative moving average (CMA) of all input values for each key up to a point. The Calculation uses the formula as such:

CMA_{n+1} = CMA_{n} + (x_{n+1} - CMA_{n})/(n+1) 

where x_{n} is nth input. In the above formula, all intermediate results are on the same order of magnitude as the original values. In fact, the largest intermediate value you can get is the difference between largest and smallest input values.

* There is one more challange, the second sommand in formula above which is a division. If the division is integer division, this can result in cut off, and these can be add up to sizable amount. Therefore, we need to keep track of the cummulated remainder (CMR) of the division. The formulas get updated to followings.

CMA_{n+1} = CMA_{n} + (x_{n+1} - CMA_{n} + CMR_{n}) div (n+1)   

CMR_{n+1} = (x_{n+1} - CMA_{n} + CMR_{n}) mod (n+1)

The biggest intermediate value in this case is divisor in both formulas, i.e., (x_{n+1} - CMA_{n} + CMR_{n}). The subtraction can at most be difference between smallest and largest input x and the CMR can be any value between -n to n. Therefor, there would be no overflow, as long as the number of elements does not get close to the maximum integer value of integer type being used. For type int the largest integer is 2,147,483,647 and for type long, the largest signed long is 2^{63} - 1 and in java 8, you can specify unsigned long with maximum of 2^{64} - 1.
      
