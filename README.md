

# FUDJ: Flexible User-Defined Distributed Joins
FUDJ is a framework for complex distributed join algorithms. The key idea of FUDJ is to allow developers to realize new distributed join algorithms into the database without delving into the database internals. 

In this repository, you will find the FUDJ interface and three optimized join algorithms implementation. 

## FUDJ Examples

 1. [Spatial Join](https://github.com/akilsevim/FUDJ/blob/main/SpatialJoin.java) (Based on: Patel, J. M., & DeWitt, D. J. (1996). Partition based spatial-merge join.  _ACM Sigmod Record_,  _25_(2), 259-270. Chicago)
 2. [Interval Join](https://github.com/akilsevim/FUDJ/blob/main/IntervalJoin.java) (Based on: Dignös, A., Böhlen, M. H., & Gamper, J. (2014, June). Overlap interval partition join. In _Proceedings of the 2014 ACM SIGMOD International Conference on Management of Data_ (pp. 1459-1470).)
 3. Set-Similarity Join (Based on: Kim, T., Li, W., Behm, A., Cetindil, I., Vernica, R., Borkar, V., ... & Li, C. (2020). Similarity query support in big data management systems. _Information Systems_, _88_, 101455.)

## FUDJ Interface
To address the common challenges in partition-based distributed optimized join algorithms FUDJ programming model consists of three phases namely, **SUMMARIZE**, **PARTITION**, and **COMBINE**. 

![FUDJ Programming Model](https://www.cs.ucr.edu/~asevi006/fudj_pm.png)

### SUMMARIZE
    public interface Summary<T> extends Serializable {  
        void add(T var1);
        void add(Summary<T> var1);  
    }

The `void add(T var1);` function reads keys from the input dataset and updates a $SUMMARY$ object. Then all $SUMMARY$ objects are merged into global $SUMMARY$ objects by `void add(Summary<T> var1)` function.

Next, `C divide(Summary<T> var1, Summary<T> var2);` function creates the `Configuration` object.

### PARTITION
Each $key$ is assigined to a bucket with the following functions. `assign1()` is used for the first input dataset. If the second input dataset requires a unique handling, developers can implement `assign2()`.

    int[] assign1(T var1, C var2);  
      
    default int[] assign2(T k2, C c) {  
        return this.assign1(k2, c);  
    }
### COMBINE
Finally, the buckets are matched using the `match()` function, and the keys are verified within the matched buckets with `verify()` function.

    default boolean match(int b1, int b2) {  
        return b1 == b2;  
    }  
      
    default boolean verify(int b1, T k1, int b2, T k2, C c) {  
		if (this.verify(k1, k2)) {  
			int[] buckets1DA = this.assign1(k1, c);  
			int[] buckets2DA = this.assign2(k2, c);  
			int i = 0;  
			int j = 0;  

			while(i < buckets1DA.length && j < buckets2DA.length) {  
				if (buckets1DA[i] == buckets2DA[j]) {  
					return buckets1DA[i] == b1 && buckets2DA[j] == b2;  
				}  

				if (buckets1DA[i] > buckets2DA[j]) {  
					++j;  
				} else {  
					++i;  
				}  
			}  
		}  
		return false;  
	}  

	boolean verify(T var1, T var2);

A default duplicate handling method is provided however developers are allowed to implement their custom method or bypass it if the join algorithm does not require one.
