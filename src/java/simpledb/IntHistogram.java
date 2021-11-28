package simpledb;

import simpledb.Predicate.Op;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int[] countBuckets;
    private double width;
    private int min;
    private int max;
    private int ntups;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.countBuckets = new int[buckets];
        this.min = min;
        this.max = max;
        this.width = Math.ceil((double)(max - min) / buckets);
        ntups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if(v == max) {
            countBuckets[countBuckets.length - 1]++;
        } else {
            int bucketNo = (int)((v - min) / width);
            countBuckets[bucketNo]++;
        }
        ntups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        // only consider <,<= , =, >=, >
        int bucketNo = (int)((v - min) / width);
        if(v == max) {
            bucketNo = countBuckets.length - 1;
        }
        double left = min + bucketNo * width;
        double right = left + width;
        
        if(op.equals(Predicate.Op.GREATER_THAN) || op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
            // corner case
            if(v <= min) {
                return 1;
            }
            if(v > max) {
                return 0;
            }
            // (right-v)/width * h + rest of heights in the right
            int restSum = 0;
            for(int i = bucketNo + 1; i < countBuckets.length; i++) {
                restSum += countBuckets[i];
            }

            double res = (((double)(right - v)) / width * countBuckets[bucketNo] + restSum) / ntups;
            if(op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
                res += calEqual(v);
            }
            return res;
            
        } else if(op.equals(Predicate.Op.LESS_THAN) || op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
            // corner case
            if(v >= max) {
                return 1;
            }
            if(v < min) {
                return 0;
            }
            // (v-left)/width * h + rest of heights in the left
            int restSum = 0;
            for(int i = 0; i < bucketNo; i++) {
                restSum += countBuckets[i];
            }
            double res = (((double)(v - left)) / width * countBuckets[bucketNo] + restSum) / ntups;
            if(op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
                res += calEqual(v);
            }
            return res;

        } else if(op.equals(Predicate.Op.EQUALS)) {
            // corner case
            if(v > max || v < min) {
                return 0;
            }
            // (h/w)/ntups
            return ((double)countBuckets[bucketNo]) / width / ntups;
        } else if(op.equals(Predicate.Op.NOT_EQUALS)) {
            // corner case
            if(v > max || v < min) {
                return 1;
            }

            return 1 - ((double)countBuckets[bucketNo]) / width / ntups;
        }
        return -1.0;
    }

    private double calEqual(int v) {
        int bucketNo = (int)((v - min) / width);
        return ((double)countBuckets[bucketNo]) / width / ntups; 
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
