package simpledb;

import java.util.Vector;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    int buckets;
    int min;
    int max;
    int width;
    int[] vs;
    int total;

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
    // [, )
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        max++;
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.width = (max - min + (buckets - 1)) / buckets;
        this.vs = new int[buckets];
        this.total = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        int index = (v - min) / width;
        vs[index]+=1;
        total++;
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
        int index = (v - min) / width;

        // System.out.println("v = " + v);
        // System.out.println("min = " + min);
        // System.out.println("width = " + width);

        switch (op) {
        case EQUALS:
            if (v < min || v >= max) return 0;
            return vs[index] / (double)width / (double)total;
        case NOT_EQUALS:
            if (v < min || v >= max) return 1;
            return (width - vs[index]) / (double)width / (double)total;
        case GREATER_THAN:
            if (v >= max) return 0;
            if (v < min) return 1;
            int b_right = (1 + index) * width + min;
            double onlyB = (vs[index] / (double)total) * ((b_right - v - 1) / (double)width);
            for (int i=index+1;i<buckets;i++) {
                onlyB += (vs[i] / (double)total);
            }
            return onlyB;
        case GREATER_THAN_OR_EQ:
            if (v >= max) return 0;
            if (v < min) return 1;
            b_right = (1 + index) * width + min;
            onlyB = (vs[index] / (double)total) * ((b_right - v) / (double)width);
            for (int i=index+1;i<buckets;i++) {
                onlyB += (vs[i] / (double)total);
            }
            return onlyB;
        case LESS_THAN:
            // !!!Notice: left is not euqal to right, because interval is [, )
            if (v < min) return 0;
            if (v >= max) return 1;
            int b_left = index * width + min;
            onlyB = (vs[index] / (double)total) * ((v - b_left) / (double)width);
            for (int i=0;i<index;i++) {
                onlyB += (vs[i] / (double)total);
            }
            return onlyB;
        case LESS_THAN_OR_EQ:
            if (v < min) return 0;
            if (v >= max) return 1;
            b_left = index * width + min;
            onlyB = (vs[index] / (double)total) * ((v - b_left + 1) / (double)width);
            for (int i=0;i<index;i++) {
                onlyB += (vs[i] / (double)total);
            }
            return onlyB;
        default:
            break;
        }

        return -1.0;
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
        StringBuffer sb = new StringBuffer();
        for (int i=0;i<buckets;i++) {
            sb.append("\t");
            sb.append(vs[i]);
        }
        return sb.toString();
    }
}
