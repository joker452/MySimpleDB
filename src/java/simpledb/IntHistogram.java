package simpledb;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    private final int[] histBuckets;
    private final int min;
    private final int max;
    private final double histRange;
    private int numVal;

    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        histBuckets = new int[buckets];
        for (int i = 0; i < buckets; ++i)
            histBuckets[i] = 0;
        this.min = min;
        this.max = max;
        histRange = (max - min + 1) / (double) buckets;
        numVal = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        int index = (int) ((v - min + 1) / histRange);
        if (index * histRange == (double) (v - min + 1))
            --index;
        ++histBuckets[index];
        ++numVal;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        // some code goes here
        int partNum = 0;
        // if v < min or v > max, we can simply return the selectivity
        if (v < min) {
            if (op == Predicate.Op.GREATER_THAN_OR_EQ || op == Predicate.Op.GREATER_THAN
                    || op == Predicate.Op.NOT_EQUALS)
                return 1.0;
            else
                return 0.0;
        }
        if (v > max) {
            if (op == Predicate.Op.LESS_THAN_OR_EQ || op == Predicate.Op.LESS_THAN
                    || op == Predicate.Op.NOT_EQUALS)
                return 1.0;
            else
                return 0.0;
        }
        int index = (int) ((v - min + 1) / histRange);
        if (index * histRange == (double) (v - min + 1))
            --index;
        int rangeLeft = (int) (min + index * histRange);
        int rangeRight = (histRange < 1.0) ? rangeLeft: (int) (rangeLeft + histRange - 1);


        double histrange = (histRange < 1.0)? 1.0: histRange;
        if (op == Predicate.Op.EQUALS)
            return histBuckets[index] / (numVal * histrange);
        else if (op == Predicate.Op.NOT_EQUALS)
            return 1 - histBuckets[index] / (numVal * histrange);
        else if (op == Predicate.Op.GREATER_THAN) {
            double fraction = (double) (rangeRight - v) / (rangeRight - rangeLeft + 1);
            for (int i = index + 1; i < histBuckets.length; ++i)
                partNum += histBuckets[i];
            return (partNum + fraction * histBuckets[index]) / numVal;
        }
        else if (op == Predicate.Op.LESS_THAN) {
            double fraction = (double) (rangeRight - v) / (rangeRight - rangeLeft + 1);
            for (int i = 0; i < index; ++i)
                partNum += histBuckets[i];
            return (partNum + fraction * histBuckets[index]) / numVal;
        }
        else if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
            double fraction = (double) (rangeRight - v + 1) / (rangeRight - rangeLeft + 1);
            for (int i = index + 1; i < histBuckets.length; ++i)
                partNum += histBuckets[i];
            return (partNum + fraction * histBuckets[index]) / numVal;
        }
        else {
            double fraction = (double) (rangeRight - v + 1) / (rangeRight - rangeLeft + 1);
            for (int i = 0; i < index; ++i)
                partNum += histBuckets[i];
            return (partNum + fraction * histBuckets[index]) / numVal;
        }
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity(Predicate.Op op) {
        // some code goes here
        double avg = 0.0;
        for (int i = min; i <= max; ++i)
        {
            avg += estimateSelectivity(op, i);
        }
        return avg / (max - min + 1);
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        StringBuilder histDescription = new StringBuilder();
        histDescription.append("IntHistogram with value in [" + min + ", " + max + "] and " +
                histBuckets.length + "buckets\n");
        for (int i = 0; i < histBuckets.length; ++i) {
            int rangeLeft = (int) (min + i * histRange);
            int rangeRight = (histRange < 1.0) ? rangeLeft: (int) (rangeLeft + histRange - 1);
            histDescription.append("[" + rangeLeft + ", " + rangeRight + "]:" + histBuckets[i] + "\n");
        }
        return histDescription.toString();
    }
}
