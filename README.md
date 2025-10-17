# Dyadic Explanation Tree

Compilation:

    cd <PATH_TO_DYADIC_EXPLANATION_TREE>

    sbt clean assembly

Execution:

    spark-submit [--master "local[NUM_THREADS]"] --class gal.udc.DyadicExplanationTree <PATH_TO_JAR_FILE> <DATASET> <clusterCounts> <splitAt> <OUTPUT_FILE>  [options]

## Usage
```
DyadicExplanationTree dataset clusterCounts splitAt output [options]
    Dataset must be a libsvm file
    clusterCounts points to a text file containing tuples (clusterID, clusterSize)
    splitAt indicates the index of the first non-numerical variable
    Options:
        -l max level
        -c number of candidate trees to explore at each node
```

## Reference
Eiras-Franco, C., Guijarro-Berdinas, B., Alonso-Betanzos, A., & Bahamonde, A. (2019). A scalable decision-tree-based method to explain interactions in dyadic data. Decision Support Systems, 127, 113141.
