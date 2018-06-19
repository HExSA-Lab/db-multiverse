# db-multiverse

## Sorting

This is not a rigorous experiment, just trying to see if I am improving or regressing the software. I will list the mean and stddev of 5 iterations of "the actual sort" run-time after 1 warm-up iteration.

```
mergesort:
  - commit: a49f1cf
    description: initial naive implementation
    mean: 13.335
    std: 0.497

  - commit: 20b7263
    description: saved pointer to chunk
    mean: 7.078
    std: 0.265

  - commit: HEAD
    description: saved pointer to column data
    mean: 7.310
    std: 0.265

countingsort:
  - commit: 3d26044
    description: initial trial
    mean: 1.531
    std: 0.066

  - commit: HEAD
    description: saved pointer to column data
    mean: 1.521
    std: 0.085
```
