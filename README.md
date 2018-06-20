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

  - commit: 4bc0628
    description: saved pointer to column data
    mean: 7.310
    std: 0.265

  - commit: HEAD
    description: >
      corrected logical error in mergesort.

      Does more copying. Note that the actual sorting time did not change much (even
      got smaller), but the time to make the out array got big. This time is not
      reported in "actual sort".

    mean: 6.889
    std: 0.214

? |-
   merge and
   countingsort
:
  - commit: HEAD
    description: initial trial
    mean: 6.345
    std: 0.206

countingsort:
  - commit: 3d26044
    description: initial trial
    mean: 1.531
    std: 0.066

  - commit: 4bc0628
    description: saved pointer to column data
    mean: 1.521
    std: 0.085
```
