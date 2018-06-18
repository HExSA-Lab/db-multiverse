# db-multiverse

## Sorting

This is not a rigorous experiment, just trying to see if I am improving or regressing the software. I will list the mean and stddev after 5 iterations of "the actual sort" run-time.

- a49f1cf
  - implemented merge sort naively
  - mean=13.335 std=0.497
- 9b6fe98
  - no relevant change
  - mean=13.242, std=0.462
- 20b7263
  - saved pointer to chunk
  - mean=7.078, std=0.265, excluding 1 initial run
- HEAD
  - switched to countingsort
  - mean=1.531, mean=0.066, excluding 1 initial run
