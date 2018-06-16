# db-multiverse

## Sorting

This is not a rigorous experiment, just trying to see if I am improving or regressing the software. I will list the mean and stddev after 5 iterations of "the actual sort" run-time.

- a49f1cf
  - implemented merge sort naively
  - mean=13,335,555, std=497,855
- 9b6fe98
  - no relevant change
  - mean=13,242,108, std=462,601
- HEAD
  - mean=7,078,681, std=265,748, HOWEVER this excludes an initial run which took 16,991,417
