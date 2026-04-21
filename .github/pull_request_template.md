## Summary

Short description of the change.

## Type

- [ ] Bug fix
- [ ] New feature / new table variant
- [ ] Performance or measurement change
- [ ] Docs / refactor only

## Verification

- [ ] `cmake --build build -j` is clean
- [ ] `./build/correctness_test --threads=16 --repeats=2 --tables=stub,chaining_coarse,chaining_fine,cuckoo_optimistic,cuckoo_striped,hopscotch` passes (this matches CI on push/PR; the full `--repeats=10` canonical bar runs nightly and locally before any canonical number change)
- [ ] (If applicable) ThreadSanitizer build passes for lock-based tables
- [ ] (If applicable) Plots or paper numbers regenerated and committed

## Notes

Anything reviewers should know (benchmarks, trade-offs, follow-ups).
