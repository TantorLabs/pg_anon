"""Row-count presets for test fixtures.

Use TINY/SMALL/MEDIUM/LARGE consciously:
- TINY: exact-row assertions, fast smoke checks
- SMALL: scanner / dict-gen correctness (default)
- MEDIUM: masks and data-regex validation, stress baseline
- LARGE: partition pruning, TOAST, parallel dump benchmarks
"""

TINY = 10
SMALL = 100
MEDIUM = 1_000
LARGE = 100_000
