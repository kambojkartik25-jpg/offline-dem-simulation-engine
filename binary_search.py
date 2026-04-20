from typing import Sequence


def binary_search(arr: Sequence[int], target: int) -> int:
    """Binary search on a sorted sequence. Returns index or -1 if not found."""
    lo = 0
    hi = len(arr)
    while lo < hi:
        mid = lo + (hi - lo) // 2
        v = arr[mid]
        if v == target:
            return mid
        if v < target:
            lo = mid + 1
        else:
            hi = mid
    return -1
