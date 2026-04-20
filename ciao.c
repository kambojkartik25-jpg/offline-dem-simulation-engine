#include <stddef.h>

// Binary search on a sorted int array.
// Returns index of target, or -1 if not found.
int binary_search(const int *arr, size_t n, int target) {
    size_t lo = 0;
    size_t hi = n;

    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        int v = arr[mid];
        if (v == target) {
            return (int)mid;
        }
        if (v < target) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return -1;
}
