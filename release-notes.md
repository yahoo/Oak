
<hr>

## OAK 0.2 Release Notes (June 01, 2020)

 01. Keys Disaggregation: Make chunks only reference the keys, thus keys are not copied during the rebalance. Improving the rebalance and ingestion performance. 
 02. Stream Scans: Gives a "stream" view on the elements, meaning only one element can be observed at a time. It greatly improves the scan performance due to objects’ re-usage.
 03. Removing Handles: Make chunks reference to the off-heap buffers directly, with synchronization and memory managing data moved to the buffer header. Memory usage decreased by one third.
 04. Memory Management: Following removing handles, the memory management is not based on the handle lock, and headers can be fully reusable (no internal fragmentation). Better memory utilization.
 05. Descending Scan Stack Refill Optimization: Fix the bug of refilling the scan stack with the same element more than once. Keys got compared by their indexes and not values. This improves the descending scan performance.
 06. Refactoring for future OakHash: EntrySet class is introduced to encapsulate (1) the entries to integers mapping and (2) managing (allocate/read/write/delete/release) individual key-value entries. Helps in future OakHash coding.
 07. No internal objects creation: Eliminate ephemeral objects creation in time of lookup. It greatly reduces the work to be done by GC and improves performance.
 08. Safe/Unsafe API: Create a possibility to work with OakBuffers in a fully safe/protected mode, so no wrong memory can be accessed/written erroneously. Safe mode requires some performance degradation, thus an alternative unsafe mode is introduced, providing greater performance and relying on the correct user actions for the security.
 09. Improving benchmarks code: Reducing the time it takes to run all benchmarks. Improving the results readability. Improving the benchmark scripts user experience.
 10. Correctness bug fixes (partial list): in non-zero-copy API, internal memory access, rebalance process.
 11. Small performance enhancements (partial list): Byte order, stack anchor movement, thread local elimination, thread index calculator improvement.

<hr>

## OAK 0.1.5 Release Notes (May 14, 2019)

 01. Accomplish all ConcurrentNavigableMap API

