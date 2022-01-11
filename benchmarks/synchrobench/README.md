# Oak synchro-bench like benchmarks
This module is a benchmark suite to benchmark and compare
Oak and other Oak like maps.
It is inspired, and largely based on the [synchro-bench 
benchmarks suite](https://github.com/gramoli/synchrobench).

## Prerequisites

Install bash version higher than 5 and getopt.

In Mac, use:
```shell
brew reinstall bash
brew install gnu-getopt
```

Make sure to open a new terminal window after install bash.

## Adding a new map
To add a new map, the developer needs to implement 
`CompositionalMap`.

The interface can be found under the following package:
```
com.yahoo.oak.synchrobench.contention.abstraction
```

To avoid code duplication, some common fields 
and methods can be found in `BenchMap`, under:
```
com.yahoo.oak.synchrobench.maps
```
In this package, two more classes can be found:
* `BenchOakMap`: encapsulate common logic for Oak maps.
* `BenchOnHeapMap`: encapsulate common logic for on-heap (Java's) maps.

The implemented class must reside under the following package:
```
com.yahoo.oak
```

Note that the benchmark infrastructure expect all the 
constructors of the `CompositionalMap` implementations to 
accept specific parameters as explained in
the `BenchMap` class doc.

For more guidance, see the interface doc,
and the other existing map implementations.

## Adding a new data type
To add a new data type, the developer needs to implement
two classes:
* `KeyGenerator`: generates and serialize/deserialize keys
* `ValueGenerator`: generates and serialize/deserialize values

The interfaces can be found under the following package:
```
com.yahoo.oak.synchrobench.contention.abstraction
```

Each implemented data-type must reside under its own
package, as follows:
```
com.yahoo.oaksynchrobench.data.<your-data-type-name>
```
Where `<your-data-type-name>` is the package for your data. 

The package must contain two classes with the following
names:
* `KeyGen`: your implementation of `KeyGenerator`
* `ValueGen`: your implementation of `ValueGenerator`

Note that the benchmark infrastructure expect all the
constructors of the above generators implementations to
accept no parameters.

For more guidance, see the interfaces docs,
and the other existing data-type implementations.
