#!/usr/bin/env bash

# Due to the use of "declare -A", this script only support bash 4 and above.
# To run this script on mac, first install gnu-getopt: `brew install gnu-getopt`

# trap ctrl-c and call ctrl_c()
trap ctrl_c INT
CONTINUE=1

function ctrl_c() {
  echo "#### User enter CTRL-C"
  CONTINUE=0
}

############################################################################
# All test scenarios and benchmarks
############################################################################
scenarios=(
  "4a-put"
  "4b-putIfAbsentComputeIfPresent"
  "4c-get-zc"
  "4c-get-copy"
  "4d-95Get5Put"
  "4e-entrySet-ascend"
  "4e-entryStreamSet-ascend"
  "4f-entrySet-descend"
  "4f-entryStreamSet-descend"
  "not-random-put"
  "50Pu50Delete"
  "25Put25Delete50Get"
  "05Put05Delete90Get"
  "50Pu50Delete_ZC"
  "25Put25Delete90Get_ZC"
  "05Put05Delete90Get_ZC"
)

declare -A data=(
  ["buffer"]="com.yahoo.oak.synchrobench.data.buffer"
  ["eventcache"]="com.yahoo.oak.synchrobench.data.eventcache"
)

declare -A benchmarks=(
  ["skip-list"]="com.yahoo.oak.JavaSkipListMap"
  ["oak"]="com.yahoo.oak.OakBenchMap"
  ["offheap-list"]="com.yahoo.oak.OffHeapList"
  ["concurrent-hash-map"]="com.yahoo.oak.JavaHashMap"
  ["oak-hash"]="com.yahoo.oak.OakBenchHash"
  ["chronicle"]="com.yahoo.oak.Chronicle"
  ["memcached"]="com.yahoo.oak.Memcached"
)

declare -A heap_limit=(
  ["oak"]="12g"
  ["offheap-list"]="12g"
  ["skip-list"]="24g"
  ["concurrent-hash-map"]="28g"
  ["oak-hash"]="24g"
  ["chronicle"]="24g"
  # Memcached doesn't use the heap
  ["memcached"]="512m"
)

declare -A direct_limit=(
  ["oak"]="24g"
  ["offheap-list"]="24g"
  # when running CSLM/CHM some off-heap memory is still required to unrelated java.util.zip.ZipFile
  ["skip-list"]="1m"
  ["concurrent-hash-map"]="1m"
  ["oak-hash"]="24g"
  ["chronicle"]="24g"
  ["memcached"]="1m"
)

declare -A gc_cmd_args=(
  ["default"]=""
  ["parallel"]="-XX:+UseParallelOldGC"
  ["concurrent"]="-XX:+UseConcMarkSweepGC"
  ["g1"]="-XX:+UseG1GC"
)

# The JDK includes two flavors of the VM -- a client-side offering, and a VM tuned for server applications.
# These two solutions share the Java HotSpot runtime environment code base, but use different compilers that are suited
# to the distinctly unique performance characteristics of clients and servers.
# These differences include the compilation inlining policy and heap defaults.
# See the following link for more information: https://www.oracle.com/technetwork/java/whitepaper-135217.html
declare -A java_modes=(
  # JVM is launched in client mode by default in SUN/Oracle JDK.
  ["default"]=""

  # The Server VM has been specially tuned to maximize peak operating speed. It is intended for executing long-running
  # server applications, which need the fastest possible operating speed more than a fast start-up time or smaller
  # runtime memory footprint.
  ["server"]="-server"

  # The Java HotSpot Client VM has been specially tuned to reduce application start-up time and memory footprint, making
  # it particularly well suited for client environments. In general, the client system is better for GUIs.
  ["client"]="-client"
)

############################################################################
# Default arguments
############################################################################
java=java

# Default output is <pwd>/output.
output=$(pwd)/output

# Automatically picks the correct synchrobench JAR file
jar_file_path=$(find "$(pwd)" -name "oak-benchmarks-synchrobench-*.jar" | grep -v "javadoc" | xargs ls -1t | head -1)
# Pipes breakdown:
#  1. find the synchrobench JAR file (show full path)
#  2. exclude the JAR with the javadoc
#  3. sort by modification date (most recent first)
#  4. take the first result (the most recent JAR file)

# Iterate on the cartesian product of these arguments (space separated)
test_scenarios=${scenarios[*]}
test_benchmarks=${!benchmarks[*]}
test_thread="01 04 08 12 16 20 24"
test_size="10_000_000"
test_gc="default"
test_java_modes="server"
test_heap_limit=""
test_direct_limit=""

key_class="buffer"
value_class="buffer"

# Defines the key size
key_size="100"

# Defines the value size
value_size="1000"

# Defines the number of warm-up (not measured) iterations.
warmup="0"

# Defines the number of measured iterations.
iterations="5"

# Defines the test runtime in seconds.
duration="30"

# Defines the sampling range for queries and insertions.
range_ratio="2"

# Defines the number of threads used for initialization
fill_threads="24"

# For flag arguments
flag_arguments=""

# This flag is used to debug the script before a long execution.
# If set to "1" (via "-v" flag in the command line), the script will produces all the output files (with all the runtime
# parameters), but will not run the benchmark command.
# Usage: when modifying the script for a specific test and running it for a long period (e.g., overnight),
# it is recommended to verify that all the parameters are as intended, and that the scripts will not fail for some
# reason after a few iterations only to be discovered in the morning.
verify_script=0


############################################################################
# Override default arguments
############################################################################
# Use gnu-opt if exits (workaround for running on mac)
get_opt="/usr/local/opt/gnu-getopt/bin/getopt"
if [ ! -f "$get_opt" ]; then
    get_opt="getopt"
fi

ARGS=$(${get_opt}\
        -o o:j:d:i:w:s:t:e:h:b:g:m:l:r:v\
        --long verify,consume-keys,consume-values,latency\
        --long output-path:,java-path:,duration:,iterations:,warmup:,heap-limit:,direct-limit:\
        --long range-ratio:,size:,threads:,scenario:,benchmark:\
        --long gc:,java-mode:,key:,value:,key-size:,value-size:,fill-threads:\
        -s sh -n 'run' -- "$@"
)
if [ $? != 0 ]; then
    echo "Cannot parse args. Terminating..." >&2;
    exit 1;
fi

eval set -- "$ARGS"

while true; do
  case "$1" in
  -v | --verify ) verify_script=1; shift ;;
  -o | --output-path ) output="$2"; shift 2 ;;
  -j | --java-path ) java="$2"; shift 2 ;;
  -d | --duration ) duration="$2"; shift 2 ;;
  -i | --iterations ) iterations="$2"; shift 2 ;;
  -w | --warmup ) warmup="$2"; shift 2 ;;
  -h | --heap-limit ) test_heap_limit="$2"; shift 2 ;;
  -l | --direct-limit) test_direct_limit="$2"; shift 2 ;;
  -r | --range-ratio ) range_ratio="$2"; shift 2 ;;
  -s | --size ) test_size="$2"; shift 2 ;;
  -t | --threads ) test_thread="$2"; shift 2 ;;
  -e | --scenario ) test_scenarios="$2"; shift 2 ;;
  -b | --benchmark ) test_benchmarks="$2"; shift 2 ;;
  -g | --gc ) test_gc="$2"; shift 2 ;;
  -m | --java-mode ) test_java_modes="$2"; shift 2 ;;
  --key ) key_class="$2"; shift 2 ;;
  --value ) value_class="$2"; shift 2 ;;
  --key-size ) key_size="$2"; shift 2 ;;
  --value-size ) value_size="$2"; shift 2 ;;
  --fill-threads ) fill_threads="$2"; shift 2 ;;
  --consume-keys ) flag_arguments="$flag_arguments --consume-keys"; shift ;;
  --consume-values ) flag_arguments="$flag_arguments --consume-values"; shift ;;
  --latency ) flag_arguments="$flag_arguments --latency"; shift ;;
  -- ) shift; break ;;
  * ) break ;;
  esac
done

############################################################################
# Changing working directory to the JAR file directory
############################################################################
synchrobench_path=$(dirname "${jar_file_path}")
jar_file_name=$(basename "${jar_file_path}")
echo "Found synchrobench JAR in: ${synchrobench_path}"
echo "Using JAR: ${jar_file_name}"
cd "${synchrobench_path}" || exit 1

timestamp=$(date '+%d-%m-%Y--%H-%M-%S')
summary="${output}/summary-${timestamp}.csv"

echo "Starting oak test: $(date)"

# Iterate over a cartesian product of the arguments
for scenario in ${test_scenarios[*]}; do for bench in ${test_benchmarks[*]}; do
  echo ""
  echo "Scenario: ${bench} ${scenario}"

  # Write blank line between scenarios (only if summary file was already initiated)
  if [ -f "${summary}" ]; then
    echo "" >>"${summary}"
  fi

  classPath="${benchmarks[${bench}]}"

  # Use input heap/direct limit if set. Otherwise, use default for current benchmark class.
  if [ -z "$test_heap_limit" ]
  then
    scenario_test_heap_limit="${heap_limit[${bench}]}"
  else
    scenario_test_heap_limit="$test_heap_limit"
  fi

  if [ -z "$test_direct_limit" ]
  then
    scenario_test_direct_limit="${direct_limit[${bench}]}"
  else
    scenario_test_direct_limit="$test_direct_limit"
  fi

  for heap_size in ${scenario_test_heap_limit[*]}; do for direct_size in ${scenario_test_direct_limit[*]}; do
    for java_mode in ${test_java_modes[*]}; do for gc_alg in ${test_gc[*]}; do
      for threads in ${test_thread[*]}; do for size in ${test_size[*]}; do
        # Check if the user hit CTRL+C before we start a new iteration
        if [[ "$CONTINUE" -ne 1 ]]; then
          echo "#### Quiting..."
          exit 1
        fi

        gc_args=${gc_cmd_args[${gc_alg}]}
        java_args="${java_modes[${java_mode}]} -Xmx${heap_size} -XX:MaxDirectMemorySize=${direct_size} ${gc_args}"

        # Allow using separator for user input
        size=${size//[_,]/}
        # Set the range to a factor of the size of the data
        range=$((range_ratio * size))

        # Add a timestamp prefix to the log file.
        # This allows repeating the benchmark with the same parameters in the future without removing the old log.
        timestamp=$(date '+%d-%m-%Y--%H-%M-%S')
        log_filename=${timestamp}-${scenario}-${bench}-xmx${heap_size}-direct${direct_size}-t${threads}-m${java_mode}-gc${gc_alg}.log
        out=${output}/${log_filename}

        # Construct the command line as a multi-lined list for aesthetics reasons
        cmd_args=(
          "${java} ${java_args} -jar ${jar_file_name} -b ${classPath} --scenario ${scenario}"
          "--key ${data[${key_class}]} --value ${data[${value_class}]}"
          "-k ${key_size} -v ${value_size} -i ${size} -r ${range} -t ${threads}"
          "-W ${warmup} -n ${iterations} -d $((duration * 1000)) ${flag_arguments}"
          "--fill-threads ${fill_threads}"
        )
        cmd=${cmd_args[*]}
        echo "${cmd}"

        # Create the output folder and summary file only if we run at least one test
        mkdir -p "${output}"
        if [ ! -f "${summary}" ]; then
            echo "timestamp,log_file,scenario,bench,heap_limit,direct_limit,threads,gc_alg,final_size,throughput,std" >"${summary}"
        fi

        # Print all arguments to the log file
        {
          echo "[Arguments]"
          # General arguments:
          echo "timestamp: ${timestamp}"
          echo "log_filename: ${log_filename}"
          echo "synchrobench_path: ${synchrobench_path}"
          # Iteration arguments:
          echo "scenario: ${scenario}"
          echo "bench: ${bench}"
          echo "heap_limit: ${heap_size}"
          echo "direct_limit: ${direct_size}"
          echo "gc_alg: ${gc_alg}"
          echo "gc_args: ${gc_args}"
          echo "java_mode: ${java_mode}"
          # CMD arguments:
          echo "cmd: ${cmd}"
          echo "java: ${java}"
          echo "java_args: ${java_args}"
          echo "jar_file_name: ${jar_file_name}"
          echo "classPath: ${classPath}"
          echo "scenario: ${scenario}"
          echo "key_class": ${key_class}
          echo "value_class": ${value_class}
          echo "key_size: ${key_size}"
          echo "value_size: ${value_size}"
          echo "warmup: ${warmup}"
          echo "iterations: ${iterations}"
          echo "size: ${size}"
          echo "range: ${range}"
          echo "range_ratio: ${range_ratio}"
          echo "threads: ${threads}"
          echo "fill_threads: ${fill_threads}"
          echo "duration: ${duration}"
          echo "flag_arguments: ${flag_arguments}"
          echo ""
          # The benchmark output will be appended here
          echo "[Output]"
        } >"${out}"

        if [[ "$verify_script" -ne 1 ]]; then
          ${cmd} >>"${out}" 2>&1

          # Read statistics from the output log
          finalSize=$(grep "Mean Total Size:" "${out}" | cut -d : -f2 | tr -d '[:space:]')
          throughput=$(grep "Mean:" "${out}" | cut -d : -f2 | tr -d '[:space:]')
          std=$(grep "Standard deviation pop:" "${out}" | cut -d : -f2 | tr -d '[:space:]')
        fi

        # Update summary
        summary_line=("${timestamp}" "${log_filename}" "${scenario}" "${bench}" "${heap_size}" "${direct_size}"
          "${threads}" "${gc_alg}" "${finalSize}" "${throughput}" "${std}")
        (
          # Define the separator to be a comma instead of a space
          # This only have effect in the context of these parenthesis
          IFS=,
          echo "${summary_line[*]}"
        ) >>"${summary}"
      done; done
    done; done
  done; done
done; done

echo "Oak test complete $(date)"
