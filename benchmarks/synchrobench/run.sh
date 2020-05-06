#!/usr/bin/env bash

dir=`pwd`

# Get the output path as the first parameter. Default is <pwd>/output.
output=${1:-${dir}/output}
java=java
jarfile="target/oak-benchmarks-synchrobench-0.1.6-SNAPSHOT.jar"

thread="01 04 08 12 16 20 24 28 32"
size="10000000"
keysize="100"
valuesize="1000"
#writes="0 50"
writes="0"
warmup="0"
iterations="5"
duration="30000"
#gcAlgorithms="-XX:+UseParallelOldGC -XX:+UseConcMarkSweepGC -XX:+UseG1GC"

declare -A heap_limit=(["OakMyBufferMap"]="12g"
                       ["OffHeapList"]="12g"
                       ["JavaSkipListMap"]="36g"
                      )

declare -A direct_limit=(["OakMyBufferMap"]="24g"
                         ["OffHeapList"]="24g"
                         ["JavaSkipListMap"]="0g"
                        )

if [ ! -d "${output}" ]; then
  mkdir $output
else
  rm -rf ${output}/*
fi


###############################
# records all benchmark outputs
###############################

declare -A scenarios=(
                      ["4a-put"]="-a 0 -u 100"
                      ["4b-putIfAbsentComputeIfPresent"]="--buffer -u 0 -s 100 -c"
                      ["4c-get-zc"]="--buffer"
                      ["4c-get-copy"]=""
                      ["4d-95Get5Put"]="--buffer -a 0 -u 5"
                      ["4e-entrySet-ascend"]="--buffer -c"
                      ["4e-entryStreamSet-ascend"]="--buffer -c --stream-iteration"
                      ["4f-entrySet-descend"]="--buffer -c -a 100"
                      ["4f-entryStreamSet-descend"]="--buffer -c -a 100 --stream-iteration"
                     )


# Oak vs JavaSkipList
benchClassPrefix="com.oath.oak"
benchs="JavaSkipListMap OakMyBufferMap OffHeapList"

summary="${output}/summary.csv"

echo "Starting oak test `date`"
echo "Scenario, Bench, Heap size, Direct Mem, # Threads, Final Size, Throughput, gc" > ${summary}

for scenario in ${!scenarios[@]}; do
  for bench in ${benchs}; do
    echo ""
    echo "Scenario: ${bench} ${scenario}"
    heapSize="${heap_limit[${bench}]}"
    directMemSize="${direct_limit[${bench}]}"
    for heapLimit in ${heapSize}; do
      #for gcAlg in ${gcAlgorithms}; do
        gcAlg=""
        javaopt="-server -Xmx${heapLimit} -XX:MaxDirectMemorySize=${directMemSize} ${gcAlg}"
        for write in ${writes}; do
          for t in ${thread}; do
            for i in ${size}; do
              r=`echo "2*${i}" | bc`
              out=${output}/oak-${scenario}-${bench}-xmx${heapLimit}-DirectMeM${directMemSize}-t${t}-${gcAlg}.log
              cmd="${java} ${javaopt} -jar ${jarfile} -b ${benchClassPrefix}.${bench} ${scenarios[$scenario]} -k ${keysize} -v ${valuesize} -i ${i} -r ${r} -n ${iterations} -t ${t} -d ${duration} -W ${warmup}"
              echo ${cmd}
              echo ${cmd} >> ${out}
              ${cmd} >> ${out} 2>&1

              # update summary
              finalSize=`grep "Mean Total Size:" ${out} | cut -d : -f2 | tr -d '[:space:]'`
              throughput=`grep "Mean:" ${out} | cut -d : -f2 | tr -d '[:space:]'`
              echo "${scenario}, ${bench}, ${heapLimit}, ${directMemSize}, ${t}, ${finalSize}, ${throughput} $gcAlg" >> ${summary}
            done
          done
        done
      #done
      echo "" >> ${summary}
    done
  done
done

echo "Oak test complete `date`"
