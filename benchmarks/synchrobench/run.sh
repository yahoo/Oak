#!/usr/bin/env bash

dir=`pwd`
output=${dir}/output
java=java
jarfile="target/oak-benchmarks-synchrobench-0.1.6-SNAPSHOT.jar"

thread="01 04 08 12"
size="10000000"
keysize="100"
valuesize="1000"
#writes="0 50"
writes="0"
warmup="0"
iterations="5"
duration="30000"
#gcAlgorithms="-XX:+UseParallelOldGC -XX:+UseConcMarkSweepGC -XX:+UseG1GC"

declare -A heap_limit=(["OakMap"]="12g"
                       ["JavaSkipListMap"]="20g"
                      )

directMemSize="20g"

if [ ! -d "${output}" ]; then
  mkdir $output
else
  rm -rf ${output}/*
fi


###############################
# records all benchmark outputs
###############################

declare -A scenarios=(["get-only"]=""
                      ["zc-get-only"]="--buffer"
                      ["ascend-only"]="-c"
                      ["zc-ascend-only"]="--buffer"
                      ["descend-only"]="-c -a 100"
                      ["zc-descend-only"]="--buffer -c -a 100"
                      ["put-only"]="-a 0 -u 100"
                     )

#declare -A scenarios=(["put-only"]="-a 0 -u 100")
#declare -A scenarios=(["descend-only"]="-c -a 100")


# Oak vs JavaSkipList
benchClassPrefix="com.oath.oak.synchrobench.maps"
benchs="OakMap JavaSkipListMap"

summary="${output}/summary.csv"

echo "Starting oak test `date`"
echo "Scenario, Bench, Heap size, Direct Mem, # Threads, Final Size, Throughput, gc" > ${summary}

for scenario in ${!scenarios[@]}; do
  for bench in ${benchs}; do
    echo ""
    echo "Scenario: ${bench} ${scenario}"
    heapSize="${heap_limit[${bench}]}"
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


