#!/bin/bash

dir=`pwd`
output=${dir}/output
java=java
jarfile="target/oak-benchmarks-synchrobench-0.1.4-SNAPSHOT.jar"
javaopt="-server"

thread="01 04 08 12"

#size="16384 65536"
size="5000000"
keysize="100"
valuesize="1000"

#writes="0 50"
writes="0"

length="5000"
l="5000"
warmup="0"
snapshot="0"
writeall="0"
iterations="5"
initialsize="50000"

if [ ! -d "${output}" ]; then
  mkdir $output
else
  rm -rf ${output}/*
fi


###############################
# records all benchmark outputs
###############################

declare -A scenarios=(["get-only"]=""
                      ["ascend-only"]="-c"
                      ["descend-only"]="-c -a 100"
                      ["put-only"]="-a 0 -u 100"
                     )

#declare -A scenarios=(["put-only"]="-a 0 -u 100")

declare -A heap_limit=(["OakMap"]="8g 12g"
                       ["JavaSkipListMap"]="20g 24g"
                      )

directMemSize="12g"

duration="30000"

# Oak vs JavaSkipList
benchClassPrefix="com.oath.oak.synchrobench.maps"
benchs="OakMap JavaSkipListMap"

echo "Starting oak test `date`"

for scenario in ${!scenarios[@]}; do
  for bench in ${benchs}; do
    echo ""
    echo "Scenario: ${bench} ${scenario}"
    heapSize="${heap_limit[${bench}]}"
    for heapLimit in ${heapSize}; do
      javaopt="-server -Xmx${heapLimit} -XX:MaxDirectMemorySize=${directMemSize}"
      for write in ${writes}; do
        for t in ${thread}; do
          for i in ${size}; do
            r=`echo "2*${i}" | bc`
            out=${output}/oak-${scenario}-${bench}-xmx${heapLimit}-DirectMeM${directMemSize}-t${t}.log
            cmd="${java} ${javaopt} -jar ${jarfile} -b ${benchClassPrefix}.${bench} ${scenarios[$scenario]} -k ${keysize} -v ${valuesize} -i ${i} -r ${r} -n ${iterations} -t ${t} -d ${duration}"
            echo ${cmd}
            echo ${cmd} >> ${out}
            ${cmd} >> ${out} 2>&1
          done
        done
      done
    done
  done
done

echo "Oak test complete `date`"

