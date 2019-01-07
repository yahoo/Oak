#!/bin/bash

dir=..
output=${dir}/output
bin=${dir}/bin
java=java
jarfile="../oak-synchrobench.jar"
javaopt="-server -Xmx8g -XX:MaxDirectMemorySize=16g"

### javac options
# -O (dead-code erasure, constants pre-computation...)
### JVM HotSpot options
# -server (Run the JVM in server mode) 
# -Xmx1g -Xms1g (set memory size)
# -Xss2048k (Set stack size)
# -Xoptimize (Use the optimizing JIT compiler) 
# -XX:+UseBoundThreads (Bind user threads to Solaris kernel threads)
###

stms="estm estmmvcc"
syncs="oak"
#syncs="sequential lockbased lockfree transactional"
#thread="1 2 4 8 16 32 64"
thread="01 04 12 24"

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
iterations="1"
initialsize="50000"

if [ ! -d "${output}" ]; then
  mkdir $output
else
  rm -rf ${output}/*
fi


mkdir ${output}/log ${output}/data ${output}/plot ${output}/ps

###############################
# records all benchmark outputs
###############################


# Oak vs JavaSkipList
benchClassPrefix="com.oath.oak.synchrobench.maps"
benchs="OakMap JavaSkipListMap"
#declare -A scenarios=(["get-only"]=""
#                      ["ascend-only"]="-c"
#                      ["descend-only"]="-c -a 100"
#                      ["put-remove-50-50"]="-a 50 -u 100"
#                     )

#declare -A scenarios=(["get-only"]=""
#                      ["ascend-only"]="-c"
#                      ["descend-only"]="-c -a 100"
#                      ["put-only"]="-a 0 -u 100"
#                     )


declare -A scenarios=(["put-only"]="-a 0 -u 100")

echo "Starting oak test `date`"

if [[ "${syncs}" =~ "oak" ]]; then
#for bench in ${benchs}; do
for scenario in ${!scenarios[@]}; do
  for bench in ${benchs}; do
    echo ""
    echo "Scenario: ${bench} ${scenario}"
    for write in ${writes}; do
      for t in ${thread}; do
        for i in ${size}; do
          r=`echo "2*${i}" | bc`
          out=${output}/log/oak-${scenario}-${bench}-i${i}-t${t}.log
          cmd="${java} ${javaopt} -jar ${jarfile} -b ${benchClassPrefix}.${bench} ${scenarios[$scenario]} -k ${keysize} -v ${valuesize} -i ${i} -r ${r} -n ${iterations} -t ${t}"
          echo ${cmd} >> ${out}
          ${cmd} >> ${out} 2>&1
        done
      done
    done
  done
done
fi

echo "Oak test complete `date`"
