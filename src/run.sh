OP_BENCHS=("normalize" "passfilter" "fillconst" "fillmean" "resample")
E2E_BENCHS=($(seq 30000 30000 300000))

function run_opbench {
    ENGINE=$1
    OUTPUT_FILE="results/op_${ENGINE}.csv"
    rm -f $OUTPUT_FILE
    echo "Operation benchmarks on $ENGINE"
    echo "Benchmark,Time(sec)"
    for bench in "${OP_BENCHS[@]}"; do
        ./run_bench.sh 60000 $ENGINE $bench | awk '{print $2 $10}' | tee -a $OUTPUT_FILE
    done
    echo ""
}

function run_e2ebench {
    ENGINE=$1
    OUTPUT_FILE="results/e2e_${ENGINE}.csv"
    rm -f $OUTPUT_FILE
    echo "End-to-end benchmark on $ENGINE"
    echo "Data size(million events),Time(sec)"
    for dur in "${E2E_BENCHS[@]}"; do
        ./run_bench.sh $dur $ENGINE endtoend | awk '{print $6 "," $10}' | tee -a $OUTPUT_FILE
    done
    echo ""
}

run_opbench trill
run_opbench numlib
run_opbench lifestream
run_e2ebench trill
run_e2ebench numlib
run_e2ebench lifestream

./plot.sh
