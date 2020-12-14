DURATION=60000
BENCHMARKS=("normalize" "passfilter" "fillconst" "fillmean" "resample" "endtoend")

function run_bench {
    ENGINE=$1
    BENCH=$2

    if [ "$ENGINE" == "lifestream" ] || [ "$ENGINE" == "trill" ]; then
        dotnet run -p LifeStream -c Release $DURATION $BENCH $ENGINE
    elif [ "$ENGINE" == "numlib" ]; then
        python3 numlib.py $DURATION $BENCH
    else
        echo "Usage: ./run.sh (trill|numlib|lifestream) [(normalize|passfilter|fillconst|fillmean|resample|endtoend)]"
        exit 1
    fi
}

if [[ $# -eq 1 ]]; then
    for bench in "${BENCHMARKS[@]}"; do
        run_bench $1 $bench
    done

else
    run_bench $1 $2
fi
