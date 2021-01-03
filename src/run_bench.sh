DURATION=$1
ENGINE=$2
BENCH=$3

if [ "$ENGINE" == "lifestream" ] || [ "$ENGINE" == "trill" ]; then
    dotnet run -p LifeStream -c Release $DURATION $BENCH $ENGINE
elif [ "$ENGINE" == "numlib" ]; then
    python3 numlib.py $DURATION $BENCH
else
    echo "Usage: ./run_bench.sh <data size (sec)> (trill|numlib|lifestream) [(normalize|passfilter|fillconst|fillmean|resample|endtoend)]"
    exit 1
fi
