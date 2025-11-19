flags=("-cm")

#flags=("-m -cm")

if [ $# -eq 0 ];
then
    echo "Error: No arguments provided."
    duration="10m"
else
  duration="$1"
fi

echo "Duration: $duration"

echo "Number of flags: ${#flags[@]}"
echo "Flags: ${flags[@]}"

for flag in "${flags[@]}"
do
    echo "Current flag: $flag"
    echo "Executing: python data_collector.py $flag -t $duration"
    python data_collector.py $flag -t $duration

    echo "Exit code: $?"
done

echo "Loop completed. Creating datasets."

python data_process.py -t $duration

echo "Datasets created."
