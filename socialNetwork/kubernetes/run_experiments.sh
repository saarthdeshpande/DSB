flags=("-cm")

#flags=("-m")

if [ $# -eq 0 ];
then
    echo "Error: No arguments provided."
    duration="10m"
elif [ $# -eq 1 ];
then
  duration="$1"
  rt=0
elif [ $# -eq 2 ];
then
  duration="$1"
  rt=1
fi

echo "Duration: $duration"

echo "Number of flags: ${#flags[@]}"
echo "Flags: ${flags[@]}"

for flag in "${flags[@]}"
do
    echo "Current flag: $flag"
    if [ $rt -eq 1 ];
    then
      echo "Executing: python data_collector.py $flag -t $duration --realtime"
      python data_collector.py $flag -t $duration --realtime
      echo "Loop completed. Creating datasets."
      python data_process.py -t $duration -r
    else
      echo "Executing: python data_collector.py $flag -t $duration"
      python data_collector.py $flag -t $duration
      echo "Loop completed. Creating datasets."
      python data_process.py -t $duration
    fi
    echo "Exit code: $?"
done

echo "Datasets created."

