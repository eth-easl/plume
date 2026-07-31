#!/bin/bash

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR" || exit 1

# configuration
trace_prefix="redshift_1w"
plume_bench="$HOME/plume-v2/build/plume/benchmarks/tpch/plume_bench"
bench_cfg="$HOME/plume-v2/benchmarks/tpch/config/aws_trace.json" 
out_dir="$HOME/trace_results"
max_splits="30"
while getopts 't:p:c:o:s:' flag; do
  case "${flag}" in
    t) trace_prefix="${OPTARG}" ;;
    p) plume_bench="${OPTARG}" ;;
    c) bench_cfg="${OPTARG}" ;;
    o) out_dir="${OPTARG}" ;;
    s) max_splits="${OPTARG}" ;;
    *) printf "Usage: %s [-t trace_prefix] [-p plume_bench] [-c bench_cfg] [-o out_dir] [-s max_splits]\n" $0
       exit 1 ;;
  esac
done
printf "Build configuration:\n trace_prefix=$trace_prefix\n plume_bench=$plume_bench\n bench_cfg=$bench_cfg\n out_dir=$out_dir\n max_splits=$max_splits\n"

if [ ! -f "$bench_cfg" ]; then
    echo "Error: Config file not found at $bench_cfg"
    exit 1
fi

mkdir -p "$out_dir"

results_prefix=$(sed -n 's/.*"resultsPrefix": "\([^"]*\)".*/\1/p' ${bench_cfg})
results_prefix="${results_prefix/\~/$HOME}"

for file in "${trace_prefix}"*; do
    if [ -e "$file" ]; then
        if [ -f "$file" ] && [ "$file" != "$(basename "${BASH_SOURCE[0]}")" ]; then
            # change config
            full_path="${DIR}/${file}"
            sed -i "s|\"path\": \".*\"|\"path\": \"${full_path}\"|g" "$bench_cfg"
            sed -i "s|\"maxSplits\": .*,|\"maxSplits\": ${max_splits},|g" "$bench_cfg"

            # run benchmark
            $plume_bench "${bench_cfg}"
            
            # copy results
            filename=$(basename "$file" .csv)
            suffix=${filename#trace_prefix}
            cp "${results_prefix}/trace_results.csv" "${out_dir}/${suffix}.csv"
        fi
    else
        echo "No trace files found matching prefix: '$trace_prefix'"
        exit 0
    fi
done
