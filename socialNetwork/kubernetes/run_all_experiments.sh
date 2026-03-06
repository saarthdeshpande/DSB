#!/bin/bash
# run_all_experiments.sh — DSB / SocialNetwork
#
# Runs experiments across 16 configurations:
#   2 CPU resource configs (res_500m, res_1000m)
#   x 2 manifests (unfixed_config, fixed_config)
#   x 4 scaling policies (cpu_50, cpu_90, memory_50, memory_90)
#
# Each experiment runs for 2 hours.
#
# unfixed_config = io-threads 8 in redis configmaps (original DSB defaults)
# fixed_config   = io-threads 1 in redis configmaps (current state)
#
# After each experiment, copies results from cpu_memory_/cpu_memory_train.csv
# into the appropriate results directory, then cleans cpu_memory_/.
#
# Usage: ./run_all_experiments.sh </dev/null 2>baselines.log &

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

DURATION="2h"

RESULTS_DIR="$SCRIPT_DIR/experiment_results"
DATA_COLLECTOR="$SCRIPT_DIR/data_collector.py"
DATA_COLLECTOR_BACKUP="$SCRIPT_DIR/data_collector.py.bak"

# Redis configmap files that need io-threads toggling
REDIS_CONFIGMAPS=(
    "$SCRIPT_DIR/home-timeline-redis/configmap.yaml"
    "$SCRIPT_DIR/social-graph-redis/configmap.yaml"
    "$SCRIPT_DIR/user-timeline-redis/configmap.yaml"
)

# --- Helper Functions ---

backup_files() {
    echo ">>> Backing up original files..."

    if [ -f "$DATA_COLLECTOR_BACKUP" ]; then
        echo ">>> Found existing data_collector.py.bak, restoring it first to prevent contamination..."
        cp "$DATA_COLLECTOR_BACKUP" "$DATA_COLLECTOR"
    else
        cp "$DATA_COLLECTOR" "$DATA_COLLECTOR_BACKUP"
    fi

    if [ -f "$SCRIPT_DIR/run_experiments.sh.bak" ]; then
        echo ">>> Found existing run_experiments.sh.bak, restoring it first to prevent contamination..."
        cp "$SCRIPT_DIR/run_experiments.sh.bak" "$SCRIPT_DIR/run_experiments.sh"
    else
        cp "$SCRIPT_DIR/run_experiments.sh" "$SCRIPT_DIR/run_experiments.sh.bak"
    fi

    for cm in "${REDIS_CONFIGMAPS[@]}"; do
        if [ -f "${cm}.bak" ]; then
            echo ">>> Found existing backup for $(basename "$cm"), restoring it first to prevent contamination..."
            cp "${cm}.bak" "$cm"
        else
            cp "$cm" "${cm}.bak"
        fi
    done
}

restore_files() {
    echo ">>> Restoring original files..."
    cp "$DATA_COLLECTOR_BACKUP" "$DATA_COLLECTOR"
    cp "$SCRIPT_DIR/run_experiments.sh.bak" "$SCRIPT_DIR/run_experiments.sh"
    for cm in "${REDIS_CONFIGMAPS[@]}"; do
        cp "${cm}.bak" "$cm"
        rm -f "${cm}.bak"
    done
    rm -f "$DATA_COLLECTOR_BACKUP"
    rm -f "$SCRIPT_DIR/run_experiments.sh.bak"
}

# set_manifest <config_name>
#   config_name: "unfixed_config" or "fixed_config"
#   unfixed = io-threads 8 in all redis configmaps
#   fixed   = io-threads 1 in all redis configmaps (current state)
set_manifest() {
    local config_name="$1"

    # Restore configmaps to original (io-threads 1) first
    for cm in "${REDIS_CONFIGMAPS[@]}"; do
        cp "${cm}.bak" "$cm"
    done

    if [ "$config_name" = "unfixed_config" ]; then
        echo ">>> Using UNFIXED config (io-threads 8)"
        for cm in "${REDIS_CONFIGMAPS[@]}"; do
            sed -i 's/io-threads 1/io-threads 8/g' "$cm"
        done
    else
        echo ">>> Using FIXED config (io-threads 1)"
        # Already the default state
    fi
}

# set_cpu_threshold <threshold>
#   Replaces ALL per-service CPU targets in service_cpu_target dict with a
#   uniform threshold
set_cpu_threshold() {
    local threshold="$1"
    echo ">>> Setting all CPU targets to ${threshold}%"
    # Only replace values inside the service_cpu_target dict (lines 49-78).
    # Pattern matches lines like:  "nginx-thrift": 50,
    sed -i '49,78 s/\(": \)[0-9]\+,/\1'"${threshold}"',/' "$DATA_COLLECTOR"
    # Also handle the default fallback: service_cpu_target.get(name, 60)
    sed -i "s/service_cpu_target.get(name, 60)/service_cpu_target.get(name, ${threshold})/" "$DATA_COLLECTOR"
}

# modify_for_memory_only <threshold>
#   Modifies data_collector.py so the HPA uses memory instead of CPU
modify_for_memory_only() {
    local threshold="$1"
    echo ">>> Modifying data_collector.py for memory-only HPA at ${threshold}%"

    # In the HPA metrics block, swap "cpu" -> "memory" so the generated HPA
    # targets memory utilization instead of CPU.
    sed -i "s/\"name\": \"cpu\"/\"name\": \"memory\"/" "$DATA_COLLECTOR"
    # Replace the per-service CPU target lookup with a fixed threshold
    sed -i "s/\"averageUtilization\": cpu_target,/\"averageUtilization\": ${threshold},/" "$DATA_COLLECTOR"
}

# set_cpu_resources <req> <lim>
#   Sets CPU request and limit in DEF_all in data_collector.py
set_cpu_resources() {
    local req="$1"
    local lim="$2"
    echo ">>> Setting CPU resources: request=${req}, limit=${lim}"
    # Order matters: replace request first (1000m→req), then limit (2000m→lim)
    sed -i "s/\"cpu\": \"1000m\"/\"cpu\": \"${req}\"/" "$DATA_COLLECTOR"
    sed -i "s/\"cpu\": \"2000m\"/\"cpu\": \"${lim}\"/" "$DATA_COLLECTOR"
}

# run_single_experiment <res_label> <cpu_req> <cpu_lim> <config_name> <metric_type> <threshold>
run_single_experiment() {
    local res_label="$1"     # res_500m or res_1000m
    local cpu_req="$2"       # 500m or 1000m
    local cpu_lim="$3"       # 1000m or 2000m
    local config_name="$4"   # unfixed_config or fixed_config
    local metric_type="$5"   # cpu or memory
    local threshold="$6"     # 50 or 90

    local experiment_label="${res_label}/${config_name}/${metric_type}_${threshold}"
    local output_dir="$RESULTS_DIR/$experiment_label"

    echo ""
    echo "============================================================"
    echo "  EXPERIMENT: $experiment_label"
    echo "  Duration:   $DURATION"
    echo "============================================================"
    echo ""

    # Restore data_collector.py to original before each experiment
    cp "$DATA_COLLECTOR_BACKUP" "$DATA_COLLECTOR"

    # Apply CPU resource config (must be after restore)
    set_cpu_resources "$cpu_req" "$cpu_lim"

    # Set the right config (io-threads)
    set_manifest "$config_name"

    # Apply threshold modifications
    if [ "$metric_type" = "cpu" ]; then
        set_cpu_threshold "$threshold"
    else
        modify_for_memory_only "$threshold"
    fi

    # Run the experiment via run_experiments.sh
    echo ">>> Running run_experiments.sh..."
    bash run_experiments.sh "$DURATION"

    # Copy results
    if [ -f "cpu_memory_/cpu_memory_train.csv" ]; then
        mkdir -p "$output_dir"
        cp "cpu_memory_/cpu_memory_train.csv" "$output_dir/"
        echo ">>> Copied results to $output_dir/cpu_memory_train.csv"
    else
        echo ">>> WARNING: cpu_memory_/cpu_memory_train.csv not found!"
    fi

    # Clean up cpu_memory_/ directory
    echo ">>> Cleaning cpu_memory_/ directory..."
    rm -rf cpu_memory_/*

    echo ">>> Experiment $experiment_label complete."
}

# --- Main ---

trap restore_files EXIT

backup_files

# Create results directory structure
mkdir -p "$RESULTS_DIR"

# Ensure run_experiments.sh uses -c flag
# (backed up in backup_files, restored by trap on EXIT)
sed -i 's/^flags=.*/flags=("-c")/' run_experiments.sh

# Run all 16 experiments (2 resource configs x 2 manifests x 4 scaling policies)
for res_config in "res_500m 500m 1000m" "res_1000m 1000m 2000m"; do
    read -r res_label cpu_req cpu_lim <<< "$res_config"
    for config in unfixed_config fixed_config; do
        for metric in cpu memory; do
            for threshold in 50 90; do
                run_single_experiment "$res_label" "$cpu_req" "$cpu_lim" "$config" "$metric" "$threshold"
            done
        done
    done
done

echo ""
echo "============================================================"
echo "  ALL EXPERIMENTS COMPLETE"
echo "  Results saved in: $RESULTS_DIR/"
echo "============================================================"
echo ""
echo "Directory structure:"
find "$RESULTS_DIR" -type f | sort
