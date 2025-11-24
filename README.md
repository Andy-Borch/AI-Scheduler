# Genetic Algorithm vs. FCFS Job Scheduler Simulator

## Overview
This project implements a **Heterogeneous Cluster Job Scheduling Simulator** that compares two distinct scheduling strategies:
1.  **First-Come, First-Served (FCFS):** A standard greedy algorithm that schedules tasks on the first available valid resource.
2.  **Genetic Algorithm (GA):** An evolutionary AI agent that optimizes schedules over multiple generations to minimize **Makespan**, **Energy Consumption**, or **Wall Time** based on user inputs.

The simulator models a realistic computing environment with specialized hardware (e.g., NVIDIA A100s vs. T4s, Fast vs. Slow CPUs) and complex job dependencies (DAGs).

## Features

*   **Heterogeneous Cluster Simulation:** Models diverse hardware with specific attributes:
    *   **Speed Multipliers:** Real-world performance differences (e.g., A100 is 6x faster than baseline).
    *   **Power Consumption:** Energy tracking (Joules) for high-power GPUs vs. low-power CPUs.
*   **Optimization Modes:** The Genetic Scheduler supports distinct "personalities":
    *   `speed`: Prioritizes wall time and throughput.
    *   `energy`: Prioritizes energy efficiency (green computing).
    *   `balanced`: A compromise between speed and energy.
*   **Advanced Genetic Algorithm:**
    *   **Multi-Parent Seeding:** Injects heuristic-based schedules (Greedy Time, Greedy Energy) into the initial population to prevent cold-start issues.
    *   **Evolutionary Operators:** Uses tournament selection, crossover, and mutation to evolve better schedules.
*   **Visualization:** Generates Gantt charts and comparative metric plots (Makespan, Energy, Wait Time).

## Project Structure

*   `scheduler.py`: The main Genetic Algorithm logic (population, fitness function, evolution loop).
*   `fcfs.py`: The baseline FCFS scheduler implementation.
*   `cluster.py`: Defines the hardware resources (Nodes, CPUs, GPUs) and their speed/power profiles.
*   `jobs.py`: Generates random workflows (DAGs) with realistic duration profiles and penalties for architecture mismatches.
*   `visualize.py`: Generates PNG plots comparing the performance of the two schedulers.

## Usage

### 1. Run the Baselines
First, run the FCFS scheduler to establish a baseline:

```bash
python fcfs.py --tasks 100 --output fcfs_results.csv
```

### 2. Run the AI Agent
Run the Genetic Algorithm. You can tweak the `--mode` to see different behaviors:

```bash
# Mode options: speed, energy, balanced
python genetic_scheduler.py --tasks 100 --mode speed --gens 100 --pop 100 --output genetic_results.csv
```

*Note: Ensure `--tasks` matches the FCFS run for a fair comparison.*

### 3. Visualize Results
Generate comparison plots (Gantt charts and Bar metrics) in the `visualizations/` directory:

```bash
python data_visualization.py
```

## How It Works

### The Cluster
The simulation uses a `Cluster` class defined in `cluster.py` that simulates:
*   **High-Performance GPUs (A100):** High speed (6.0x), High power (400W).
*   **Efficiency GPUs (T4):** Moderate speed (1.5x), Low power (70W).
*   **Standard CPUs:** Baseline speed.
*   **Legacy CPUs:** Slower (0.8x) but functional.

### The Workflow
`jobs.py` generates a Directed Acyclic Graph (DAG) of tasks.
*   **Dependencies:** Tasks must wait for parent tasks to finish.
*   **Suitability:** GPU tasks run fast on GPUs but incur massive time penalties (10x-20x) if forced onto CPUs (and vice versa).

### The Genetic Algorithm
1.  **Initialization:** Creates a population of random schedules + specific heuristic seeds (Time-optimized and Energy-optimized).
2.  **Evaluation:** Calculates a fitness score based on the selected mode (Weights for Makespan, Energy, and Wall Time).
3.  **Selection:** Uses Tournament Selection to pick the best "parents."
4.  **Crossover & Mutation:** Swaps tasks between parents and randomly reassigns nodes to explore new solutions.
5.  **Elitism:** Keeps the best schedules from the previous generation to ensure performance never degrades.

## Interpretation of Results

After running `visualize.py`, check the `visualizations/` folder:
*   **`gantt_comparison.png`**: Shows the "Tetris packing" of jobs. A tighter pack means better utilization.
*   **`makespan_comparison.png`**: Total time to complete the workflow.
*   **`energy_comparison.png`**: Total energy consumed (Joules).
*   **`wall_time_comparison.png`**: Average time a job spends in the system (Wait + Run).
