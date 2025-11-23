import argparse
import random
import csv
from cluster import Cluster
from jobs import Workflow

class FCFSScheduler:
    def __init__(self, cluster, workflow):
        self.cluster = cluster
        self.workflow = workflow

    def run(self, output_file="schedule_results.csv"):
        sorted_tasks = self.topological_sort()
        
        node_free_time = {node: 0 for node in self.cluster.get_all_nodes()}
        task_finish_time = {}
        task_start_time = {}
        schedule_map = {}
        
        csv_rows = []

        print(f"Scheduling {len(sorted_tasks)} tasks...")

        for task in sorted_tasks:
            best_node = None
            best_finish_time = float('inf')
            best_start_time = float('inf')
            selected_duration = 0

            if not task.dependencies:
                deps_ready = 0
            else:
                deps_ready = max(task_finish_time[dep] for dep in task.dependencies)

            # --- 1. Determine "Preferred" Resource ---
            # We assume the "Preferred" option is the one with the shortest duration.
            # (In a real scenario, preference might be complex, but speed is a safe default).
            preferred_type = min(task.duration_profiles, key=task.duration_profiles.get)
            preferred_duration = task.duration_profiles[preferred_type]

            # --- 2. Find Best Execution Slot ---
            valid_options_found = False
            for r_type, duration in task.duration_profiles.items():
                valid_nodes = [
                    n for n in self.cluster.get_all_nodes() 
                    if self.cluster.get_node_type(n) == r_type
                ]
                
                for node in valid_nodes:
                    valid_options_found = True
                    machine_ready = node_free_time[node]
                    potential_start = max(machine_ready, deps_ready)
                    potential_finish = potential_start + duration
                    
                    if potential_finish < best_finish_time:
                        best_finish_time = potential_finish
                        best_start_time = potential_start
                        best_node = node
                        selected_duration = duration
            
            if not valid_options_found:
                print(f"ERROR: No valid nodes found for task {task.name}")
                continue

            # --- 3. Assign & Update State ---
            schedule_map[task.name] = best_node
            node_free_time[best_node] = best_finish_time
            task_finish_time[task.name] = best_finish_time
            task_start_time[task.name] = best_start_time
            
            # --- 4. Calculate Metadata ---
            assigned_type = self.cluster.get_node_type(best_node)
            power = self.cluster.get_power_consumption(best_node)
            energy = selected_duration * power
            
            # Did we fallback? 
            # Yes, if the assigned type is NOT the preferred type.
            # (e.g. Wanted GPU, got CPU)
            is_fallback = (assigned_type != preferred_type)

            csv_rows.append({
                'Job ID': task.name,
                'Assigned Node': best_node,
                'Preferred Resource': preferred_type.upper(),
                'Assigned Resource': assigned_type.upper(),
                'Fallback Occurred': "YES" if is_fallback else "No",
                'Start Time (s)': best_start_time,
                'Finish Time (s)': best_finish_time,
                'Wait Time (s)': best_start_time,
                'Runtime (s)': selected_duration,
                'Walltime (s)': best_finish_time,
                'Energy (J)': energy,
                'Dependencies': ";".join(task.dependencies) if task.dependencies else "None"
            })

        self.write_csv(csv_rows, output_file)
        self.print_results(task_start_time, task_finish_time, schedule_map)

    def write_csv(self, rows, filename):
        if not rows: return
        fieldnames = rows[0].keys()
        try:
            with open(filename, mode='w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            print(f"\nDetailed schedule written to: {filename}")
        except IOError as e:
            print(f"Error writing to CSV: {e}")

    def topological_sort(self):
        sorted_tasks = []
        processed_ids = set()
        pending_tasks = list(self.workflow.tasks)
        while pending_tasks:
            progress = False
            for task in pending_tasks[:]:
                if all(dep in processed_ids for dep in task.dependencies):
                    sorted_tasks.append(task)
                    processed_ids.add(task.name)
                    pending_tasks.remove(task)
                    progress = True
            if not progress: raise Exception("Circular dependency detected!")
        return sorted_tasks

    def print_results(self, task_start_time, task_finish_time, schedule_map):
        first_start = min(task_start_time.values())
        last_finish = max(task_finish_time.values())
        total_duration = last_finish - first_start
        total_energy = 0
        wait_times = []
        wall_times = []
        
        fallback_count = 0
        
        for task_name, node in schedule_map.items():
            task = self.workflow.get_task(task_name)
            node_type = self.cluster.get_node_type(node)
            actual_duration = task.duration_profiles[node_type]
            power = self.cluster.get_power_consumption(node)
            total_energy += actual_duration * power
            
            wait_times.append(task_start_time[task_name])
            wall_times.append(task_finish_time[task_name])
            
            # Check fallback for summary stats
            preferred = min(task.duration_profiles, key=task.duration_profiles.get)
            if node_type != preferred:
                fallback_count += 1

        avg_energy = total_energy / len(self.workflow.tasks)
        avg_wait = sum(wait_times) / len(wait_times)
        avg_wall = sum(wall_times) / len(wall_times)

        print("\n--- Simulation Results (FCFS) ---")
        print(f"Tasks: {len(self.workflow.tasks)}")
        print(f"Fallbacks: {fallback_count} jobs ran on slower resources")
        print(f"1. Total Workflow Duration: {total_duration:.2f} s")
        print(f"2. Total Energy Consumed:   {total_energy:.2f} J")
        print(f"3. Avg Wait Time per Job:   {avg_wait:.2f} s")
        print(f"4. Avg Wall Time per Job:   {avg_wall:.2f} s")
        print(f"5. Avg Energy per Job:      {avg_energy:.2f} J")
        print("---------------------------------")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run FCFS Scheduler Simulation")
    parser.add_argument("--tasks", type=int, default=20, help="Number of tasks to generate")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--simple", action="store_true", help="Run the simple manual example instead of random")
    parser.add_argument("--output", type=str, default="schedule_results.csv", help="Output CSV filename")
    
    args = parser.parse_args()
    
    c = Cluster()
    w = Workflow()
    
    if args.simple:
        print("Running Simple Sample Workflow...")
        w.create_sample_workflow()
    else:
        w.generate_random_workflow(num_tasks=args.tasks, seed=args.seed)
    
    scheduler = FCFSScheduler(c, w)
    scheduler.run(output_file=args.output)