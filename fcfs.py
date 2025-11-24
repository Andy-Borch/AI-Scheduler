import argparse
import csv
from cluster import Cluster
from jobs import Workflow

class FCFSScheduler:
    def __init__(self, cluster, workflow):
        self.cluster = cluster
        self.workflow = workflow

    def run(self):
        print(f"Scheduling {len(self.workflow.tasks)} tasks...")
        
        node_free_time = {node: 0 for node in self.cluster.get_all_nodes()}
        
        task_finish_time = {}
        task_start_time = {}
        
        schedule = {} # Store the final assignment {task_id: node_id}
        
        sorted_tasks = self.topological_sort()
        
        for task in sorted_tasks:
            if not task.dependencies:
                deps_ready_time = 0
            else:
                deps_ready_time = max(task_finish_time[dep] for dep in task.dependencies)
            
            best_node = None
            earliest_start = float('inf')
            earliest_finish = float('inf')
            
            candidate_nodes = []
            for r_type in task.duration_profiles.keys():
                 nodes = [n for n in self.cluster.get_all_nodes() if self.cluster.get_node_type(n) == r_type]
                 candidate_nodes.extend(nodes)
            
            if not candidate_nodes:
                raise Exception(f"No valid nodes for task {task.name}")

            for node in candidate_nodes:
                node_ready = node_free_time[node]
                start_time = max(node_ready, deps_ready_time)
                
                r_type = self.cluster.get_node_type(node)
                base_duration = task.duration_profiles[r_type]
                speed = self.cluster.get_node_speed(node)
                duration = base_duration / speed
                
                finish_time = start_time + duration
                
                if finish_time < earliest_finish:
                    earliest_finish = finish_time
                    earliest_start = start_time
                    best_node = node
            
            schedule[task.name] = best_node
            node_free_time[best_node] = earliest_finish
            task_finish_time[task.name] = earliest_finish
            task_start_time[task.name] = earliest_start

        return schedule, task_finish_time, task_start_time

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
            if not progress: raise Exception("Circular dependency!")
        return sorted_tasks

    def save_results_to_csv(self, schedule, filename):
        node_free_time = {node: 0 for node in self.cluster.get_all_nodes()}
        task_finish_time = {}
        task_start_time = {}
        
        sorted_tasks = self.topological_sort()
        csv_rows = []
        
        total_energy = 0
        
        for task in sorted_tasks:
            assigned_node = schedule[task.name]
            node_type = self.cluster.get_node_type(assigned_node)
            
            base_duration = task.duration_profiles[node_type]
            speed = self.cluster.get_node_speed(assigned_node)
            duration = base_duration / speed
            
            power = self.cluster.get_power_consumption(assigned_node)
            
            preferred_type = min(task.duration_profiles, key=task.duration_profiles.get)
            is_fallback = (node_type != preferred_type)
            
            machine_ready = node_free_time[assigned_node]
            if not task.dependencies:
                deps_ready = 0
            else:
                deps_ready = max(task_finish_time[dep] for dep in task.dependencies)
                
            start_time = max(machine_ready, deps_ready)
            finish_time = start_time + duration
            
            node_free_time[assigned_node] = finish_time
            task_finish_time[task.name] = finish_time
            task_start_time[task.name] = start_time
            
            energy = duration * power
            total_energy += energy

            csv_rows.append({
                'Job ID': task.name,
                'Assigned Node': assigned_node,
                'Preferred Resource': preferred_type.upper(),
                'Assigned Resource': node_type.upper(),
                'Fallback Occurred': "YES" if is_fallback else "No",
                'Start Time (s)': start_time,
                'Finish Time (s)': finish_time,
                'Wait Time (s)': start_time,
                'Runtime (s)': duration,
                'Walltime (s)': finish_time,
                'Energy (J)': energy,
                'Dependencies': ";".join(task.dependencies) if task.dependencies else "None"
            })
            
        # Write CSV
        if csv_rows:
            fieldnames = csv_rows[0].keys()
            with open(filename, mode='w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(csv_rows)
            print(f"\nDetailed schedule written to: {filename}")

        # Print Summary
        first_start = min(task_start_time.values())
        last_finish = max(task_finish_time.values())
        total_duration = last_finish - first_start
        
        wait_times = list(task_start_time.values())
        wall_times = list(task_finish_time.values())
        
        fallback_count = sum(1 for row in csv_rows if row['Fallback Occurred'] == 'YES')
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
    parser = argparse.ArgumentParser(description="Run FCFS Scheduler")
    parser.add_argument("--tasks", type=int, default=20, help="Number of tasks")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--output", type=str, default="fcfs_results.csv", help="Output CSV file")
    
    args = parser.parse_args()
    
    c = Cluster()
    w = Workflow()
    w.generate_random_workflow(num_tasks=args.tasks, seed=args.seed)
    
    scheduler = FCFSScheduler(c, w)
    scheduler.run()
    
    final_schedule, _, _ = scheduler.run() 
    scheduler.save_results_to_csv(final_schedule, args.output)