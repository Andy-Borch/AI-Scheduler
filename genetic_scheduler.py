import argparse
import random
import csv
import copy
from cluster import Cluster
from jobs import Workflow

class GeneticScheduler:
    def __init__(self, cluster, workflow, population_size=100, generations=100, mode='balanced'):
        self.cluster = cluster
        self.workflow = workflow
        self.population_size = population_size
        self.generations = generations
        self.mode = mode
        self.population = [] 
        
        self.weight_profiles = {
            'balanced': {'makespan': 1.0, 'energy': 0.001, 'wall': 1.0},
            'speed':    {'makespan': 1.0, 'energy': 0.0001, 'wall': 5.0}, 
            'energy':   {'makespan': 0.01, 'energy': 0.005, 'wall': 0.01}   
        }
        
        print(f"Scheduler Mode: {mode.upper()}")
        weights = self.weight_profiles[mode]
        print(f"Weights -> Makespan: {weights['makespan']}, Energy: {weights['energy']}, Avg Wall: {weights['wall']}")

    def generate_heuristic_schedule(self, strategy='time'):
        #FCFS baseline implementation
        schedule = {}

        node_free_time = {}
        for node in self.cluster.get_all_nodes():
            node_free_time[node] = 0
        
        task_finish_time = {}
        
        sorted_tasks = self.topological_sort()
        
        for task in sorted_tasks:
            best_node = None
            best_metric = float('inf')
            
            if not task.dependencies:
                deps_ready = 0
            else:
                deps_ready = max(task_finish_time[dep] for dep in task.dependencies)

            for r_type, base_duration in task.duration_profiles.items():
                valid_nodes = []
                for n in self.cluster.get_all_nodes():
                    if self.cluster.get_node_type(n) == r_type:
                        valid_nodes.append(n)
                for node in valid_nodes:
                    speed = self.cluster.get_node_speed(node)
                    real_duration = base_duration / speed
                    
                    start = max(node_free_time[node], deps_ready)
                    finish = start + real_duration
                    
                    power = self.cluster.get_power_consumption(node)
                    energy = real_duration * power
                    
                    if strategy == 'time':
                        current_metric = finish
                    elif strategy == 'energy':
                        current_metric = energy
                    else:
                        current_metric = finish
                        
                    if current_metric < best_metric:
                        best_metric = current_metric
                        best_node = node
            
            if best_node is None:
                valid_nodes = []
                for n in self.cluster.get_all_nodes():
                    if self.cluster.get_node_type(n) in task.duration_profiles:
                        valid_nodes.append(n)
                best_node = random.choice(valid_nodes)
                finish = 0 

            schedule[task.name] = best_node
            
            r_type = self.cluster.get_node_type(best_node)
            speed = self.cluster.get_node_speed(best_node)
            duration = task.duration_profiles[r_type] / speed
            
            start = max(node_free_time[best_node], deps_ready)
            finish = start + duration
            
            node_free_time[best_node] = finish
            task_finish_time[task.name] = finish
            
        return schedule

    def initialize_population(self):
        print(f"Initializing population with {self.population_size} schedules...")
        self.population = []
        
        self.population.append(self.generate_heuristic_schedule(strategy='time'))
        self.population.append(self.generate_heuristic_schedule(strategy='energy'))
        
        all_nodes = self.cluster.get_all_nodes()
        for _ in range(self.population_size - 2):
            chromosome = {}
            for task in self.workflow.tasks:
                valid_nodes = []
                for r_type in task.duration_profiles.keys():
                    for n in all_nodes:
                        if self.cluster.get_node_type(n) == r_type:
                            valid_nodes.append(n)
                
                if not valid_nodes:
                    raise Exception(f"Task {task.name} has no valid resources!")
                
                chromosome[task.name] = random.choice(valid_nodes)
            self.population.append(chromosome)

    def calculate_fitness(self, chromosome):
        node_free_time = {}
        for node in self.cluster.get_all_nodes():
            node_free_time[node] = 0
        task_finish_time = {}
        sorted_tasks = self.topological_sort()
        
        total_energy = 0
        total_wall_time = 0
        
        for task in sorted_tasks:
            assigned_node = chromosome[task.name]
            node_type = self.cluster.get_node_type(assigned_node)
            
            base_duration = task.duration_profiles[node_type]
            speed_factor = self.cluster.get_node_speed(assigned_node)
            duration = base_duration / speed_factor
            
            power = self.cluster.get_power_consumption(assigned_node)
            
            machine_ready = node_free_time[assigned_node]
            if not task.dependencies:
                deps_ready = 0
            else:
                deps_ready = max(task_finish_time[dep] for dep in task.dependencies)
            
            start_time = max(machine_ready, deps_ready)
            finish_time = start_time + duration
            
            node_free_time[assigned_node] = finish_time
            task_finish_time[task.name] = finish_time
            
            total_energy += (duration * power)
            total_wall_time += finish_time

        makespan = max(task_finish_time.values())
        avg_wall = total_wall_time / len(sorted_tasks)
        
        weights = self.weight_profiles[self.mode]
        
        score = (makespan * weights['makespan']) + \
                (total_energy * weights['energy']) + \
                (avg_wall * weights['wall'])
        
        return score, makespan, total_energy

    def topological_sort(self):
        sorted_tasks = []
        processed_ids = set()
        pending_tasks = list(self.workflow.tasks)
        while pending_tasks:
            progress = False
            for task in pending_tasks[:]:
                all_deps_in_processed = True
                for dep in task.dependencies:
                    if dep not in processed_ids:
                        all_deps_in_processed = False
                        break
                if all_deps_in_processed:
                    sorted_tasks.append(task)
                    processed_ids.add(task.name)
                    pending_tasks.remove(task)
                    progress = True
            if not progress: raise Exception("Circular dependency!")
        return sorted_tasks
    
    def select_parents(self):
        tournament_size = 5
        tournament = []
        for _ in range(tournament_size):
            tournament.append(random.choice(self.population))
        best = tournament[0]
        best_score = self.calculate_fitness(best)[0]
        for chromo in tournament:
            score = self.calculate_fitness(chromo)[0]
            if score < best_score:
                best = chromo
                best_score = score
        return best

    def crossover(self, parent1, parent2):
        child = {}
        for task_name in parent1.keys():
            rand_val = random.random()
            if rand_val > 0.5:
                child[task_name] = parent1[task_name]
            else:
                child[task_name] = parent2[task_name]
        return child

    def mutate(self, chromosome):
        mutation_rate = 0.15
        if random.random() > mutation_rate:
            return chromosome

        task_to_change = random.choice(list(chromosome.keys()))
        task = self.workflow.get_task(task_to_change)
        
        valid_nodes = []
        all_nodes = self.cluster.get_all_nodes()
        for r_type in task.duration_profiles.keys():
            for n in all_nodes:
                if self.cluster.get_node_type(n) == r_type:
                    valid_nodes.append(n)
        
        chromosome[task_to_change] = random.choice(valid_nodes)
        return chromosome

    def run(self):
        self.initialize_population()
        best_overall = None

        print(f"Starting evolution for {self.generations} generations...")

        for generation in range(self.generations):
            scored_pop = []
            for chromo in self.population:
                score, makespan, energy = self.calculate_fitness(chromo)
                scored_pop.append((score, chromo))

            scored_pop.sort(key=lambda x: x[0])
            current_best_score = scored_pop[0][0]
            current_best_chromo = scored_pop[0][1]
            
            if best_overall is None or current_best_score < best_overall[0]:
                best_overall = (current_best_score, current_best_chromo)
            
            if generation % 10 == 0:
                print(f"Gen {generation:<3} | Best Score: {current_best_score:.2f}")

            new_pop = []
            new_pop.append(scored_pop[0][1]) 
            new_pop.append(scored_pop[1][1]) 
            
            while len(new_pop) < self.population_size:
                parent1 = self.select_parents()
                parent2 = self.select_parents()
                child = self.crossover(parent1, parent2)
                child = self.mutate(child)
                new_pop.append(child)
            
            self.population = new_pop

        print(f"Evolution Complete. Best Score: {best_overall[0]:.2f}")
        return best_overall[1]

    def save_results_to_csv(self, chromosome, filename):
        node_free_time = {node: 0 for node in self.cluster.get_all_nodes()}
        task_finish_time = {}
        task_start_time = {}
        
        sorted_tasks = self.topological_sort()
        csv_rows = []
        total_energy = 0
        
        for task in sorted_tasks:
            assigned_node = chromosome[task.name]
            node_type = self.cluster.get_node_type(assigned_node)
            
            base_duration = task.duration_profiles[node_type]
            speed_factor = self.cluster.get_node_speed(assigned_node)
            duration = base_duration / speed_factor
            
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
            
        if csv_rows:
            fieldnames = csv_rows[0].keys()
            try:
                with open(filename, mode='w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(csv_rows)
                print(f"\nDetailed schedule written to: {filename}")
            except IOError as e:
                print(f"Error writing to CSV: {e}")

        self.print_results_summary(task_start_time, task_finish_time, chromosome, total_energy)

    def print_results_summary(self, task_start_time, task_finish_time, chromosome, total_energy):
        first_start = min(task_start_time.values())
        last_finish = max(task_finish_time.values())
        total_duration = last_finish - first_start
        
        wait_times = []
        wall_times = []
        fallback_count = 0
        
        for task_name, assigned_node in chromosome.items():
            task = self.workflow.get_task(task_name)
            wait_times.append(task_start_time[task_name])
            wall_times.append(task_finish_time[task_name])
            
            preferred = min(task.duration_profiles, key=task.duration_profiles.get)
            node_type = self.cluster.get_node_type(assigned_node)
            if node_type != preferred:
                fallback_count += 1

        avg_energy = total_energy / len(self.workflow.tasks)
        avg_wait = sum(wait_times) / len(wait_times)
        avg_wall = sum(wall_times) / len(wall_times)

        print("\n--- Simulation Results (Genetic Best) ---")
        print(f"Tasks: {len(self.workflow.tasks)}")
        print(f"Fallbacks: {fallback_count} jobs ran on slower resources")
        print(f"1. Total Workflow Duration: {total_duration:.2f} s")
        print(f"2. Total Energy Consumed:   {total_energy:.2f} J")
        print(f"3. Avg Wait Time per Job:   {avg_wait:.2f} s")
        print(f"4. Avg Wall Time per Job:   {avg_wall:.2f} s")
        print(f"5. Avg Energy per Job:      {avg_energy:.2f} J")
        print("-----------------------------------------")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Genetic Algorithm Scheduler")
    parser.add_argument("--tasks", type=int, default=20, help="Number of tasks")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--simple", action="store_true", help="Run simple example")
    parser.add_argument("--output", type=str, default="genetic_results.csv", help="Output CSV file")
    parser.add_argument("--gens", type=int, default=100, help="Generations to evolve")
    parser.add_argument("--pop", type=int, default=100, help="Population size")
    parser.add_argument("--mode", type=str, default="balanced", choices=['speed', 'energy', 'balanced'],
                        help="Optimization Mode: speed, energy, or balanced")
    
    args = parser.parse_args()
    
    c = Cluster()
    w = Workflow()
    
    if args.simple:
        w.create_sample_workflow()
    else:
        w.generate_random_workflow(num_tasks=args.tasks, seed=args.seed)
    
    ai = GeneticScheduler(c, w, population_size=args.pop, generations=args.gens, mode=args.mode)
    best_schedule = ai.run()
    
    ai.save_results_to_csv(best_schedule, args.output)