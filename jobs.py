import random

class Task:
    def __init__(self, name, duration_profiles, dependencies=None):
        self.name = name
        self.duration_profiles = duration_profiles
        self.dependencies = dependencies if dependencies else []

class Workflow:
    def __init__(self):
        self.tasks = []

    def get_task(self, name):
        for t in self.tasks:
            if t.name == name:
                return t
        return None

    def create_sample_workflow(self):
        self.tasks = [
            Task("job_1", {'cpu': 200, 'gpu': 20}),
            Task("job_2", {'cpu': 400, 'gpu': 40}, dependencies=["job_1"]),
            Task("job_3", {'cpu': 200}),
            Task("job_4", {'gpu': 50, 'cpu': 500}, dependencies=["job_1"]),
            Task("job_5", {'cpu': 300}, dependencies=["job_2", "job_3"])
        ]

    def generate_random_workflow(self, num_tasks=20, seed=42):
        """
        Generates a random DAG (Directed Acyclic Graph) of tasks.
        With REALISTIC Penalties for architecture mismatches.
        """
        random.seed(seed)
        self.tasks = []
        
        for i in range(num_tasks):
            # 70% CPU tasks, 30% GPU tasks
            is_gpu_task = (random.random() < 0.3)
            
            profiles = {}
            
            if is_gpu_task:
                gpu_time = random.randint(200, 600)
                profiles['gpu'] = gpu_time
                # Penalty: 20x slower on CPU
                profiles['cpu'] = gpu_time * 20 
            else:
                cpu_time = random.randint(100, 1000)
                profiles['cpu'] = cpu_time
                
                profiles['gpu'] = cpu_time * 10.0 

            deps = []
            if i > 0:
                if random.random() < 0.4:
                    num_deps = random.randint(1, 4)
                    window_start = max(0, i - 10)
                    potential_parents = [t.name for t in self.tasks[window_start:i]]
                    
                    if potential_parents:
                        deps = random.sample(potential_parents, min(len(potential_parents), num_deps))
            
            self.tasks.append(Task(f"job_{i}", profiles, deps))

        print(f"Generating workflow with {num_tasks} tasks...")