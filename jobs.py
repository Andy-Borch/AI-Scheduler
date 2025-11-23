import random

class Task:
    def __init__(self, name, duration_profiles, dependencies=None):
        self.name = name
        self.duration_profiles = duration_profiles 
        self.dependencies = dependencies if dependencies else []

    def __repr__(self):
        return f"Task({self.name}, {self.duration_profiles})"

class Workflow:
    def __init__(self):
        self.tasks = []

    def add_task(self, task):
        self.tasks.append(task)

    def get_task(self, name):
        for t in self.tasks:
            if t.name == name:
                return t
        return None
    
    def create_sample_workflow(self):
        t_a = Task('Task A', {'cpu': 15}, [])
        t_b1 = Task('Task B1', {'cpu': 10}, [])
        t_b2 = Task('Task B2', {'gpu': 20, 'cpu': 65}, ['Task B1'])
        t_b3 = Task('Task B3', {'cpu': 5}, ['Task B2'])
        self.tasks = [t_a, t_b1, t_b2, t_b3]

    def generate_random_workflow(self, num_tasks=20, seed=None):
        """
        Generates a complex, random DAG of tasks with enough parallelism to fill a cluster.
        """
        if seed is not None:
            random.seed(seed)
            
        self.tasks = []
        print(f"Generating workflow with {num_tasks} tasks...")
        
        # Ensure ~25% of tasks are "roots" (start at T=0 with no dependencies)
        num_roots = max(4, int(num_tasks * 0.25))

        for i in range(num_tasks):
            name = f"Job_{i}"
            
            # Randomize Resource Profile
            r_type = random.random()
            
            if r_type < 0.4:
                # CPU Only
                duration = random.randint(10, 50)
                profile = {'cpu': duration}
                
            elif r_type < 0.6:
                # GPU Only
                duration = random.randint(10, 40)
                profile = {'gpu': duration}
                
            else:
                # Hybrid with Tradeoff
                gpu_time = random.randint(10, 30)
                cpu_time = int(gpu_time * random.uniform(2.0, 5.0)) 
                profile = {'gpu': gpu_time, 'cpu': cpu_time}
            
            # 2. Randomize Dependencies
            deps = []
            
            if i >= num_roots:
                if random.random() > 0.2:
                    num_parents = random.randint(1, 3)
                    candidates = [t.name for t in self.tasks]
                    deps = random.sample(candidates, num_parents)
            
            self.tasks.append(Task(name, profile, deps))