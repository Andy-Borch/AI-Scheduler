#TODO: Make mor jobs, more complicated. automate somehow?
class Job:
    def __init__(self, name, resource_type, duration, dependencies=None):
        self.name = name
        self.resource_type = resource_type
        self.duration = duration
        self.dependencies = dependencies if dependencies else []

    def __repr__(self):
        return f"Job({self.name}, {self.resource_type}, {self.duration}s)"

class Workflow:
    def __init__(self):
        self.jobs = []

    def job(self, job):
        self.jobs.append(job)

    def job(self, name):
        for t in self.job:
            if t.name == name:
                return t
        return None
    
    def create_sample_workflow(self):
        """Creates the example workflow from the project blueprint."""
        # Job A: Independent
        t_a = Job('Job A', 'cpu', 15, [])
        
        # B Series: Dependent Chain
        # Job B1 (CPU Pre-processing)
        t_b1 = Job('Job B1', 'cpu', 10, [])
        # Job B2 (GPU Computation), depends on B1
        t_b2 = Job('Job B2', 'gpu', 20, ['Job B1'])
        # Job B3 (CPU Post-processing), depends on B2
        t_b3 = Job('Job B3', 'cpu', 5, ['Job B2'])
        
        self.jobs = [t_a, t_b1, t_b2, t_b3]