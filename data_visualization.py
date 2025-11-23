import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import os

def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def plot_gantt(ax, df, title):
    """Helper to draw a Gantt chart on a specific axes."""
    colors = {'CPU': '#1f77b4', 'GPU': '#ff7f0e'}
    
    nodes = sorted(df['Assigned Node'].unique())
    yticks = range(len(nodes))
    node_map = {node: i for i, node in enumerate(nodes)}
    
    for _, row in df.iterrows():
        node = row['Assigned Node']
        start = row['Start Time (s)']
        duration = row['Runtime (s)']
        r_type = row['Assigned Resource']
        
        ax.broken_barh([(start, duration)], (node_map[node] - 0.4, 0.8), 
                       facecolors=colors.get(r_type, 'gray'), edgecolor='black')
        
        # Only label if the bar is wide enough to be readable
        if duration > 5:
            ax.text(start + duration/2, node_map[node], row['Job ID'], 
                    ha='center', va='center', color='white', fontsize=6)

    ax.set_yticks(yticks)
    ax.set_yticklabels(nodes)
    ax.set_xlabel("Time (seconds)")
    ax.set_title(title)
    ax.grid(True, axis='x', linestyle='--', alpha=0.5)
    
    patches = [mpatches.Patch(color=c, label=l) for l, c in colors.items()]
    ax.legend(handles=patches, loc='upper right')

def compare_results():
    output_dir = "visualizations"
    ensure_dir(output_dir)

    try:
        fcfs = pd.read_csv("fcfs_results.csv")
        genetic = pd.read_csv("genetic_results.csv")
    except FileNotFoundError:
        print("Error: Could not find input files.")
        print("Please run:")
        print("  python fcfs.py --output fcfs_results.csv")
        print("  python scheduler.py --output genetic_results.csv")
        return

    # --- Calculate Metrics ---
    makespan_fcfs = fcfs['Finish Time (s)'].max()
    makespan_gen = genetic['Finish Time (s)'].max()
    
    energy_fcfs = fcfs['Energy (J)'].sum()
    energy_gen = genetic['Energy (J)'].sum()
    
    fb_fcfs = fcfs[fcfs['Fallback Occurred'] == 'YES'].shape[0]
    fb_gen = genetic[genetic['Fallback Occurred'] == 'YES'].shape[0]
    
    wait_fcfs = fcfs['Wait Time (s)'].mean()
    wait_gen = genetic['Wait Time (s)'].mean()
    
    wall_fcfs = fcfs['Walltime (s)'].mean()
    wall_gen = genetic['Walltime (s)'].mean()

    # --- Define Plot List ---
    # Format: (Title, [Values], Color, Filename, Y-Label)
    plots_to_generate = [
        ('Makespan Comparison', [makespan_fcfs, makespan_gen], 'green', 'makespan_comparison.png', 'Total Duration (s)'),
        ('Total Energy Comparison', [energy_fcfs, energy_gen], 'blue', 'energy_comparison.png', 'Energy (J)'),
        ('Fallback Count', [fb_fcfs, fb_gen], 'orange', 'fallback_comparison.png', 'Count'),
        ('Average Wait Time', [wait_fcfs, wait_gen], 'purple', 'wait_time_comparison.png', 'Time (s)'),
        ('Average Wall Time', [wall_fcfs, wall_gen], 'teal', 'wall_time_comparison.png', 'Time (s)')
    ]

    # --- Generate Individual Plots ---
    for title, values, color, filename, ylabel in plots_to_generate:
        plt.figure(figsize=(8, 6))
        bars = plt.bar(['FCFS', 'Genetic'], values, color=['gray', color])
        
        plt.title(title, fontsize=14)
        plt.ylabel(ylabel, fontsize=12)
        plt.grid(axis='y', linestyle='--', alpha=0.5)
        
        # Add text labels on bars
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height,
                     f'{height:.1f}',
                     ha='center', va='bottom', fontweight='bold')
        
        save_path = os.path.join(output_dir, filename)
        plt.savefig(save_path)
        print(f"Saved {title} to: {save_path}")
        plt.close() # Close figure to free memory

    # --- Generate Gantt Comparison (Stacked) ---
    # We keep these together because aligning the X-axis is critical for comparison
    fig_gantt, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10), sharex=True)
    
    plot_gantt(ax1, fcfs, f"FCFS Schedule (Makespan: {makespan_fcfs:.1f}s)")
    plot_gantt(ax2, genetic, f"Genetic Schedule (Makespan: {makespan_gen:.1f}s)")
    
    plt.tight_layout()
    gantt_path = os.path.join(output_dir, "gantt_comparison.png")
    plt.savefig(gantt_path)
    print(f"Saved Gantt comparison to: {gantt_path}")
    plt.close(fig_gantt)

if __name__ == "__main__":
    compare_results()