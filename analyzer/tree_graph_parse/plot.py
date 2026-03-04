from typing import Optional
from numpy.typing import NDArray
import numpy as np
import matplotlib.pyplot as plt


def plot_percentiles(confirm_times: NDArray[np.float64], *, max_percentile: int = 100, save_fig: Optional[str] = None):
    # --- Step 1: Prepare the Data ---
    data = confirm_times

    # --- Step 2: Sort the data ---
    # To plot percentiles, the data must be sorted in ascending order.
    data_sorted = np.sort(data)

    # --- Step 3: Calculate the percentile ranks ---
    # We create a corresponding y-axis that ranges from 0 to 100.
    # np.linspace creates an array of evenly spaced numbers over a specified interval.
    percentiles = np.linspace(0, 100, len(data_sorted))

    # --- Step 4: Create the Plot ---
    # Set up the plot figure and axes
    fig, ax = plt.subplots(figsize=(12, 6))

    # Plot the sorted data against the percentiles
    ax.plot(percentiles[percentiles<=max_percentile], data_sorted[percentiles<=max_percentile])

    # --- Step 5: Customize the Plot with English Text ---
    # Add a title to the plot
    ax.set_title("Confirmation Time by Percentile", fontsize=24)

    # Add a label to the x-axis
    ax.set_xlabel("Node Percentile", fontsize=18)

    # Add a label to the y-axis
    ax.set_ylabel("Confirm Time (seconds)", fontsize=18)

    # Add a grid for better readability
    ax.grid(True)

    if save_fig is not None:
        fig.savefig(save_fig)
        
    # Display the plot
    plt.show()