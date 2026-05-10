# Pipeline Merge Sort

Interactive and console-based simulations of a pipeline merge sort.

The project contains two versions:

- `pipeline-merge-sort.html` - browser visualization with step-by-step controls.
- `pipeline-merge-sort.py` - terminal simulation that prints queue states per clock tick.

## Browser Visualization

Open `pipeline-merge-sort.html` in a browser.

The visualization shows:

- input consumed from the right side,
- processors `P1`, `P2`, `P3`, ... with two queues each,
- queue capacities based on processor rank,
- extra visual buffer slots for readability,
- colored buffer groups that show which cells belong together,
- final output added from the left,
- forward and backward stepping.

Controls:

- `Reset` restarts the simulation from the input field.
- `Krok vpred` advances one clock tick.
- `Krok zpet` restores the previous simulation state.

## Console Simulation

Run:

```bash
python3 pipeline-merge-sort.py
```

The script prints the processor queues and final output after each clock tick.

## Timing Model

Each clock tick is simulated in two phases:

1. All processors call `work()` using the state available at the beginning of the tick.
2. New values are delivered to the first processor, between processors, or to the final output.

This means a value delivered to the next processor during a tick is available for that processor on the next tick.
