"""
Visualization utilities for autoscaling.
Creates plots and charts from cluster data.
"""
import calendar
import os
from datetime import datetime
from typing import Optional

import matplotlib.backends.backend_pdf
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


class ClusterVisualizer:
    """
    Visualizer for cluster data.
    Creates PDF charts from CSV log data.
    """

    def __init__(
        self,
        csv_file: str = "autoscaling.csv",
        output_dir: str = ".",
    ):
        """
        Initialize the visualizer.

        Args:
            csv_file: Path to the CSV log file
            output_dir: Directory for output files
        """
        self.csv_file = csv_file
        self.output_dir = output_dir

    def visualize(
        self,
        time_range: Optional[str] = None,
        output_file: Optional[str] = None,
    ) -> Optional[str]:
        """
        Create visualization PDF from CSV data.

        Args:
            time_range: Time range filter (format: "y-m-t-h:y-m-t-h")
            output_file: Output PDF file path

        Returns:
            Path to generated PDF or None on error
        """
        # Load CSV data
        df = self._load_csv_data()
        if df is None:
            return None

        # Apply time range filter
        if time_range:
            df = self._apply_time_range(df, time_range)
            if df.empty:
                print("No data in this time range")
                return None

        # Generate output filename
        if output_file is None:
            base_name = os.path.splitext(self.csv_file)[0]
            output_file = f"{base_name}_visual.pdf"

        # Create PDF
        pdf = matplotlib.backends.backend_pdf.PdfPages(output_file)

        # Create memory usage chart
        fig1 = self._create_memory_chart(df)
        pdf.savefig(fig1, bbox_inches="tight")
        plt.close(fig1)

        # Create pending jobs chart
        fig2 = self._create_pending_jobs_chart(df)
        pdf.savefig(fig2, bbox_inches="tight")
        plt.close(fig2)

        pdf.close()

        print(f"Visualization saved to {output_file}")
        return output_file

    def _load_csv_data(self) -> Optional[pd.DataFrame]:
        """
        Load CSV data into pandas DataFrame.

        Returns:
            DataFrame or None on error
        """
        if not os.path.exists(self.csv_file):
            print(f"CSV file not found: {self.csv_file}")
            return None

        try:
            df = pd.read_csv(self.csv_file, sep=",", on_bad_lines="warn")
            return df
        except Exception as e:
            print(f"Error loading CSV: {e}")
            return None

    def _apply_time_range(
        self, df: pd.DataFrame, time_range: str
    ) -> pd.DataFrame:
        """
        Apply time range filter to DataFrame.

        Args:
            df: Input DataFrame
            time_range: Time range string

        Returns:
            Filtered DataFrame
        """
        if ":" not in time_range:
            return df

        parts = time_range.split(":")
        if len(parts) != 2:
            return df

        try:
            start_parts = list(map(int, parts[0].split("-")))
            stop_parts = list(map(int, parts[1].split("-")))

            start_time = calendar.timegm(
                datetime(
                    start_parts[0],
                    start_parts[1],
                    start_parts[2],
                    start_parts[3],
                    0,
                    0,
                ).timetuple()
            )
            stop_time = calendar.timegm(
                datetime(
                    stop_parts[0],
                    stop_parts[1],
                    stop_parts[2],
                    stop_parts[3],
                    0,
                    0,
                ).timetuple()
            )

            df = df[(df["time"] > start_time) & (df["time"] < stop_time)]
        except (ValueError, IndexError) as e:
            print(f"Error parsing time range: {e}")

        return df

    def _create_memory_chart(self, df: pd.DataFrame) -> plt.Figure:
        """
        Create memory usage chart.

        Args:
            df: DataFrame with log data

        Returns:
            Matplotlib figure
        """
        mem_scale = 1000000

        # Prepare data
        df["date"] = pd.to_datetime(df["time"], unit="s")
        df["alloc"] = df["worker_mem_alloc"] / mem_scale
        df["alloc_drain"] = df["worker_mem_alloc_drain"] / mem_scale
        df["mix"] = df["worker_mem_mix"] / mem_scale
        df["mix_drain"] = df["worker_mem_mix_drain"] / mem_scale
        df["idle_drain"] = df["worker_mem_idle_drain"] / mem_scale
        df["idle"] = df["worker_mem_idle"] / mem_scale

        # Calculate chart width
        data_count = len(df)
        width = max(data_count * 5, 10)

        # Create figure
        fig, ax = plt.subplots(figsize=(width, 5))

        # Plot memory usage
        bottom = 0
        widths = df["date"].diff().fillna(0).values
        widths = _numpy_append(widths, widths[-1])

        names = [
            "allocated",
            "allocated + drain",
            "mix",
            "mix+drain",
            "idle",
            "idle+drain",
        ]
        colors = ["tab:blue", "tab:orange", "tab:green", "tab:red", "tab:purple", "tab:brown"]

        for col, name, color in zip(
            ["alloc", "alloc_drain", "mix", "mix_drain", "idle", "idle_drain"],
            names,
            colors,
        ):
            if not df[col].eq(0.0).all():
                ax.bar(
                    df["date"],
                    df[col],
                    bottom=bottom,
                    width=widths,
                    align="edge",
                    alpha=0.4,
                    label=name,
                    color=color,
                )
                bottom += df[col]

        # Plot job resources
        if not df["job_alloc"].isnull().all():
            ax.plot(
                df["date"],
                df["job_alloc"] / mem_scale,
                linestyle="solid",
                color="blue",
                alpha=0.4,
                label="allocated by jobs",
            )

        # Add scaling markers
        self._add_scaling_markers(ax, df)

        # Configure axes
        ax.set_xlabel("time")
        ax.set_ylabel("memory resources (TB)")
        ax.set_title("Cluster Memory Usage")
        ax.margins(x=0.01)
        plt.grid()
        plt.xticks(rotation=45)
        ax.legend(loc="upper left")

        return fig

    def _create_pending_jobs_chart(self, df: pd.DataFrame) -> plt.Figure:
        """
        Create pending jobs chart.

        Args:
            df: DataFrame with log data

        Returns:
            Matplotlib figure
        """
        # Calculate chart width
        data_count = len(df)
        width = max(data_count * 5, 10)

        # Create figure
        fig, ax = plt.subplots(figsize=(width, 5))

        # Prepare data
        df["date"] = pd.to_datetime(df["time"], unit="s")
        widths = df["date"].diff().fillna(0).values
        widths = _numpy_append(widths, widths[-1])

        # Plot pending jobs
        ax.bar(
            df["date"],
            df["job_cnt_pending"],
            width=widths,
            align="edge",
            alpha=0.4,
            label="pending jobs",
            color="tab:blue",
        )

        # Configure axes
        ax.set_xlabel("time")
        ax.set_ylabel("number of pending jobs")
        ax.margins(x=0.01)
        plt.grid()
        plt.xticks(rotation=45)
        ax.legend(loc="upper left")

        return fig

    def _add_scaling_markers(self, ax, df: pd.DataFrame) -> None:
        """
        Add scaling event markers to chart.

        Args:
            ax: Matplotlib axes
            df: DataFrame with log data
        """
        scale = df["worker_mem_alloc"].max() / 1000000

        markers = {
            "U": ("^", "scale-up", "green", 4),
            "D": ("v", "scale-down", "orange", 4),
            "M": ("1", "autoscale", "black", 4),
            "E": ("X", "error", "red", 4),
            "Y": ("d", "scale-down drain", "blue", 4),
            "X": ("o", "reactivate worker", "orange", 4),
            "I": (">", "scale-up (api)", "green", 4),
            "L": ("D", "insufficient resources", "red", 4),
        }

        for scale_val, (marker, label, color, size) in markers.items():
            if scale_val in df["scale"].values:
                dfmark = df[df["scale"] == scale_val].copy()
                dfmark["scale_val"] = dfmark["scale"].map({scale_val: scale / 5})
                ax.plot(
                    dfmark["date"],
                    dfmark["scale_val"],
                    marker=marker,
                    linestyle="None",
                    label=label,
                    color=color,
                    alpha=0.4,
                    markersize=size,
                )


# --- Helper Functions ---


def _numpy_append(arr, value):
    """Simple append for numpy arrays."""
    return np.append(arr, value)


def visualize_cluster_data(
    csv_file: str = "autoscaling.csv",
    time_range: Optional[str] = None,
    output_file: Optional[str] = None,
) -> Optional[str]:
    """
    Convenience function to create visualization.

    Args:
        csv_file: Path to CSV log file
        time_range: Time range filter
        output_file: Output PDF file path

    Returns:
        Path to generated PDF or None
    """
    visualizer = ClusterVisualizer(csv_file=csv_file)
    return visualizer.visual(time_range=time_range, output_file=output_file)
