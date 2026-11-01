"""Execute the two offline demonstration notebooks without network.

The complete original studies are never executed or rewritten by this script.

No kernel/server is started. These notebooks use ordinary Python cells only.
By default outputs are validated in memory; --write saves freshly executed outputs.
"""
import argparse
import base64
from contextlib import redirect_stdout
import hashlib
from io import BytesIO, StringIO
from pathlib import Path
import socket
from unittest.mock import patch

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import nbformat


def deny_network(*args, **kwargs):
    raise RuntimeError("Network is disabled during notebook execution")


def execute(path, write=False):
    notebook = nbformat.read(path, as_version=4)
    nbformat.validate(notebook)
    namespace = {"__name__": "__main__"}
    count = 0
    for index, cell in enumerate(notebook.cells):
        if cell.cell_type != "code":
            continue
        count += 1
        outputs = []
        stream = StringIO()

        def display(value):
            data = {"text/plain": str(value)}
            if hasattr(value, "to_html"):
                data["text/html"] = value.to_html()
            outputs.append(nbformat.v4.new_output("display_data", data=data))

        def show(*args, **kwargs):
            for number in plt.get_fignums():
                buffer = BytesIO()
                plt.figure(number).savefig(buffer, format="png", dpi=100, bbox_inches="tight")
                outputs.append(nbformat.v4.new_output("display_data", data={
                    "image/png": base64.b64encode(buffer.getvalue()).decode("ascii"),
                    "text/plain": "Synthetic portfolio wealth chart"}))
            plt.close("all")

        namespace["display"] = display
        with patch.object(socket.socket, "connect", deny_network), \
             patch.object(socket.socket, "connect_ex", deny_network), \
             patch.object(socket, "create_connection", deny_network), \
             patch.object(plt, "show", show), redirect_stdout(stream):
            exec(compile(cell.source, f"{path.name}:cell-{index}", "exec"), namespace)
        if stream.getvalue():
            outputs.insert(0, nbformat.v4.new_output("stream", name="stdout", text=stream.getvalue()))
        cell.outputs, cell.execution_count = outputs, count
    notebook.metadata["demonstration"] = {
        "dataset_kind": "synthetic", "network": "blocked_during_execution",
        "core_sha256": hashlib.sha256((path.parent / "risk_return/core.py").read_bytes()).hexdigest(),
        "execution": "ordinary Python cells, fresh namespace, captured display and figures"}
    nbformat.validate(notebook)
    if write:
        nbformat.write(notebook, path)
    print(f"{path.name}: {count} code cells passed; synthetic outputs {'saved' if write else 'validated'}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    for language in ("EN", "ES"):
        execute(root / f"riskandreturn-offline-{language}.ipynb", args.write)


if __name__ == "__main__":
    main()
