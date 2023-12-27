import json

import click

from compgraph.algorithms import pmi_graph


@click.command()
@click.argument("input_filepath", nargs=1)
@click.argument("output_filepath", nargs=1)
def run_pmi(input_filepath: str, output_filepath: str) -> None:
    graph = pmi_graph(input_filepath, from_file=True)

    result = graph.run(input=lambda: input_filepath)
    with open(output_filepath, "w") as out:
        json.dump(list(result), out)


if __name__ == "__main__":
    run_pmi()
