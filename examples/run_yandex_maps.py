import json

import click

from compgraph.algorithms import yandex_maps_graph


@click.command()
@click.argument("input_time_filepath", nargs=1)
@click.argument("input_length_filepath", nargs=1)
@click.argument("output_filepath", nargs=1)
def run_yandex_maps(
    input_time_filepath: str, input_length_filepath: str, output_filepath: str
) -> None:
    graph = yandex_maps_graph(
        input_time_filepath,
        input_length_filepath,
        time_from_file=True,
        length_from_file=True,
    )

    result = graph.run()
    with open(output_filepath, "w") as out:
        json.dump(list(result), out)


if __name__ == "__main__":
    run_yandex_maps()
