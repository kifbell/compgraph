import json

import click

from compgraph.algorithms import word_count_graph


@click.command()
@click.argument("input_stream_name", nargs=1)
@click.argument("output_filepath", nargs=1)
def main(input_stream_name: str, output_filepath: str) -> None:
    graph = word_count_graph(input_stream_name=input_stream_name, from_file=True)

    result = graph.run()
    with open(output_filepath, "w") as out:
        # for row in result:
        #     print(row, file=out)
        json.dump(list(result), out)


if __name__ == "__main__":
    main()
