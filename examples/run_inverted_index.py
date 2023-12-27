import json

import click

from compgraph.algorithms import inverted_index_graph


@click.command()
@click.argument("input_filepath", nargs=1)
@click.argument("output_filepath", nargs=1)
def run_inverted_index(input_filepath: str, output_filepath: str) -> None:
    graph = inverted_index_graph(input_filepath, from_file=True)

    result = graph.run()
    print(result)
    with open(output_filepath, "w") as out:
        json.dump(list(result), out)


if __name__ == "__main__":
    input_filename = "input.txt"
    output_filename = "output.txt"

    rows = [
        {"doc_id": 1, "text": "hello, little world"},
        {"doc_id": 2, "text": "little"},
        {"doc_id": 3, "text": "little little little"},
        {"doc_id": 4, "text": "little? hello little world"},
        {"doc_id": 5, "text": "HELLO HELLO! WORLD..."},
        {"doc_id": 6, "text": "world? world... world!!! WORLD!!! HELLO!!!"},
    ]

    with open(input_filename, "w") as fp:
        json.dump(rows, fp)

    graph = inverted_index_graph(input_filename, from_file=True)

    result = graph.run()
    # print(list(result))
    with open(output_filename, "w") as out:
        json.dump(list(result), out)
