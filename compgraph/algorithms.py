import json
from copy import copy

from compgraph.graph import Graph
from compgraph.joiner import InnerJoiner
from compgraph.mapper import CalcHaversine
from compgraph.mapper import Divide
from compgraph.mapper import Filter
from compgraph.mapper import FilterPunctuation
from compgraph.mapper import LowerCase
from compgraph.mapper import NaturalLog
from compgraph.mapper import ParseTime
from compgraph.mapper import Product
from compgraph.mapper import Project
from compgraph.mapper import Split
from compgraph.misc import Columns
from compgraph.misc import TRowsGenerator
from compgraph.reducer import Count
from compgraph.reducer import NUnique
from compgraph.reducer import Speed
from compgraph.reducer import TermFrequency
from compgraph.reducer import TopN


def json_line_parser(line: str) -> TRowsGenerator:
    yield from json.loads(line)


def init_graph(input_stream_name: str, from_file: bool = False) -> "Graph":
    if from_file:
        return Graph.graph_from_file(input_stream_name, json_line_parser)
    return Graph.graph_from_iter(input_stream_name)


def word_count_graph(
    input_stream_name: str,
    text_column: str = "text",
    count_column: str = "count",
    from_file: bool = False,
) -> Graph:
    """Constructs graph which counts words in text_column of all rows passed"""

    return (
        init_graph(input_stream_name, from_file)
        .map(FilterPunctuation(text_column))
        .map(LowerCase(text_column))
        .map(Split(text_column))
        .sort([text_column])
        .reduce(Count(count_column), [text_column])
        .sort([count_column, text_column])
    )


def inverted_index_graph(
    input_stream_name: str,
    doc_column: str = "doc_id",
    text_column: str = "text",
    result_column: str = "tf_idf",
    from_file: bool = False,
) -> Graph:
    """Constructs graph which calculates tf-idf for every word/document pair"""
    graph = init_graph(input_stream_name, from_file)

    split_word = (
        copy(graph)
        .map(FilterPunctuation(text_column))
        .map(LowerCase(text_column))
        .map(Split(text_column))
    )

    count_docs = (
        copy(graph)
        .map(Project([doc_column]))
        .reduce(NUnique(doc_column, Columns.n_docs), [])
    )

    freq = (
        copy(split_word)
        .sort([doc_column])
        .reduce(TermFrequency(text_column, Columns.frequency), [doc_column])
        .sort([text_column])
    )

    words = (
        copy(split_word)
        .sort([text_column])
        .reduce(NUnique(doc_column, Columns.presence_in_docs), [text_column])
        .sort([text_column])
    )

    tf_idf = (
        copy(count_docs)
        .join(InnerJoiner(), freq, [])
        .join(InnerJoiner(), words, [text_column])
        .map(Divide(Columns.n_docs, Columns.presence_in_docs, Columns.fraction))
        .map(NaturalLog(Columns.fraction, Columns.log))
        .map(Product([Columns.frequency, Columns.log], result_column))
    )
    result = (
        copy(tf_idf)
        .map(Project([doc_column, text_column, result_column]))
        .sort([text_column])
        .reduce(TopN(result_column, 3), [text_column])
    )

    return result


def pmi_graph(
    input_stream_name: str,
    doc_column: str = "doc_id",
    text_column: str = "text",
    result_column: str = "pmi",
    from_file: bool = False,
) -> Graph:
    """Constructs graph which gives for every document the top 10 words ranked by pointwise mutual information"""

    graph = init_graph(input_stream_name, from_file)

    graph = (
        copy(graph)
        .map(FilterPunctuation(text_column))
        .map(LowerCase(text_column))
        .map(Split(text_column))
        .sort([doc_column, text_column])
    )

    filtered1 = copy(graph).map(Filter(lambda row: len(row[text_column]) > 4))
    filtered2 = (
        copy(graph)
        .reduce(Count(Columns.count), [doc_column, text_column])
        .map(Filter(lambda row: row["count"] >= 2))
    )
    filtered = copy(filtered1).join(
        InnerJoiner(), filtered2, [doc_column, text_column]
    )

    frequency = (
        copy(filtered)
        .reduce(TermFrequency(text_column, Columns.frequency), [doc_column])
        .sort([text_column])
    )

    frequency_all = (
        copy(filtered)
        .reduce(TermFrequency(text_column, Columns.frequency_all), [])
        .sort([text_column])
    )

    result = (
        copy(frequency)
        .join(InnerJoiner(), frequency_all, [text_column])
        .map(Divide(Columns.frequency, Columns.frequency_all, Columns.fraction))
        .map(NaturalLog(Columns.fraction, result_column))
        .map(Project([doc_column, text_column, result_column]))
        .sort([doc_column, result_column, text_column])
        .reduce(TopN(result_column, 10), [doc_column])
    )
    return result


def yandex_maps_graph(
    input_stream_name_time: str,
    input_stream_name_length: str,
    enter_time_column: str = "enter_time",
    leave_time_column: str = "leave_time",
    edge_id_column: str = "edge_id",
    start_coord_column: str = "start",
    end_coord_column: str = "end",
    weekday_result_column: str = "weekday",
    hour_result_column: str = "hour",
    speed_result_column: str = "speed",
    time_from_file: bool = False,
    length_from_file: bool = False,
) -> Graph:
    """Constructs graph which measures average speed in km/h depending on the weekday and hour"""
    graph_time = init_graph(input_stream_name_time, time_from_file)

    time = graph_time.map(
        ParseTime(
            enter_time_column,
            "%Y%m%dT%H%M%S.%f",
            weekday_result_column,
            hour_result_column,
        )
    ).sort([edge_id_column])

    graph_length = init_graph(input_stream_name_length, length_from_file)

    length = graph_length.map(
        CalcHaversine(start_coord_column, end_coord_column, "length")
    ).sort([edge_id_column])

    graph = (
        time.join(InnerJoiner(), length, [edge_id_column])
        .sort([weekday_result_column, hour_result_column])
        .reduce(
            Speed(
                "length",
                enter_time_column,
                leave_time_column,
                "%Y%m%dT%H%M%S.%f",
                speed_result_column,
            ),
            [weekday_result_column, hour_result_column],
        )
    )

    return graph
