from sqlite3 import connect
import pandas as pd
from json import load
from rdflib import Graph, Literal, RDF, RDFS, URIRef


def upload_to_db(db_path, df: pd.DataFrame, name):
    try:
        with connect(db_path) as con:
            df.to_sql(name, con, if_exists='replace', index=False)
        return True
    except Exception as e:
        print(f"Upload failed: {str(e)}")
        return False


def remove_invalid_char(s: str) -> str:
    if '\"' in s:
        return s.replace('\"', '\\\"')
    elif '"' in s:
        return s.replace('"', '\\\"')
    else:
        return s


def create_graph(json_obj: dict, base_url: str, create_graph: Graph):
    collection_id = URIRef(json_obj['id'])
    type_canvas = URIRef(base_url + 'Canvas')
    type_collection = URIRef(base_url + 'Collection')
    type_manifest = URIRef(base_url + 'Manifest')
    type_metadata = URIRef(base_url + 'Metadata')
    prop_id = URIRef('https://schema.org/identifier')
    prop_part = URIRef('https://www.w3.org/2002/07/owl#hasPart')
    prop_label = URIRef(base_url + 'label')
    prop_items = URIRef(base_url + 'items')

    label_list = list(json_obj['label'].values())
    label_value = label_list[0][0]
    label_value = remove_invalid_char(str(label_value))

    create_graph.add((collection_id, prop_id, Literal(json_obj['id'])))
    create_graph.add((collection_id, RDF.type, type_collection))
    create_graph.add((collection_id, RDFS.label, Literal(str(label_value))))

    for manifest in json_obj['items']:
        manifest_id = URIRef(manifest['id'])

        create_graph.add((collection_id, prop_part, manifest_id))
        create_graph.add((manifest_id, prop_id, Literal(manifest['id'])))
        create_graph.add((manifest_id, RDF.type, type_manifest))

        manifest_label_list = list(manifest['label'].values())
        manifest_label_value = manifest_label_list[0][0]
        manifest_label_value = remove_invalid_char(str(manifest_label_value))

        create_graph.add((manifest_id, RDFS.label, Literal(str(manifest_label_value))))

        for canvas in manifest['items']:
            canvas_id = URIRef(canvas['id'])

            create_graph.add((manifest_id, prop_part, canvas_id))
            create_graph.add((canvas_id, prop_id, Literal(canvas['id'])))
            create_graph.add((canvas_id, RDF.type, type_canvas))

            canvas_label_list = list(canvas['label'].values())
            canvas_label_value = canvas_label_list[0][0]
            canvas_label_value = remove_invalid_char(str(canvas_label_value))

            create_graph.add((canvas_id, RDFS.label, Literal(str(canvas_label_value))))