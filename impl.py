import pandas as pd
import rdflib
from models.main_models import *  # import the entirety of main_models
from utils import upload_to_db, create_graph, remove_invalid_char
import sqlite3
from sqlite3 import connect
from pandas import read_sql, concat
from sparql_dataframe import get
from json import load
from rdflib import Graph, URIRef  # for loading rdflibrary, used in CollectionProcessor
from rdflib.plugins.stores.sparqlstore import (
    SPARQLUpdateStore,
)  # for using rdflib plugin for sparql store update function
from rdflib.plugins.sparql import prepareQuery


# https://github.com/comp-data/2022-2023/tree/main/docs/project#uml-of-additional-classes


class Processor(object):
    def __init__(self):
        self.dbPathOrUrl = None

    def getDbPathOrUrl(self) -> str:
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl: str) -> bool:
        self.dbPathOrUrl = pathOrUrl
        return True


class MetadataProcessor(Processor):
    def uploadData(self, path: str) -> bool:
        metadata = pd.read_csv(
            path,
            keep_default_na=False,
            dtype={"id": "string", "title": "string", "creator": "string"},
        )
        return upload_to_db(self.dbPathOrUrl, metadata, "Metadata")


# Done by Evan
class CollectionProcessor(Processor):
    def __init__(self):
        super().__init__()

    def uploadData(self, path: str):
        try:
            base_url = "https://github.com/mjavadf/rumi_group_project/"  # "D:/Projects/rumi_group_project"
            new_graph = Graph()

            with open(path, mode="r", encoding="utf-8") as j:
                json_obj = load(j)

            # this check the loaded json file
            if type(json_obj) is list:
                for collection in json_obj:
                    create_graph(collection, base_url, new_graph)
            else:
                create_graph(json_obj, base_url, new_graph)

            store = SPARQLUpdateStore()
            endpoint = self.getDbPathOrUrl()
            store.open((endpoint, endpoint))
            for triple in new_graph.triples((None, None, None)):
                store.add(triple)
            store.close()

            # delete below comment in case we want to visualize a turtle file from the Collections
            # new_graph.serialize(destination="Turtle_Visualization.ttl", format="turtle")
            return True

        except Exception as e:
            print(f"Upload failed: {str(e)}")
            return False


class AnnotationProcessor(Processor):
    def uploadData(self, path: str) -> bool:
        annotations = pd.read_csv(
            path,
            keep_default_na=False,
            dtype={
                "id": "string",
                "body": "string",
                "target": "string",
                "motivation": "string",
            },
        )
        return upload_to_db(self.dbPathOrUrl, annotations, "Annotations")


class QueryProcessor(Processor):
    def getEntityById():
        pass


# Done by Thomas and Evgeniia
class TriplestoreQueryProcessor(Processor):
    SPARQL_PREFIXES = """
        PREFIX schema: <https://schema.org/>
        PREFIX rumi: <https://github.com/mjavadf/rumi_group_project/>
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#> 
    """

    def getEntityById(self, entity_id):
        endpoint = self.getDbPathOrUrl()
        query = f"""
            {TriplestoreQueryProcessor.SPARQL_PREFIXES}
            
            SELECT ?id ?type ?label
            WHERE {{
                ?entity schema:identifier "{entity_id}" .
                ?entity schema:identifier ?id .
                ?entity rdf:type ?type .
                ?entity rdfs:label ?label .
                
            }}
            """
        df_sparql = get(endpoint, query, True)
        df_sparql = pd.DataFrame(df_sparql)
        return df_sparql

    def getAllCanvases(self):
        endpoint = self.getDbPathOrUrl()
        query_Canvas = f"""
            {TriplestoreQueryProcessor.SPARQL_PREFIXES}

            SELECT ?id ?label 
            WHERE {{
                ?s rdf:type rumi:Canvas ;
                    schema:identifier ?id ;
                    rdfs:label ?label .

            }}
            """
        df_sparql_getAllCanvases = get(endpoint, query_Canvas, True)
        df_canvases = pd.DataFrame(df_sparql_getAllCanvases)
        return df_canvases

    def getAllCollections(self):
        endpoint = self.getDbPathOrUrl()
        query_Collection = f"""
            {TriplestoreQueryProcessor.SPARQL_PREFIXES}
            
            SELECT ?id ?label 
            WHERE {{
                ?s rdf:type rumi:Collection ;
                   schema:identifier ?id ;
                   rdfs:label ?label .
            }}
            """
        df_sparql_getAllCollections = get(endpoint, query_Collection, True)
        df_collections = pd.DataFrame(df_sparql_getAllCollections)
        return df_collections

    def getAllManifests(self):
        endpoint = self.getDbPathOrUrl()
        query_Manifest = f"""
            {TriplestoreQueryProcessor.SPARQL_PREFIXES}
            
            SELECT ?id ?label
            WHERE {{
                ?s rdf:type rumi:Manifest ;
                schema:identifier ?id ;
                rdfs:label ?label .
        
            }}
            """
        df_sparql_getAllManifests = get(endpoint, query_Manifest, True)
        df_manifests = pd.DataFrame(df_sparql_getAllManifests)
        return df_manifests

    def getCanvasesInCollection(self, collection_id):
        endpoint = self.getDbPathOrUrl()
        query_CanvasInCollection = f"""
            {TriplestoreQueryProcessor.SPARQL_PREFIXES}
    
            SELECT ?id ?label
            WHERE {{
                ?collection_id rdf:type rumi:Collection ;
                    rumi:items ?manifest_id .
                ?manifest_id rdf:type rumi:Manifest ;
                    rumi:items ?Canvas .
                ?Canvas schema:identifier ?id ;
                        rdfs:label ?label .

                FILTER(?collection_id = <{collection_id}> )     
            }}
            """

        df_sparql_CanvasesInCollection = get(endpoint, query_CanvasInCollection, True)
        df_canvas_in_collection = pd.DataFrame(df_sparql_CanvasesInCollection)
        return df_canvas_in_collection

    def getCanvasesInManifest(self, manifest_id):
        endpoint = self.getDbPathOrUrl()
        query_CanvasInManifest = f"""
            {TriplestoreQueryProcessor.SPARQL_PREFIXES}
        
            SELECT ?id ?label
            WHERE {{
                ?manifest_id rdf:type rumi:Manifest ;
                    rumi:items ?id .
                ?id rdf:type rumi:Canvas ;
                    rdfs:label ?label .

                FILTER(?manifest_id = <{manifest_id}> )            
            }}
            """
        df_sparql_CanvasesInManifest = get(endpoint, query_CanvasInManifest, True)
        df_canvas_in_manifest = pd.DataFrame(df_sparql_CanvasesInManifest)
        return df_canvas_in_manifest

    def getEntitiesWithLabel(self, label):
        endpoint = self.getDbPathOrUrl()
        query_EntitiesWLabel = f"""
            {TriplestoreQueryProcessor.SPARQL_PREFIXES}
            
            SELECT ?id ?type ?label 
            WHERE {{
            ?entity rdfs:label "{label}" .
            ?entity schema:identifier ?id .
            ?entity rdf:type ?type .
            ?entity rdfs:label ?label .

            }}
            """
        df_sparql_getEntitiesWLabel = get(endpoint, query_EntitiesWLabel, True)
        df_entity_with_label = pd.DataFrame(df_sparql_getEntitiesWLabel)
        return df_entity_with_label

    def getManifestsInCollection(self, collection_id):
        endpoint = self.getDbPathOrUrl()
        query_ManifestsInCollection = f"""
            {TriplestoreQueryProcessor.SPARQL_PREFIXES}
            
            SELECT ?id ?label
            WHERE {{
                ?collection_id rdf:type rumi:Collection ;
                               rumi:items ?id .
                ?id rdf:type rumi:Manifest ;
                    rdfs:label ?label .

                FILTER(?collection_id = <{collection_id}> )     
            }}                            
            """
        df_sparql_ManifestsInCollection = get(
            endpoint, query_ManifestsInCollection, True
        )
        df_manifest_in_collection = pd.DataFrame(df_sparql_ManifestsInCollection)
        return df_manifest_in_collection


# by Evgeniia
class RelationalQueryProcessor(QueryProcessor):
    def getAllAnnotations(self):
        with sqlite3.connect(self.dbPathOrUrl) as con:
            query = "SELECT * FROM annotations"
            result = pd.read_sql(query, con)
        return result

    def getAllImages(self):
        with sqlite3.connect(self.dbPathOrUrl) as con:
            query = "SELECT body FROM annotations"
            result = pd.read_sql(query, con)
        return result

    def getAnnotationsWithBody(self, body):
        with sqlite3.connect(self.dbPathOrUrl) as con:
            query = "SELECT * FROM annotations WHERE body = ?"
            result = pd.read_sql(query, con, params=(body,))
        return result

    def getAnnotationsWithBodyAndTarget(self, body, target):
        with sqlite3.connect(self.dbPathOrUrl) as con:
            query = "SELECT * FROM annotations WHERE body = ? AND target = ?"
            result = pd.read_sql(
                query,
                con,
                params=(
                    body,
                    target,
                ),
            )
        return result

    def getAnnotationsWithTarget(self, target):
        with sqlite3.connect(self.dbPathOrUrl) as con:
            query = "SELECT * FROM annotations WHERE target = ?"
            result = pd.read_sql(query, con, params=(target,))
        return result

    def getEntitiesWithCreator(self, creator):
        with sqlite3.connect(self.dbPathOrUrl) as con:
            query = "SELECT * FROM metadata WHERE creator = ?"
            result = pd.read_sql(query, con, params=(creator,))
        return result

    # By javad
    def getEntitiesWithType(self, type):
        with sqlite3.connect(self.dbPathOrUrl) as con:
            query = "SELECT * FROM metadata WHERE creator = ?"
            result = pd.read_sql(query, con, params=(type,))
        return result

    def getEntitiesWithTitle(self, title):
        with sqlite3.connect(self.dbPathOrUrl) as con:
            query = "SELECT * FROM metadata WHERE title = ?"
            result = pd.read_sql(query, con, params=(title,))
        return result

    def getEntityById(self, id):
        if not isinstance(id, str):
            return pd.DataFrame()

        with sqlite3.connect(self.dbPathOrUrl) as con:
            query = "SELECT * FROM metadata WHERE id = ?"
            cursor = con.cursor()
            cursor.execute(query, (id,))
            metadata_result = cursor.fetchall()

            query = "SELECT * FROM annotations WHERE id = ?"
            cursor.execute(query, (id,))
            annotations_result = cursor.fetchall()

        metadata_df = pd.DataFrame(metadata_result, columns=["id", "title", "creator"])
        annotations_df = pd.DataFrame(
            annotations_result, columns=["id", "body", "target", "motivation"]
        )

        if not metadata_df.empty and not annotations_df.empty:
            result = pd.concat([metadata_df, annotations_df], ignore_index=True)
        elif not metadata_df.empty:
            result = metadata_df
        elif not annotations_df.empty:
            result = annotations_df
        else:
            result = pd.DataFrame()

        return result


class GenericQueryProcessor(QueryProcessor):
    query_processors = []

    def cleanQueryProcessors(self):
        success = True
        for processor in self.query_processors:
            if isinstance(
                processor, (RelationalQueryProcessor, TriplestoreQueryProcessor)
            ):
                try:
                    processor.connection.close()
                except Exception as e:
                    print("Operation is failed")
                    success = False
        return success

    def addQueryProcessor(self, query_processors):
        if isinstance(
            query_processors, (RelationalQueryProcessor, TriplestoreQueryProcessor)
        ):
            query_processors = [query_processors]

        for processor in query_processors:
            if not isinstance(
                processor, (RelationalQueryProcessor, TriplestoreQueryProcessor)
            ):
                raise ValueError("Query_processors are not from our model")

        self.query_processors.extend(query_processors)
        return True

    # by Evgeniia
    def getEntityById(self, entity_id):
        entity = None
        for processor in self.query_processors:
            data = processor.getEntityById(entity_id)
            if data is not None and not data.empty:
                if entity is None:
                    entity = data
                else:
                    entity = pd.concat([entity, data], axis=0)
                    entity = entity.drop_duplicates(subset=["id"])

                return entity

        return entity

    def getAllAnnotations(self):
        annotations = []

        for processor in self.query_processors:
            try:
                annotations_data = processor.getAllAnnotations()
                if not annotations_data.empty:
                    annotations.extend(
                        [
                            Annotation(
                                id=annotation["id"],
                                body=Image(id=annotation["body"]),
                                target=IdentifiableEntity(id=annotation["target"]),
                                motivation=annotation["motivation"],
                            )
                            for _, annotation in annotations_data.iterrows()
                        ]
                    )
            except AttributeError:
                continue

        return annotations

    def getAllCanvas(self):
        canvases = []

        for processor in self.query_processors:
            try:
                canvases_data = processor.getAllCanvases()
                if not canvases_data.empty:
                    canvases.extend(
                        [
                            Canvas(id=canvas["id"], label=canvas["label"])
                            for _, canvas in canvases_data.iterrows()
                        ]
                    )
            except AttributeError:
                continue

        return canvases

    def getAllImages(self):
        images = []

        for processor in self.query_processors:
            try:
                images_data = processor.getAllImages()
                if not images_data.empty:
                    images.extend(
                        [Image(id=image["body"]) for _, image in images_data.iterrows()]
                    )
            except AttributeError:
                continue

        return images
    
    # Done by Javad

    def getAnnotationsToCanvas(self, canvasId: str) -> list:
        annotations = []

        for processor in self.query_processors:
            try:
                annotations_data = processor.getAnnotationsWithTarget(canvasId)
                for idx, row in annotations_data.iterrows():
                    annotations.append(
                        Annotation(
                            row["id"], row["body"], row["target"], row["motivation"]
                        )
                    )
            except Exception as e:
                continue

        return annotations

    def getAnnotationsToCollection(self, collectionId: str) -> list:
        annotations = []

        for processor in self.query_processors:
            try:
                annotations_data = processor.getAnnotationsWithTarget(collectionId)
                for idx, row in annotations_data.iterrows():
                    annotations.append(
                        Annotation(
                            row["id"], row["body"], row["target"], row["motivation"]
                        )
                    )
            except Exception as e:
                continue

        return annotations

    def getAnnotationsToManifest(self, manifestId: str) -> list:
        annotations = []

        for processor in self.query_processors:
            try:
                annotations_data = processor.getAnnotationsWithTarget(manifestId)
                for idx, row in annotations_data.iterrows():
                    annotations.append(
                        Annotation(
                            row["id"], row["body"], row["target"], row["motivation"]
                        )
                    )
            except Exception as e:
                continue

        return annotations

    def getAnnotationsWithBody(self, bodyId: str) -> list:
        annotations = []

        for processor in self.query_processors:
            try:
                annotations_data = processor.getAnnotationsWithBody(bodyId)
                for idx, row in annotations_data.iterrows():
                    annotations.append(
                        Annotation(
                            row["id"], row["body"], row["target"], row["motivation"]
                        )
                    )
            except Exception as e:
                continue

        return annotations

    def getAnnotationsWithBodyAndTarget(self, bodyId: str, targetId: str) -> list:
        annotations = []

        for processor in self.query_processors:
            try:
                annotations_data = processor.getAnnotationsWithBodyAndTarget(
                    bodyId, targetId
                )
                for idx, row in annotations_data.iterrows():
                    annotations.append(
                        Annotation(
                            row["id"], row["body"], row["target"], row["motivation"]
                        )
                    )
            except Exception as e:
                continue

        return annotations

    def getAnnotationsWithTarget(self, targetId: str) -> list:
        annotations = []

        for processor in self.query_processors:
            try:
                annotations_data = processor.getAnnotationsWithTarget(targetId)
                for idx, row in annotations_data.iterrows():
                    annotations.append(
                        Annotation(
                            row["id"], row["body"], row["target"], row["motivation"]
                        )
                    )
            except Exception as e:
                continue

        return annotations

    # by Evgeniia
    def getCanvasesInCollection(self, collection_id):
        canvases = []

        for processor in self.query_processors:
            if hasattr(processor, "getCanvasesInCollection"):
                try:
                    canvases_data = processor.getCanvasesInCollection(collection_id)
                    if not canvases_data.empty:
                        canvases.extend(
                            [
                                Canvas(id=canvas["id"], label=canvas.get("label"))
                                for _, canvas in canvases_data.iterrows()
                            ]
                        )
                except AttributeError:
                    continue

        return canvases

    def getCanvasesInManifest(self, manifest_id):
        canvases = []

        for processor in self.query_processors:
            if hasattr(processor, "getCanvasesInManifest"):
                try:
                    canvases_data = processor.getCanvasesInManifest(manifest_id)
                    if not canvases_data.empty:
                        canvases.extend(
                            [
                                Canvas(
                                    id=canvas["id"],
                                    label=canvas.get("label"),
                                )
                                for _, canvas in canvases_data.iterrows()
                            ]
                        )
                except AttributeError:
                    continue

        return canvases

    def getAllManifests(self):
        manifests = []
        relational_processor = None

        for processor in self.query_processors:
            try:
                manifests_data = processor.getAllManifests()
                if not manifests_data.empty:
                    manifests.extend(
                        [
                            Manifest(
                                id=manifest["id"],
                                label=manifest.get("label"),
                                title=relational_processor.getEntityById(
                                    manifest["id"]
                                ).loc[0, "title"],
                                creator=relational_processor.getEntityById(
                                    manifest["id"]
                                ).loc[0, "creator"],
                                items=self.getCanvasesInManifest(manifest["id"]),
                            )
                            for _, manifest in manifests_data.iterrows()
                        ]
                    )
            except AttributeError:
                relational_processor = processor
                continue

        return manifests

    def getEntitiesWithCreator(self, creator_name):
        entities = []
        seen_entity_ids = set()  
        triple_processor = None

        for processor in self.query_processors:
            if "getEntitiesWithCreator" in dir(processor):
                entities_data = processor.getEntitiesWithCreator(creator_name)
                for index, entity_row in entities_data.iterrows():
                    entity_id = entity_row["id"]
                    if entity_id not in seen_entity_ids:
                        seen_entity_ids.add(entity_id)
                        entity_title = entity_row["title"]
                        entity_creator = entity_row["creator"]
                        triple_qp = None 
                        entity_label = None

                        if triple_qp is not None:
                            entity_info = triple_processor.getEntityById(entity_id)
                            if entity_info is not None:
                                entity_label = entity_info.get("label")

                        entity_object = EntityWithMetadata(
                            id=entity_id,
                            label=entity_label,
                            title=entity_title,
                            creator=entity_creator,
                        )
                        entities.append(entity_object)

        return entities

    def getEntitiesWithLabel(self, label):
        entities = []
        seen_entity_ids = set()
        relational_processor = None

        for processor in self.query_processors:
            if "getEntitiesWithLabel" in dir(processor):
                entities_data = processor.getEntitiesWithLabel(label)
                for index, entity_row in entities_data.iterrows():
                    entity_id = entity_row["id"]
                    if entity_id not in seen_entity_ids:
                        seen_entity_ids.add(entity_id)
                        entity_label = entity_row["label"]
                        entity_title = None
                        entity_creator = None

                    if relational_processor is not None:
                        entity_info = relational_processor.getEntityById(entity_id)
                        if entity_info is not None:
                            entity_creator = entity_info.get("creator")
                            entity_title = entity_info.get("title")

                    entity_object = EntityWithMetadata(
                        id=entity_id,
                        label=entity_label,
                        title=entity_title,
                        creator=entity_creator,
                    )
                    entities.append(entity_object)

        return entities

    def getEntitiesWithTitle(self, title):
        entities = []
        seen_entity_ids = set() 
        triple_processor = None

        for processor in self.query_processors:
            if "getEntitiesWithTitle" in dir(processor):
                entities_data = processor.getEntitiesWithTitle(title)
                for index, entity_row in entities_data.iterrows():
                    entity_id = entity_row["id"]
                    if entity_id not in seen_entity_ids:
                        seen_entity_ids.add(entity_id)
                        entity_title = entity_row["title"]
                        entity_creator = entity_row["creator"]
                        triple_qp = None
                        entity_label = None

                        if triple_qp is not None:
                            entity_info = triple_processor.getEntityById(entity_id)
                            if entity_info is not None:
                                entity_label = entity_info.get("label")

                        entity_object = EntityWithMetadata(
                            id=entity_id,
                            label=entity_label,
                            title=entity_title,
                            creator=entity_creator,
                        )
                        entities.append(entity_object)

        return entities

    def getImagesAnnotatingCanvas(self, canvas_id):
        images = []

        for processor in self.query_processors:
            if hasattr(processor, "getAnnotationsWithTarget"):
                annotations_data = processor.getAnnotationsWithTarget(canvas_id)
                if not annotations_data.empty:
                    images.extend(
                        [
                            Image(id=annotation["body"])
                            for _, annotation in annotations_data.iterrows()
                        ]
                    )

        return images

    def getManifestsInCollection(self, collection_id):
        manifests = []

        triple_processor = None
        relational_processor = None

        for processor in self.query_processors:
            if isinstance(processor, TriplestoreQueryProcessor):
                triple_processor = processor
            elif isinstance(processor, RelationalQueryProcessor):
                relational_processor = processor

        if triple_processor is None or relational_processor is None:
            return [] 

        triple_manifest = triple_processor.getManifestsInCollection(collection_id)

        for _, manifest in triple_manifest.iterrows():
            manifest_id = manifest["id"]
            label = manifest.get("label")
            items = (self.getCanvasesInManifest(manifest["id"]),)
            title = None
            creator = None

            entity_data = relational_processor.getEntityById(manifest_id)
            if not entity_data.empty:
                title = entity_data.loc[0, "title"]
                creator = entity_data.loc[0, "creator"]

            manifests.append(
                Manifest(
                    id=manifest_id,
                    label=label,
                    title=title,
                    creator=creator,
                    items=items,
                )
            )

        return manifests

    def getAllCollections(self):
        collections = []

        triple_processor = None
        relational_processor = None

        for processor in self.query_processors:
            if isinstance(processor, TriplestoreQueryProcessor):
                triple_processor = processor
            elif isinstance(processor, RelationalQueryProcessor):
                relational_processor = processor

        if triple_processor is None or relational_processor is None:
            return []

        triple_collections = triple_processor.getAllCollections()

        for _, collection in triple_collections.iterrows():
            collection_id = collection["id"]
            label = collection.get("label")
            title = None
            creator = None

            entity_data = relational_processor.getEntityById(collection_id)
            if not entity_data.empty:
                title = entity_data.loc[0, "title"]
                creator = entity_data.loc[0, "creator"]

            items = self.getManifestsInCollection(collection_id)

            collections.append(
                Collection(
                    id=collection_id,
                    label=label,
                    title=title,
                    creator=creator,
                    items=items,
                )
            )

        return collections

    # extra methods: by Evan & Javad
    def getCollectionsContainingCanvases(
        self, canvases: list[Canvas]
    ) -> list[Collection]:
        """
        It returns a list of objects having class Collection, included in the databases accessible
        via the query processor, that contain (indirectly, via the related manifests) any of the
        canvases specified as input.
        """
        collections = []
        for processor in self.query_processors:
            try:
                collections_data = processor.getCollectionsContainingCanvases(canvases)
                for idx, row in collections_data.iterrows():
                    collections.append(
                        Collection(
                            row["id"],
                            row["label"],
                            row["title"],
                            row["creator"],
                            row["items"],
                        )
                    )
            except Exception as e:
                continue

        return collections

    def getManifestContainingCanvases(self, canvases: list[Canvas]) -> list[Manifest]:
        """
        It returns a list of objects having class Manifest, included in the databases
        accessible via the query processor, that contain any of the canvases specified as input.
        """
        manifests = []
        for processor in self.query_processors:
            try:
                manifests_data = processor.getManifestContainingCanvases(canvases)
                for idx, row in manifests_data.iterrows():
                    manifests.append(
                        Manifest(
                            row["id"],
                            row["label"],
                            row["title"],
                            row["creator"],
                            row["items"],
                        )
                    )
            except Exception as e:
                continue

        return manifests

    def getAnnotationsToImage(self, imageId: str) -> list[Annotation]:
        """
        It returns a list of objects having class Annotation, included in
        the databases accessible via the query processors, that have, as
        annotation target, the canvas specified by the input identifier.
        """
        annotations = []
        for processor in self.query_processors:
            try:
                annotations_data = processor.getAnnotationsWithTarget(imageId)
                for idx, row in annotations_data.iterrows():
                    annotations.append(
                        Annotation(
                            row["id"], row["body"], row["target"], row["motivation"]
                        )
                    )
            except Exception as e:
                continue

        return annotations

    def getAnnotationsToAnnotation(self, annotationId: str) -> list[Annotation]:
        """
        It returns a list of objects having class Annotation, included in the databases accessible
        via the query processors, that have, as annotation target, the canvas specified by the input identifier.
        """
        annotations = []
        for processor in self.query_processors:
            try:
                annotations_data = processor.getAnnotationsWithTarget(annotationId)
                for idx, row in annotations_data.iterrows():
                    annotations.append(
                        Annotation(
                            row["id"], row["body"], row["target"], row["motivation"]
                        )
                    )
            except Exception as e:
                continue

        return annotations

    def getEntityByType(self, entity_type: str) -> list[IdentifiableEntity]:
        """
        It returns a list of objects having class the type identified by the input string.
        The possible values of the input string are "annotation", "image", "collection", "manifest", "canvas”.
        """
        entities = []
        for processor in self.query_processors:
            try:
                entites_data = processor.getEntitiesWithType(entity_type)
                for idx, row in entites_data.iterrows():
                    entities.append(
                        Annotation(
                            row["id"], row["body"], row["target"], row["motivation"]
                        )
                    )
            except Exception as e:
                continue

        return entities
