import pandas as pd
import rdflib
from models.main_models import *  # import the entirety of main_models
from utils import upload_to_db, create_graph
import sqlite3
from sqlite3 import connect
from pandas import read_sql, concat
from sparql_dataframe import get
from json import load
from rdflib import Graph, URIRef  # for loading rdflibrary, used in CollectionProcessor
from rdflib.plugins.stores.sparqlstore import \
    SPARQLUpdateStore  # for using rdflib plugin for sparql store update function
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
        metadata = pd.read_csv(path,
                               keep_default_na=False,
                               dtype={
                                   'id': 'string',
                                   'title': 'string',
                                   'creator': 'string'})
        return upload_to_db(self.dbPathOrUrl, metadata, "Metadata")


# Done by Evan
class CollectionProcessor(Processor):
    def __init__(self):
        super().__init__()

    def uploadData(self, path: str):
        try:
            base_url = "https://github.com/mjavadf/rumi_group_project/"  # "D:/Projects/rumi_group_project"
            new_graph = Graph()

            with open(path, mode='r', encoding="utf-8") as j:
                json_obj = load(j)

            # this check the loaded json file
            if type(json_obj) is list:
                for collection in json_obj:
                    create_graph(collection, base_url, new_graph)
            else:
                create_graph(json_obj, base_url, new_graph)

            # this uses SPARQLUpdateStore to store the triples
            store = SPARQLUpdateStore()
            endpoint = self.getDbPathOrUrl()
            store.open((endpoint, endpoint))
            for triple in new_graph.triples((None, None, None)):
                store.add(triple)
            store.close()
            # delete below comment in case we want to visualize a turtle file from the Collections
            # new_graph.serialize(destination="Turtle_Visualization.ttl", format="turtle")
            return True
        # error check
        except Exception as e:
            print(f"Upload failed: {str(e)}")
            return False


class AnnotationProcessor(Processor):

    def uploadData(self, path: str) -> bool:
        annotations = pd.read_csv(path,
                                  keep_default_na=False,
                                  dtype={
                                      'id': 'string',
                                      'body': 'string',
                                      'target': 'string',
                                      'motivation': 'string'
                                  })
        return upload_to_db(self.dbPathOrUrl, annotations, "Annotations")


class QueryProcessor(Processor):
    def getEntityById(self, entityId: str) -> pd.DataFrame:
        """
        it returns a data frame with all the entities matching the input identifier (i.e. maximum one entity).
        """
        endpoint = self.getDbPathOrUrl()
        query = """
            PREFIX schema: <https://schema.org/>
            PREFIX rumi: <https://github.com/mjavadf/rumi_group_project/>
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            
            Select *
                Where {
                ?entity schema:identifier "{}".
                ?entity rdfs:label ?label.
                ?entity rdf:type ?type. }
        """.format(entityId)

        df_sparql = get(endpoint, query, True)
        return df_sparql


# Have done by Thomas

class TriplestoreQueryProcessor:
    def __init__(self):
        super().__init__()

    def getAllCanvases(self):

        endpoint = self.getDbPathOrUrl()
        query_Canvas = """
        PREFIX schema: <https://schema.org/>
        PREFIX rumi: <https://github.com/mjavadf/rumi_group_project/>
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT ?canvas ?id ?label
        WHERE{
        ?canvas a rumi:Canvas;
        schema:identifier ?id;
        rdfs:label ?label.
         }"""
        df_sparql_getAllCanvases = get(endpoint, query_Canvas,True)
        return df_sparql_getAllCanvases

    def getAllCollections(self):

        endpoint = self.getDbPathOrUrl()
        query_Collection = """
        PREFIX schema: <https://schema.org/>
        PREFIX rumi: <https://github.com/mjavadf/rumi_group_project/>
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
             
        SELECT ?collection ?id ?label
        WHERE{
        ?collection a rumi:Collection;
        schema:identifier ?id;
        rdfs:label ?label.
         }"""
        df_sparql_getAllCollections = get(endpoint, query_Collection,True)
        return df_sparql_getAllCollections

    def getAllManifests(self):

        endpoint = self.getDbPathOrUrl()
        query_Manifest = """
        PREFIX schema: <https://schema.org/>
        PREFIX rumi: <https://github.com/mjavadf/rumi_group_project/>
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT ?manifest ?id ?label
        WHERE{
        ?manifest a rumi:Manifest;
        schema:identifier ?id;
        rdfs:label ?label.
         }"""
        df_sparql_getAllManifests = get(endpoint, query_Manifest,True)
        return df_sparql_getAllManifests

    def getCanvasesInCollection(self, collectionId: str):

        endpoint = self.getDbPathOrUrl()
        query_CanvasInCollection = """
        PREFIX schema: <https://schema.org/>
        PREFIX rumi: <https://github.com/mjavadf/rumi_group_project/>
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT ?canvas ?id ?label
        WHERE{
        ?collection a rumi:Collection;
        schema: identifier "%s";
        rdf:items ?manifest .
        ?manifest a rumi:Manifest;
        rdf:items ?canvas . 
        ?canvas a rumi:Canvas;
        schema:identifier ?id;
        rdfs:label ?label .
         }
         """
        df_sparql_CanvasesInCollection = get(endpoint, query_CanvasInCollection, True)
        return df_sparql_CanvasesInCollection

    def getCanvasesinManifest(self,):
        endpoint = self.getDbPathOrUrl()
        query_CanvasInManifest = """
        PREFIX schema: <https://schema.org/>
        PREFIX rumi: <https://github.com/mjavadf/rumi_group_project/>
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT ?canvas ?id ?label
        WHERE{
        ?manifest a rumi:Manifest;
        rdf:items ?canvas .
        ?canvas a rumi:Canvas;
        schema:identifier ?id;
        rdfs:label ?label .
        }"""
        df_sparql_CanvasesInManifest = get(endpoint, query_CanvasInManifest, True)
        return df_sparql_CanvasesInManifest

    def getEntitiesWithLabel(self, label):
        endpoint = self.getDbPathOrUrl()
        query_EntitiesWLabel = """
        PREFIX schema: <https://schema.org/>
        PREFIX rumi: <https://github.com/mjavadf/rumi_group_project/>
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT ?entity ?type ?label ?id
        WHERE {
            ?entity refs:label "%s" ;
            a ?type ;
            rdfs: label ?label ;
            schema: identifier ?id
            }
             """ % remove_special_chars(label)

        df_sparql_getEntitiesWLabel= get(endpoint, query_EntitiesWLabel, True)
        return df_sparql_getEntitiesWLabel

    def getManifestsInCollection(self):
        endpoint = self.getDbPathOrUrl()
        query_ManifestsInCollection = """
        PREFIX schema: <https://schema.org/>
        PREFIX rumi: <https://github.com/mjavadf/rumi_group_project/>
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT ?manifest ?id ?label
        WHERE{
        ?collection a rumi:Collection;
        schema: identifier "%s";
        rdf:items ?manifest .
        ?manifest a rumi:Manifest;
        rdf:identifier ?id;
        rdfs:label ?label.
        }"""
        df_sparql_ManifestsInCollection = get(endpoint, query_ManifestsInCollection, True)
        return df_sparql_ManifestsInCollection

# by Evgeniia
r_path = 'relational.db'

an = AnnotationProcessor()
an.setDbPathOrUrl(r_path)
an.uploadData('data/annotations.csv')

met = MetadataProcessor()
met.setDbPathOrUrl(r_path)
met.uploadData('data/metadata.csv')


class RelationalQueryProcessor(QueryProcessor):
    def __init__(self, r_path):
        super().__init__()
        self.db_path = r_path
        self.connection = sqlite3.connect(r_path)

    def getAllAnnotations(self):
        query = "SELECT * FROM annotations"
        result = pd.read_sql(query, self.connection)
        return result

    def getAllImages(self):
        query = "SELECT * FROM annotations WHERE body LIKE '%.jpg' OR body LIKE '%.jpeg' OR body LIKE '%.png'"
        result = pd.read_sql(query, self.connection)
        return result

    def getAnnotationsWithBody(self, body):
        cursor = self.connection.cursor()
        query = "SELECT * FROM annotations WHERE body = ?"
        cursor.execute(query, (body,))
        result = pd.read_sql(query, self.connection, params=(body,))
        return result

    def getAnnotationsWithBodyAndTarget(self, body, target):
        cursor = self.connection.cursor()
        query = "SELECT * FROM annotations WHERE body = ? AND target = ?"
        cursor.execute(query, (body, target,))
        result = pd.read_sql(query, self.connection, params=(body, target,))
        return result

    def getAnnotationsWithTarget(self, target):
        cursor = self.connection.cursor()
        query = "SELECT * FROM annotations WHERE target = ?"
        cursor.execute(query, (target,))
        result = pd.read_sql(query, self.connection, params=(target,))
        return result

    def getEntitiesWithCreator(self, creator):
        cursor = self.connection.cursor()
        query = "SELECT * FROM metadata WHERE creator = ?"
        cursor.execute(query, (creator,))
        result = pd.read_sql(query, self.connection, params=(creator,))
        return result

    def getEntitiesWithTitle(self, title):
        cursor = self.connection.cursor()
        query = "SELECT * FROM metadata WHERE title = ?"
        cursor.execute(query, (title,))
        result = pd.read_sql(query, self.connection, params=(title,))
        return result


# testing for Relational Query Processor
rel = RelationalQueryProcessor(r_path)


# print(rel.getEntitiesWithTitle('Il Canzoniere'))

# in progress by Evgeniia=================================================

# union_data = concat([r_path, rdf_file_path], ignore_index=True)
# union_no_duplicates = union_data.drop_duplicates(subset=["id"])
# need to check final database


class GenericQueryProcessor(QueryProcessor):
    def __init__(self, final_data, query_processors):
        super().__init__()
        if not all(isinstance(processor, (RelationalQueryProcessor, TriplestoreQueryProcessor)) for processor in
                   query_processors):
            raise ValueError("Query_processors are not from our model")

        self.queryProcessors = query_processors
        self.db_path = final_data

    def cleanQueryProcessors(self):
        success = True
        for processor in self.queryProcessors:
            if isinstance(processor, (RelationalQueryProcessor, TriplestoreQueryProcessor)):
                try:
                    processor.connection.close()
                except Exception as e:
                    print("Operation is failed")
                    success = False
        return success

    def addQueryProcessor(self, query_processors):
        for processor in query_processors:
            if not isinstance(processor, (RelationalQueryProcessor, TriplestoreQueryProcessor)):
                return False

        self.queryProcessors.extend(query_processors)
        return True

    def getAllAnnotations(self) -> list:
        pass

    def getAllCanvas(self) -> list:
        pass

    def getAllCollections(self) -> list:
        pass

    def getAllManifests(self) -> list:
        pass

    def getAllImages(self) -> list:
        pass

    # ===============

    def getAnnotationsToCanvas(self, canvasId: str) -> list:
        with connect(self.db_path) as conn:
            annotations = []
            query = f"SELECT * FROM annotations WHERE target = {canvasId}"
            annotations_df = pd.read_sql(query, conn)
            for idx, row in annotations_df.iterrows():
                annotations.append(Annotation(row['id'], row['body'], row['target'], row['motivation']))

        return annotations

    def getAnnotationsToCollection(self, collectionId: str) -> list:
        with connect(self.db_path) as conn:
            annotations = []
            query = f"SELECT * FROM annotations WHERE target = {collectionId}"
            annotations_df = pd.read_sql(query, conn)
            for idx, row in annotations_df.iterrows():
                annotations.append(Annotation(row['id'], row['body'], row['target'], row['motivation']))

        return annotations

    def getAnnotationsToManifest(self, manifestId: str) -> list:
        with connect(self.db_path) as conn:
            annotations = []
            query = f"SELECT * FROM annotations WHERE target = {manifestId}"
            annotations_df = pd.read_sql(query, conn)
            for idx, row in annotations_df.iterrows():
                annotations.append(Annotation(row['id'], row['body'], row['target'], row['motivation']))

        return annotations

    def getAnnotationsWithBody(self, bodyId: str) -> list:
        with connect(self.db_path) as conn:
            annotations = []
            query = f"SELECT * FROM annotations WHERE body = {bodyId}"
            annotations_df = pd.read_sql(query, conn)
            for idx, row in annotations_df.iterrows():
                annotations.append(Annotation(row['id'], row['body'], row['target'], row['motivation']))

        return annotations

    def getAnnotationsWithBodyAndTarget(self, bodyId: str, targetId: str) -> list:
        with connect(self.db_path) as conn:
            annotations = []
            query = f"SELECT * FROM annotations WHERE body = {bodyId} AND target = {targetId}"
            annotations_df = pd.read_sql(query, conn)
            for idx, row in annotations_df.iterrows():
                annotations.append(Annotation(row['id'], row['body'], row['target'], row['motivation']))

        return annotations

    def getAnnotationsWithTarget(self, targetId: str) -> list:
        with connect(self.db_path) as conn:
            annotations = []
            query = f"SELECT * FROM annotations WHERE target = {targetId}"
            annotations_df = pd.read_sql(query, conn)
            for idx, row in annotations_df.iterrows():
                annotations.append(Annotation(row['id'], row['body'], row['target'], row['motivation']))

        return annotations

    def getCanvasesInCollection(self, collectionId: str) -> list:
        pass

    def getCanvasesInManifest(self, manifestId: str) -> list:
        pass

    def getEntityById(self, entityId: str) -> IdentifiableEntity | None:
        pass

    def getEntitiesWithCreator(self, creator_name: str) -> list:
        pass

    def getEntitiesWithLabel(self, label: str) -> list:
        pass

    def getEntitiesWithTitle(self, title: str) -> list:
        pass

    def getImagesAnnotatingCanvas(self, canvasId: str) -> list:
        pass

    def getManifestsInCollection(self, collectionId: str) -> list:
        pass

    def getCollectionsContainingCanvases(self, canvases: list[Canvas]) -> list[Collection]:
        """
        It returns a list of objects having class Collection, included in the databases accessible
        via the query processor, that contain (indirectly, via the related manifests) any of the
        canvases specified as input.
        """
        pass

    def getManifestContainingCanvases(self, canvases: list[Canvas]) -> list[Manifest]:
        """
        It returns a list of objects having class Manifest, included in the databases
        accessible via the query processor, that contain any of the canvases specified as input.
        """
        pass

    def getAnnotationsToImage(self, imageId: str) -> list[Annotation]:
        """
        It returns a list of objects having class Annotation, included in
        the databases accessible via the query processors, that have, as
        annotation target, the canvas specified by the input identifier.
        """
        pass

    def getAnnotationsToAnnotation(self, annotationId: str) -> list[Annotation]:
        """
        It returns a list of objects having class Annotation, included in the databases accessible
        via the query processors, that have, as annotation target, the canvas specified by the input identifier.
        """
        pass

    def getEntityByType(self, entity_type: str) -> list[IdentifiableEntity]:
        """
        It returns a list of objects having class the type identified by the input string.
        The possible values of the input string are "annotation", "image", "collection", "manifest", "canvas”.
        """
        pass
