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
        endpint = self.getDbPathOrUrl()
        query = '''
        select * where {
            {} ?predicate ?object .
        '''.format(entityId)

        df_sparql = get(endpint, query, True)
        return df_sparql



# Have done by Thomas

class TriplestoreQueryProcessor:
    def __init__(self, rdf_file_path):
        self.g = Graph()
        self.g.parse(rdf_file_path, format="turtle")

    def getAllCanvases(self):
        query = prepareQuery(
            'SELECT ?canvas WHERE {?canvas a <https://dl.ficlit.unibo.it/iiif/2/28429/canvas> . FILTER regex(str(?canvas), "^https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p[0-9]+$")}',
            initNs={'iiif': 'http://iiif.io/api/presentation/3/context.json'})
        result = self.g.query(query)
        df = pd.DataFrame(columns=result.vars)
        for row in result:
            df = df.append(pd.Series(list(row), index=result.vars), ignore_index=True)
        return df

    def getAllCollections(self):
        query = prepareQuery(
            'SELECT ?collection WHERE {?collection a <https://dl.ficlit.unibo.it/iiif/28429/collection>}',
            initNs={'iiif': 'http://iiif.io/api/presentation/3/context.json'})
        result = self.g.query(query)
        df = pd.DataFrame(columns=result.vars)
        for row in result:
            df = df.append(pd.Series(list(row), index=result.vars), ignore_index=True)
        return df

    def getAllManifests(self):
        query = prepareQuery(
            'SELECT ?manifest WHERE {?manifest a <http://iiif.io/api/presentation/3/context.json#Manifest>}')
        result = self.g.query(query)
        df = pd.DataFrame(columns=result.vars)
        for row in result:
            df = df.append(pd.Series(list(row), index=result.vars), ignore_index=True)
        return df

    def getCanvasesInCollection(self, collection_id):
        query = prepareQuery(
            'SELECT ?canvas WHERE {?collection <https://dl.ficlit.unibo.it/iiif/28429/collection> ?canvas . ?collection <http://www.w3.org/2000/01/rdf-schema#label> ?label . FILTER(?collection = <' + collection_id + '>) .}',
            initNs={'iiif': 'http://iiif.io/api/presentation/3/context.json',
                    'rdfs': 'http://www.w3.org/2000/01/rdf-schema#'})
        result = self.g.query(query)
        df = pd.DataFrame(columns=result.vars)
        for row in result:
            df = df.append(pd.Series(list(row), index=result.vars), ignore_index=True)
        return df

    def getCanvasesInManifest(self, manifest_id):
        query = prepareQuery(
            'SELECT ?canvas WHERE {?manifest <http://iiif.io/api/presentation/2#contains> ?canvas . ?manifest <http://www.w3.org/2000/01/rdf-schema#label> ?label . FILTER(?manifest = <' + manifest_id + '>) .}',
            initNs={'iiif': 'http://iiif.io/api/presentation/3/context.json',
                    'rdfs': 'http://www.w3.org/2000/01/rdf-schema#'})
        result = self.g.query(query)
        df = pd.DataFrame(columns=result.vars)
        for row in result:
            df = df.append(pd.Series(list(row), index=result.vars), ignore_index=True)
        return df

    def getManifestsInCollection(self, collection_id):
        query = prepareQuery(
            'SELECT ?manifest WHERE {?collection <http://iiif.io/api/presentation/2#contains> ?manifest . FILTER(?collection = <' + collection_id + '>) }',
            initNs={'iiif': 'http://iiif.io/api/presentation/3/context.json'})
        result = self.g.query(query)
        return self._refResultToDataFrame(result)

    def _rdfResultToDataFrame(self, result):
        df = pd.DataFrame(columns=result.vars)
        for row in result:
            df = df.append(pd.Series(list(row), index=result.vars), ignore_index=True)
        return df


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

    def getAllImages(self) -> list:
        pass

    # ===============

    def getAllManifests(self) -> list:
        pass

    def getAnnotationsToCanvas(self, canvasId: str) -> list:
        pass

    def getAnnotationsToCollection(self, collectionId: str) -> list:
        pass

    def getAnnotationsToManifest(self, manifestId: str) -> list:
        pass

    def getAnnotationsWithBody(self, bodyId: str) -> list:
        pass

    def getAnnotationsWithBodyAndTarget(self, bodyId: str, targetId: str) -> list:
        pass

    def getAnnotationsWithTarget(self, targetId: str) -> list:
        pass

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

#
# gen = GenericQueryProcessor()
# print(gen.addQueryProcessor(RelationalQueryProcessor))
