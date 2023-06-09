import pandas as pd
from models.main_models import IdentifiableEntity
from utils import upload_to_db
import sqlite3
from sqlite3 import connect
from pandas import read_sql


# https://github.com/comp-data/2022-2023/tree/main/docs/project#uml-of-additional-classes


class Processor(object):
    def __init__(self):
        self.dbPathOrUrl = None

    def getDbPathOrUrl(self) -> str:
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl: str) -> None:
        self.dbPathOrUrl = pathOrUrl


class MetadataProcessor(Processor):

    def uploadData(self, path: str) -> bool:
        metadata = pd.read_csv(path,
                               keep_default_na=False,
                               dtype={
                                   'id': 'string',
                                   'title': 'string',
                                   'creator': 'string'})
        return upload_to_db(self.dbPathOrUrl, metadata, "Metadata")


class CollectionProcessor(Processor):

    def uploadData(self, path: str) -> bool:
        pass


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
        pass


# Have done by Thomas
class TriplestoreQueryProcessor:
    def __init__(self, rdf_file_path):
        self.g = Graph()
        self.g.parse(rdf_file_path, format="turtle")

    def getAllCanvases(self):
        query = prepareQuery('SELECT ?canvas WHERE {?canvas a <http://iiif.io/api/presentation/2#Canvas>}',
                             initNs={'iiif': 'http://iiif.io/api/presentation/3/context.json'})
        result = self.g.query(query)
        return self._rdfResultToDataFrame(result)

    def getAllCollections(self):
        query = prepareQuery('SELECT ?collection WHERE {?collection a <https://dl.ficlit.unibo.it/iiif/28429/collection>}',
                             initNs={'iiif': 'http://iiif.io/api/presentation/3/context.json'})
        result = self.g.query(query)
        return self._rdfResultToDataFrame(result)

    def getAllManifests(self):
        query = prepareQuery('SELECT ?manifest WHERE {?manifest a <https://dl.ficlit.unibo.it/iiif/2/28429/manifest>}',
                             initNs={'iiif': 'http://iiif.io/api/presentation/3/context.json'})
        result = self.g.query(query)
        return self._rdfResultToDataFrame(result)

    def getCanvasesInCollection(self, collection_id):
        query = prepareQuery(
            'SELECT ?canvas WHERE {?collection <https://dl.ficlit.unibo.it/iiif/28429/collection> ?canvas . ?collection <http://www.w3.org/2000/01/rdf-schema#label> ?label . FILTER(?collection = <' + collection_id + '>) .}',
            initNs={'iiif': 'http://iiif.io/api/presentation/3/context.json', 'rdfs': 'http://www.w3.org/2000/01/rdf-schema#'})
        result = self.g.query(query)
        return self._rdfResultToDataFrame(result)

    def getCanvasesInManifest(self, manifest_id):
        query = prepareQuery(
            'SELECT ?canvas WHERE {?manifest <http://iiif.io/api/presentation/2#contains> ?canvas . ?manifest <http://www.w3.org/2000/01/rdf-schema#label> ?label . FILTER(?manifest = <' + manifest_id + '>) .}',
            initNs={'iiif': 'http://iiif.io/api/presentation/3/context.json', 'rdfs': 'http://www.w3.org/2000/01/rdf-schema#'})
        result = self.g.query(query)
        return self._rdfResultToDataFrame(result)

    def getManifestsInCollection(self, collection_id):
        query = prepareQuery(
            'SELECT ?manifest WHERE {?collection <http://iiif.io/api/presentation/2#contains> ?manifest . FILTER(?collection = <' + collection_id + '>) .}',
            initNs={'iiif': 'http://iiif.io/api/presentation/3/context.json'})
        result = self.g.query(query)
        return self._rdfResultToDataFrame(result)

    def _rdfResultToDataFrame(self, result):
        df = pd.DataFrame(columns=result.vars)
        for row in result:
            df = df.append(pd.Series(list(row), index=result.vars), ignore_index=True)
        return df
    

#by Evgeniia   
r_path = 'relational.db'

an = AnnotationProcessor()
an.setDbPathOrUrl(r_path)
an.uploadData('data/annotations.csv')

met = MetadataProcessor()
met.setDbPathOrUrl(r_path)
met.uploadData('data/metadata.csv')

class RelationalQueryProcessor(QueryProcessor):
    def __init__(self, db_path):
        super().__init__()
        self.db_path = db_path
        self.connection = sqlite3.connect(db_path)

    def getAllAnnotations(self):
        query = "SELECT * 
                FROM annotations"
        result = pd.read_sql(query, self.connection)
        return result

    def getAllImages(self):
        query = "SELECT body 
                FROM annotations
                WHERE annotations.body LIKE '%.jpg' 
                OR annotations.body LIKE '%.jpeg' 
                OR annotations.body LIKE '%.png'"
        result = pd.read_sql(query, self.connection)
        return result

    def getAnnotationsWithBody(self, body_id):
        query = "SELECT * 
                FROM annotations 
                WHERE body = body_id"
        result = pd.read_sql(query, self.connection)
        return result

    def getAnnotationsWithBodyAndTarget(self, body_id, target_id):
        query = "SELECT * 
                FROM annotations 
                WHERE body = body_id
                AND target = target_id"
        result = pd.read_sql(query, self.connection)
        return result

    def getAnnotationsWithTarget(self, target_id):
        query = "SELECT * 
                FROM annotations 
                WHERE target = target_id"
        result = pd.read_sql(query, self.connection)
        return result

    def getEntitiesWithCreator(self, creator_name):
        query = "SELECT * 
                FROM annotations 
                WHERE creator = creator_name"
        result = pd.read_sql(query, self.connection)
        return result

    def getEntitiesWithTitle(self, title_id):
        query = "SELECT * 
                FROM annotations 
                WHERE title = title_id"
        result = pd.read_sql(query, self.connection)
        return result

#=================================================

class GenericQueryProcessor(object):
    def __init__(self, queryProcessors: QueryProcessor) -> None:
        self.queryProcessors = queryProcessors

    def cleanQueryProcessors(self) -> bool:
        pass

    def addQueryProcessor(self, processor: QueryProcessor) -> bool:
        pass

    def getAllAnnotations(self) -> list:
        pass

    def getAllCanvas(self) -> list:
        pass

    def getAllCollections(self) -> list:
        pass

    def getAllImages(self) -> list:
        pass

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

    def getEntityById(self, entityId: str) -> IdentifiableEntity:
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
