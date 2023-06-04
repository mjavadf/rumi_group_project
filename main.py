import pandas as pd
from models.main_models import IdentifiableEntity
from utils import upload_to_db


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
class RelationalQueryProcessor(QueryProcessor):
    def getAllAnnotations(self) -> pd.DataFrame:
        pass

    def getAllImages(self) -> pd.DataFrame:
        pass

    def getAnnotationsWithBody(self, bodyId: str) -> pd.DataFrame:
        pass

    def getAnnotationsWithBodyAndTarget(self, bodyId: str, targetId: str) -> pd.DataFrame:
        pass

    def getAnnotationsWithTarget(self, targetId: str) -> pd.DataFrame:
        pass

    def getEntitiesWithCreator(self, creatorName: str) -> pd.DataFrame:
        pass

    def getEntitiesWithLabel(self, label: str) -> pd.DataFrame:
        pass

    def getEntitiesWithTitle(self, title: str) -> pd.DataFrame:
        pass


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
