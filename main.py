import pandas as pd
from models.main_models import IdentifiableEntity

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
        pass


class CollectionProcessor(Processor):

    def uploadData(self, path: str) -> bool:
        pass


class AnnotationProcessor(Processor):

    def uploadData(self, path: str) -> bool:
        pass


class QueryProcessor(Processor):

    def getEntityById(self, entityId: str) -> pd.DataFrame:
        pass


class TriplestoreQueryProcessor(QueryProcessor):
    def getAllCanvases(self) -> pd.DataFrame:
        pass

    def getAllImages(self) -> pd.DataFrame:
        pass

    def getAllManifests(self) -> pd.DataFrame:
        pass

    def getCanvasesInCollection(self, collectionId: str) -> pd.DataFrame:
        pass

    def getCanvasesInManifest(self, manifestId: str) -> pd.DataFrame:
        pass

    def getEntitiesWithLabel(self, label: str) -> pd.DataFrame:
        pass

    def getManifestsInCollection(self, collectionId: str) -> pd.DataFrame:
        pass


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
