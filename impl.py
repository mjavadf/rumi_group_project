import pandas as pd
import rdflib
from models.main_models import *  # import the entirety of main_models
import sqlite3
from sqlite3 import connect
from pandas import read_sql, concat
from sparql_dataframe import get
from json import load
from rdflib import Graph, URIRef, Literal, RDF, RDFS  # for loading rdflibrary, used in CollectionProcessor
from rdflib.plugins.stores.sparqlstore import \
    SPARQLUpdateStore  # for using rdflib plugin for sparql store update function


# https://github.com/comp-data/2022-2023/tree/main/docs/project#uml-of-additional-classes

# helper functions 
def upload_to_db(db_path, df: pd.DataFrame, name):
    try:
        with connect(db_path) as con:
            df.to_sql(name, con, if_exists='append', index=False)
        return True
    except Exception as e:
        print(f"Upload failed: {str(e)}")
        return False


def remove_invalid_char(string: str):
    #housekeeping - remove invalid chars found in the imported lists
    if '\"' in string:
        return string.replace('\"', '\\\"')
    elif '"' in string:
        return string.replace('"', '\\\"')
    else:
        return string

def create_graph(json_obj: dict, base_url: str, graph: Graph):
    #create collection id to collect json file
    collection_id = URIRef(json_obj['id'])
    #define URIs types
    type_canvas = URIRef(base_url + 'Canvas')
    type_collection = URIRef(base_url + 'Collection')
    type_manifest = URIRef(base_url + 'Manifest')
    type_metadata = URIRef(base_url + 'Metadata')
    #define URIs properties
    prop_id = URIRef('https://schema.org/identifier')
    prop_part = URIRef('https://www.w3.org/2002/07/owl#hasPart')
    prop_label = URIRef(base_url + 'label')
    prop_items = URIRef(base_url + 'items')

    # labeling json_obj
    label_list = list(json_obj['label'].values())
    label_value = label_list[0][0]
    label_value = remove_invalid_char(str(label_value))

    # create graph triples from the collection
    graph.add((collection_id, prop_id, Literal(json_obj['id'])))
    graph.add((collection_id, RDF.type, type_collection))
    graph.add((collection_id, RDFS.label, Literal(str(label_value))))

    # populate manifest
    for manifest in json_obj['items']:
        manifest_id = URIRef(manifest['id'])

        graph.add((collection_id, prop_items, manifest_id))
        graph.add((manifest_id, prop_id, Literal(manifest['id'])))
        graph.add((manifest_id, RDF.type, type_manifest))
        

        manifest_label_list = list(manifest['label'].values())
        manifest_label_value = manifest_label_list[0][0]
        manifest_label_value = remove_invalid_char(str(manifest_label_value))

        graph.add((manifest_id, RDFS.label, Literal(str(manifest_label_value))))

    #populate canvas
        for canvas in manifest['items']:
            canvas_id = URIRef(canvas['id'])

            graph.add((manifest_id, prop_items, canvas_id))
            graph.add((canvas_id, prop_id, Literal(canvas['id'])))
            graph.add((canvas_id, RDF.type, type_canvas))

            canvas_label_list = list(canvas['label'].values())
            canvas_label_value = canvas_label_list[0][0]
            canvas_label_value = remove_invalid_char(str(canvas_label_value))

            graph.add((canvas_id, RDFS.label, Literal(str(canvas_label_value))))

class Processor(object):
    """
    The base class for the processors. The variable path_url containing the path 
    or the URL of the database,initially set as an empty string, 
    that will be updated with the method setDbPathOrUrl
    """

    def __init__(self):
        self.dbPathOrUrl = None

    def getDbPathOrUrl(self) -> str:
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl: str) -> bool:
        self.dbPathOrUrl = pathOrUrl
        return True


class QueryProcessor(Processor):
    """
    It returns a data frame with all the entities matching the input identifier 
    (i.e. maximum one entity)
    """
    
    def getEntityById():
        pass


#=======RELATIONAL DATADASE=========


class MetadataProcessor(Processor):
    """
    It takes in input the path of a CSV file containing metadata 
    and uploads them in the database.
    This method can be called everytime there is a need 
    to upload metadata in the database
    """

    def uploadData(self, path: str) -> bool:
        metadata = pd.read_csv(
            path,
            keep_default_na=False,
            dtype={"id": "string", "title": "string", "creator": "string"},
        )
        return upload_to_db(self.dbPathOrUrl, metadata, "Metadata")


class AnnotationProcessor(Processor):
    """
    It takes in input the path of a CSV file containing annotations 
    and uploads them in the database.
    This method can be called everytime there is a need 
    to upload annotations in the database
    """

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


# Done by Evgeniia
class RelationalQueryProcessor(QueryProcessor):
    
    def getAllAnnotations(self):
        """
        It returns a data frame containing all the annotations 
        included in the database
        """

        with sqlite3.connect(self.dbPathOrUrl) as con:
            query = "SELECT * FROM annotations"
            result = pd.read_sql(query, con)
        return result
    
    
    def getAllImages(self):
        """
        It returns a data frame containing all the images 
        included in the database
        """

        with sqlite3.connect(self.dbPathOrUrl) as con:
            query = "SELECT body FROM annotations"
            result = pd.read_sql(query, con)
        return result
    
    
    def getAnnotationsWithBody(self, body):
        """
        It returns a data frame containing all the annotations 
        included in the database that have, as annotation body, 
        the entity specified by the input identifier
        """

        with sqlite3.connect(self.dbPathOrUrl) as con:
            query = "SELECT * FROM annotations WHERE body = ?"
            result = pd.read_sql(query, con, params=(body,))
        return result

    
    def getAnnotationsWithBodyAndTarget(self, body, target):
        """
        It returns a data frame containing all the annotations 
        included in the database that have, as annotation body and annotation target,
        the entities specified by the input identifiers
        """

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
        """
        It returns a data frame containing all the annotations 
        included in the database that have, as annotation target, 
        the entity specified by the input identifier
        """

        with sqlite3.connect(self.dbPathOrUrl) as con:
            query = "SELECT * FROM annotations WHERE target = ?"
            result = pd.read_sql(query, con, params=(target,))
        return result

    
    def getEntitiesWithCreator(self, creator):
        """
        It returns a data frame containing all the metadata included in the database
        related to the entities having the input creator as one of their creators
        """

        with sqlite3.connect(self.dbPathOrUrl) as con:
            query = "SELECT DISTINCT id, title, creator FROM metadata WHERE creator = ?"
            result = pd.read_sql(query, con, params=(creator,))
        return result


    def getEntitiesWithType(self, type):
        """
        It returns a list of objects having the type identified 
        by the input string
        """

        if not isinstance(type, str):
            return pd.DataFrame()

        with sqlite3.connect(self.dbPathOrUrl) as con:

            if type == 'image':
                query = "SELECT body AS id FROM annotations"
                result = pd.read_sql(query, con)

            elif type == 'annotation':
                query = "SELECT * FROM annotations"
                result = pd.read_sql(query, con)

            else:
                query = "SELECT id, title, creator FROM metadata WHERE id LIKE {canvas}"
                result = pd.read_sql(query, con, params=('%' + type + '%',))

        return result

    
    def getEntitiesWithTitle(self, title):
        """
        It returns a data frame containing all the metadata included in the database
        related to the entities having, as title, the input title
        """

        with sqlite3.connect(self.dbPathOrUrl) as con:
            query = "SELECT DISTINCT id, title, creator FROM metadata WHERE title = ?"
            result = pd.read_sql(query, con, params=(title,))
        return result

    def getEntityById(self, id):
        """
        It returns a data frame containing an entity identified 
        by the input identifier
        """
        if not isinstance(id, str):
            return pd.DataFrame()

        with sqlite3.connect(self.dbPathOrUrl) as con:
            # search in the metadata table
            query = "SELECT id, title, creator FROM metadata WHERE id = ?"
            cursor = con.cursor()
            cursor.execute(query, (id,))
            metadata_result = cursor.fetchall()

            # search in the annotations table
            query = "SELECT id, body, target, motivation FROM annotations WHERE id = ?"
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


#=======GRAPH DATADASE=========

# Done by Evan
class CollectionProcessor(Processor):
    """
    It takes in input the path of a JSON file and uploads them 
    in the graph database
    """

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


# Done by Thomas and Evgeniia

class TriplestoreQueryProcessor(QueryProcessor):
    SPARQL_PREFIXES = """
        PREFIX schema: <https://schema.org/>
        PREFIX rumi: <https://github.com/mjavadf/rumi_group_project/>
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#> 
    """

    def getEntityById(self, entity_id):
        """
        It returns a data frame containing an entity 
        identified by the input identifier
        """

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
        return df_sparql


    def getAllCanvases(self):
        """
        It returns a data frame containing all the canvases included in the database
        """

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
        return df_sparql_getAllCanvases

    def getAllCollections(self):
        """
        It returns a data frame containing all the collections 
        included in the database
        """

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
        return df_sparql_getAllCollections


    def getAllManifests(self):
        """
        It returns a data frame containing all the manifests 
        included in the database
        """

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
        return df_sparql_getAllManifests


    def getCanvasesInCollection(self, collection_id):
        """
        It returns a data frame containing all the canvases included in the database
        that are contained in the collection identified by the input identifier
        """

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
        return df_sparql_CanvasesInCollection


    def getCanvasesInManifest(self, manifest_id):
        """
        It returns a data frame containing all the canvases included in the database
        that are contained in the manifest identified by the input identifier
        """

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
        return df_sparql_CanvasesInManifest


    def getEntitiesWithLabel(self, label):
        """
        It returns a data frame containing all the entities included in the database
        that have the input label
        """

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
        return df_sparql_getEntitiesWLabel


    def getManifestsInCollection(self, collection_id):
        """
        It returns a data frame containing all the manifests included in the database
        that are contained in the collection identified by the input identifier
        """

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
        return df_sparql_ManifestsInCollection
    

    def getCollectionsContainingCanvases(self, canvas_id):
        """
        It returns a dataframe containing all the collections that contains the 
        canvas specified as an input
        """

        endpoint = self.getDbPathOrUrl()
        query_getCollectionsContainingCanvases = f"""
            {TriplestoreQueryProcessor.SPARQL_PREFIXES}
            
            SELECT ?id ?label
            WHERE {{
                ?collection rdf:type rumi:Collection ;
                            schema:identifier ?id ;
                            rdfs:label ?label ;
                            rumi:items ?manifest .
                ?manifest rdf:type rumi:Manifest ;
                          rumi:items ?canvas_id .
                ?canvas_id schema:identifier "{canvas_id}" .
                           
            
            }}                            
            """
        df_sparql_getCollectionsContainingCanvases = get(
            endpoint, query_getCollectionsContainingCanvases, True
        )
        return df_sparql_getCollectionsContainingCanvases
    

    def getManifestsContainingCanvases(self, canvas_id):
        """
        It returns a dataframe containing all the manifests that contains
        the canvas specified as an input
        """

        endpoint = self.getDbPathOrUrl()
        query_getManifestsContainingCanvases = f"""
            {TriplestoreQueryProcessor.SPARQL_PREFIXES}
            
            SELECT ?id ?label
            WHERE {{
                ?manifest rdf:type rumi:Manifest ;
                            schema:identifier ?id ;
                            rdfs:label ?label ;
                            rumi:items ?canvas_id .
                ?canvas_id schema:identifier "{canvas_id}" .
                           
            
            }}                            
            """
        df_sparql_getManifestsContainingCanvases = get(
            endpoint, query_getManifestsContainingCanvases, True
        )
        return df_sparql_getManifestsContainingCanvases
    

    def getEntitiesWithType(self, entity_type):
        """
        It returns a list of objects having class the type 
        identified by the input string
        """

        endpoint = self.getDbPathOrUrl()
        query = f"""
            {TriplestoreQueryProcessor.SPARQL_PREFIXES}
            
            SELECT ?id ?label
            WHERE {{
                ?entity rdf:type rumi:{entity_type.capitalize()} .
                ?entity schema:identifier ?id .
                ?entity rdfs:label ?label .
                
            }}
            """
        df_sparql = get(endpoint, query, True)
        return df_sparql


#==============QUERIES IN THE BOTH DATABASES==============

class GenericQueryProcessor(QueryProcessor):
    query_processors = []

    def cleanQueryProcessors(self):
        """
        It clean the list query_processors from all the QueryProcessor objects
        it includes
        """

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
        """
        It adds the input QueryProcessor object 
        to the list query_processors 
        """

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
    def convert_triple(self, dataframe):
        match_types = {
            "https://github.com/mjavadf/rumi_group_project/Canvas": Canvas,
            "https://github.com/mjavadf/rumi_group_project/Collection": Collection,
            "https://github.com/mjavadf/rumi_group_project/Manifest": Manifest
        }

        def build_object(row):
            """It converts the input row into an object having the class model."""
            model = match_types.get(row["type"])

            if model:
                return model(
                    id=row["id"],
                    label=row.get("label"),
                    title=row.get("title"),
                    creator=row.get("creator"),
                )
            else:
                return None

        return [build_object(row) for _, row in dataframe.iterrows()]

    def getEntityById(self, entity_id):
        """
        It returns an identifiable entity with the same id as in the input
        or it returns None
        """
        entities_data = pd.DataFrame(columns=["id"])
                
        for processor in self.query_processors:
            data = processor.getEntityById(entity_id)
            if data is not None and not data.empty:
                entities_data = pd.merge(entities_data, data, on='id', how='outer')
                entities_data = entities_data[~entities_data.duplicated(subset='id')]  
                            
        if entities_data.empty:
            return None
        
        if "type" in entities_data.columns:
           return self.convert_triple(entities_data)
        
        elif "motivation" in entities_data.columns:
            entity = entities_data.iloc[0].to_dict()
            return Annotation(
                id=entity.get("id"),
                motivation=entity.get("motivation"),
                body=Image(id=entity.get("body")),
                target=IdentifiableEntity(id=entity.get("target")),
            )
        
        else:
            return None


    def getAllAnnotations(self):
        """
        It returns a list of objects having class Annotation 
        included in the databases accessible via the query processors
        """

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
        """
        It returns a list of objects having class Canvas included 
        in the databases accessible via the query processors
        """

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
        """
        It returns a list of objects having class Image included 
        in the databases accessible via the query processors
        """

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
        """
        It returns a list of objects having class Annotation, 
        included in the databases accessible via the query processors, 
        that have, as annotation target, the canvas specified by the input identifier
        """

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
        """
        It returns a list of objects having class Annotation, 
        included in the databases accessible via the query processors, that have, 
        as annotation target, the collection specified by the input identifier
        """

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
        """
        It returns a list of objects having class Annotation, 
        included in the databases accessible via the query processors, that have, 
        as annotation target, the manifest specified by the input identifier
        """

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
        """
        It returns a list of objects having class Annotation, 
        included in the databases accessible via the query processors, that have, 
        as annotation body, the entity specified by the input identifier
        """

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
        """
        It returns a list of objects having class Annotation, 
        included in the databases accessible via the query processors, that have, 
        as annotation body and annotation target, the entities specified 
        by the input identifiers
        """

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
        """
        It returns a list of objects having class Annotation, 
        included in the databases accessible via the query processors, that have, 
        as annotation target, the manifest specified by the input identifier
        """

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
        """
        It returns a list of objects having class Canvas, included in the databases 
        accessible via the query processors, that are contained in the collection 
        identified by the input identifier
        """

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
        """
        It returns a list of objects having class Canvas, included in the databases
        accessible via the query processors, that are contained in the manifest 
        identified by the input identifier
        """

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
        """
        It returns a list of objects having class Manifest included 
        in the databases accessible via the query processors
        """

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
        """
        It returns a list of objects of the class EntityWithMetadata
        with the same creator as in the input 
        """

        entities =[]
        unique_ids = set()  # To keep track of unique IDs
        triple_processor = None
        relational_processor = None

        # find the triple and relational processors
        for processor in self.query_processors:
            if isinstance(processor, TriplestoreQueryProcessor):
                triple_processor = processor
            elif isinstance(processor, RelationalQueryProcessor):
                relational_processor = processor
    
        if triple_processor is None or relational_processor is None:
            return []  # return an empty list if either processor is not found  
        
        relational_entity = relational_processor.getEntitiesWithCreator(creator_name)

        for _, entity in relational_entity.iterrows():
            entity_id = entity["id"]
            if entity_id not in unique_ids:
                    unique_ids.add(entity_id)

            entity_info = self.getEntityById(entity_id)
            entities.extend(entity_info)   
                
        return entities       


    def getEntitiesWithLabel(self, label):
        """
        It returns a list of objects having class EntityWithMetadata, 
        included in the databases accessible via the query processors, 
        related to the entities having, as label, the input label
        """

        entities = []
        unique_ids = set()  # To keep track of unique IDs
        triple_processor = None
        relational_processor = None

        # find the triple and relational processors
        for processor in self.query_processors:
            if isinstance(processor, TriplestoreQueryProcessor):
                triple_processor = processor
            elif isinstance(processor, RelationalQueryProcessor):
                relational_processor = processor
    
        if triple_processor is None or relational_processor is None:
            return []  # return an empty list if either processor is not found  
        
        triple_entity = triple_processor.getEntitiesWithLabel(label)

        for _, entity in triple_entity.iterrows():
            entity_id = entity["id"]
            if entity_id not in unique_ids:
                unique_ids.add(entity_id)

            entity_info = self.getEntityById(entity_id)
            entities.extend(entity_info)   
                
        return entities   
            

    def getEntitiesWithTitle(self, title):
        """
        It returns a list of objects having class EntityWithMetadata, 
        included in the databases accessible via the query processors, related to 
        the entities having, as title, the input title
        """

        entities = []
        unique_ids = set()  # To keep track of unique IDs
        triple_processor = None
        relational_processor = None

        # find the triple and relational processors
        for processor in self.query_processors:
            if isinstance(processor, TriplestoreQueryProcessor):
                triple_processor = processor
            elif isinstance(processor, RelationalQueryProcessor):
                relational_processor = processor
    
        if triple_processor is None or relational_processor is None:
            return []  # return an empty list if either processor is not found  
        
        relational_entity = relational_processor.getEntitiesWithTitle(title)

        for _, entity in relational_entity.iterrows():
            entity_id = entity["id"]
            if entity_id not in unique_ids:
                unique_ids.add(entity_id)

            entity_info = self.getEntityById(entity_id)
            entities.extend(entity_info)
                
        return entities        


    def getImagesAnnotatingCanvas(self, canvas_id):
        """
        It returns a list of objects having class Image, included in the databases 
        accessible via the query processors, that are body of the annotations 
        targetting the canvases specified by the input identifier
        """

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
        """
        It returns a list of objects having class Manifest, included in the databases
        accessible via the query processors, that are contained in the collection 
        identified by the input identifier
        """

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
        """
        It returns a list of objects having class Collection included in the 
        databases accessible via the query processors
        """

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
  
    def getCollectionsContainingCanvases(self, canvases): 
        """
        It returns a list of objects having class Collection, included in the 
        databases accessible via the query processor, that contain 
        any of the canvases specified as input
        """
        collections_unique = set()
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
        
        for canvas_id in canvases:
            triple_collection = triple_processor.getCollectionsContainingCanvases(canvas_id)

            for _, collection in triple_collection.iterrows():
                collection_id = collection["id"]
                label = collection.get("label")
                title = None
                creator = None

                entity_data = relational_processor.getEntityById(collection_id)
                if not entity_data.empty:
                    title = entity_data.loc[0, "title"]
                    creator = entity_data.loc[0, "creator"]

                items = self.getManifestsInCollection(collection_id)    
      
                if collection_id not in collections_unique:
                    collections_unique.add(collection_id)
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

    def getManifestContainingCanvases(self, canvases):
        """
        It returns a list of objects having class Manifest, included in the 
        databases accessible via the query processors, that contain any of the 
        canvases specified as input
        """

        manifests_unique = set()
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
        
        for canvas_id in canvases:
            triple_manifest = triple_processor.getManifestsContainingCanvases(canvas_id)

            for _, manifest in triple_manifest.iterrows():
                manifest_id = manifest["id"]
                label = manifest.get("label")
                title = None
                creator = None

                entity_data = relational_processor.getEntityById(manifest_id)
                if not entity_data.empty:
                    title = entity_data.loc[0, "title"]
                    creator = entity_data.loc[0, "creator"]

                items = self.getCanvasesInManifest(manifest_id)    
      
                if manifest_id not in manifests_unique:
                    manifests_unique.add(manifest_id)
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
    
    def getEntityByType(self, entity_type):
        """
        It returns a list of objects having class the type identified by 
        the input string. The possible values of the input string are 
        "annotation", "image", "collection", "manifest", "canvas”
        """

        allowed_types = ["annotation", "image", "collection", "manifest", "canvas"]

        if entity_type not in allowed_types:
            raise ValueError("Invalid entity type")

        entities = []
        entities_data = pd.DataFrame()
        for processor in self.query_processors:
            data = processor.getEntitiesWithType(entity_type)
            if data is not None and not data.empty:
                entities_data = pd.concat([entities_data, data], axis=0, ignore_index=True)
                entities_data.drop_duplicates(subset=['id'], inplace=True)   

        for _, entity_row in entities_data.iterrows():
            entity_id = str(entity_row["id"])  
            entities.append(IdentifiableEntity(id=entity_id))    

        if entities:
            return entities
        else:
             return None
    
    # by Evan & Javad
    
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
        It returns a list of objects having class Annotation, included in the 
        databases accessible via the query processors, that have, as annotation 
        target, the canvas specified by the input identifier
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

    
