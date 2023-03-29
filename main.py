import pandas as pd
from models.main_models import IdentifiableEntity


class Processor(object):
    def __int__(self, db_path_or_url: str):
        self.db_path_or_url = db_path_or_url

    def get_db_path_or_url(self) -> str:
        pass

    def set_db_path_or_url(self, path_or_url: str) -> None:
        pass


class MetadataProcessor(Processor):

    def upload_data(self, path: str) -> bool:
        pass


class CollectionProcessor(Processor):

    def upload_data(self, path: str) -> bool:
        pass


class AnnotationProcessor(Processor):

    def upload_data(self, path: str) -> bool:
        pass


class QueryProcessor(Processor):

    def get_entity_by_id(self, entity_id: str) -> pd.DataFrame:
        pass


class TriplestoreQueryProcessor(QueryProcessor):
    def get_all_canvases(self) -> pd.DataFrame:
        pass

    def get_all_images(self) -> pd.DataFrame:
        pass

    def get_all_manifests(self) -> pd.DataFrame:
        pass

    def get_canvases_in_collection(self, collection_id: str) -> pd.DataFrame:
        pass

    def get_canvases_in_manifest(self, manifest_id: str) -> pd.DataFrame:
        pass

    def get_entities_with_label(self, label: str) -> pd.DataFrame:
        pass

    def get_manifests_in_collection(self, collection_id: str) -> pd.DataFrame:
        pass


class RelationalQueryProcessor(QueryProcessor):
    def get_all_annotations(self) -> pd.DataFrame:
        pass

    def get_all_images(self) -> pd.DataFrame:
        pass

    def get_annotations_with_body(self, body_id: str) -> pd.DataFrame:
        pass

    def get_annotations_with_body_and_target(self,
                                             body_id: str,
                                             target_id: str) -> pd.DataFrame:
        pass

    def get_annotations_with_target(self, target_id: str) -> pd.DataFrame:
        pass

    def get_entities_with_creator(self, creator_name: str) -> pd.DataFrame:
        pass

    def get_entities_with_title(self, title: str) -> pd.DataFrame:
        pass


class GenericQueryProcessor(object):
    def __init__(self, query_processor: QueryProcessor) -> None:
        self.query_processor = query_processor

    def clean_query_processor(self) -> bool:
        pass

    def add_query_processor(self, processor: QueryProcessor) -> bool:
        pass

    def get_all_annotations(self) -> list:
        pass

    def get_all_canvas(self) -> list:
        pass

    def get_all_collections(self) -> list:
        pass

    def get_all_images(self) -> list:
        pass

    def get_all_manifests(self) -> list:
        pass

    def get_annotations_to_canvas(self, canvas_id: str) -> list:
        pass

    def get_annotations_to_collection(self, collection_id: str) -> list:
        pass

    def get_annotations_to_manifest(self, manifest_id: str) -> list:
        pass

    def get_annotations_with_body(self, body_id: str) -> list:
        pass

    def get_annotations_with_body_and_target(self,
                                             body_id: str,
                                             target_id: str) -> list:
        pass

    def get_annotations_with_target(self, target_id: str) -> list:
        pass

    def get_canvases_in_collection(self, collection_id: str) -> list:
        pass

    def get_canvases_in_manifest(self, manifest_id: str) -> list:
        pass

    def get_entity_by_id(self, entity_id: str) -> IdentifiableEntity:
        pass

    def get_entities_with_creator(self, creator_name: str) -> list:
        pass

    def get_with_label(self, label: str) -> list:
        pass

    def get_entities_with_title(self, title: str) -> list:
        pass

    def get_images_annotating_canvas(self, canvas_id: str) -> list:
        pass

    def get_manifests_in_collection(self, collection_id: str) -> list:
        pass
