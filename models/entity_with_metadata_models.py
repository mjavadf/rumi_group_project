from main_models import IdentifiableEntity


class EntityWithMetadata(IdentifiableEntity):
    pass


class Collection(EntityWithMetadata):
    pass


class Manifest(EntityWithMetadata):
    pass


class Canvas(EntityWithMetadata):
    pass
