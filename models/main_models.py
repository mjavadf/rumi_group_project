class IdentifiableEntity(object):
    """A base class that provides an identifier for an entity."""

    def __init__(self, id: str) -> None:
        """
        Initialize an instance of the class with an identifier.
        
        :param id: A unique identifier for the entity.
        :type id: str
        """
        self.id = id

    def getId(self) -> str:
        """
        Get the identifier for the entity.
        
        :return: The identifier for the entity.
        :rtype: str
        """
        return self.id


class Image(IdentifiableEntity):
    """A subclass of the IdentifiableEntity class that represents an image entity with a unique identifier."""
    def __init__(self, id: str):
        super().__init__(id)


class Annotation(IdentifiableEntity):
    """
    A subclass of the IdentifiableEntity class that represents an annotation entity with a unique identifier, 
    a motivation, a target, and a body.
    """

    def __init__(self, id: str, motivation: str, target: IdentifiableEntity, body: Image):
        """
        Initialize an instance of the class with an identifier, a motivation, a target, and a body.
        
        :param id: A unique identifier for the entity.
        :type id: str
        :param motivation: The motivation behind the annotation.
        :type motivation: str
        :param target: The target of the annotation.
        :type target: IdentifiableEntity
        :param body: The body of the annotation, which is an image.
        :type body: Image
        """
        super().__init__(id)
        self.body = body
        self.target = target
        self.motivation = motivation


    def getBody(self) -> Image:
        """
        Get the body of the annotation.
        
        :return: The body of the annotation.
        :rtype: Image
        """
        return self.body

    def getTarget(self) -> IdentifiableEntity:
        """
        Get the target of the annotation.
        
        :return: The target of the annotation.
        :rtype: IdentifiableEntity
        """
        return self.target

    def getMotivation(self) -> str:
        """
        Get the motivation behind the annotation.
        
        :return: The motivation behind the annotation.
        :rtype: str
        """
        return self.motivation


class EntityWithMetaData(IdentifiableEntity):
    def __init__(self, id, label, title=None, creator=None) -> None:
        if creator is None:
            creator = []
        self.label = label
        self.title = title
        self.creator = creator
        super().__init__(id)

    def getLabel(self) -> str:
        return self.label

    def getTitle(self) -> str:
        if len (self.title) > 0:
            return self.title
        else:
            return None

    def getCreator(self) -> str:
        return self.creator


class Collection(EntityWithMetaData):
    def __init__(self, id, label, items=list, title=None, creator=None):
        super().__init__(id, label, title, creator)
        self.items = list()
        for item in items:
            self.items.append(item)

    def getItems(self):
        return self.items


class Manifest(EntityWithMetaData):
    def __init__(self, id, label, items=list, title=None, creator=None):
        super().__init__(id, label, title, creator)
        self.items = list()
        for item in items:
            self.items.append(item)

    def getItems(self):
        return self.items


class Canvas(EntityWithMetaData):
    def __init__(self, id, label, title=None, creator=None):
        super().__init__(id, label, title, creator)
