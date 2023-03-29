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
    pass


class Annotation(IdentifiableEntity):
    """
    A subclass of the IdentifiableEntity class that represents an annotation entity with a unique identifier, 
    a motivation, a target, and a body.
    """

    def __init__(self, id: str, motivation: str, target: IdentifiableEntity, body: Image) -> None:
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
        self.motivation = motivation
        self.target = target
        self.body = body
        super().__init__(id)

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
