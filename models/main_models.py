class IdentifiableEntity(object):
    def __init__(self, id: str) -> None:
        self.id = id
    
    def getId(self) -> str:
        return self.id


class Image(IdentifiableEntity):
    pass


class Annotation(IdentifiableEntity):
    def __init__(self, id: str, motivation: str, target: IdentifiableEntity, body: Image) -> None:
        self.motivation = motivation
        self.target = target
        self.body = body
        super().__init__(id)
    
    def getBody(self) -> Image:
        return self.body
    
    def getTarget(self) -> IdentifiableEntity:
        return self.target
    
    def getMotivation(self) -> str:
        return self.motivation