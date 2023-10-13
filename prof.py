# Supposing that all the classes developed for the project
# are contained in the file 'impl.py', then:

# 1) Importing all the classes for handling the relational database
from impl import AnnotationProcessor, MetadataProcessor, RelationalQueryProcessor

# 2) Importing all the classes for handling RDF database
from impl import CollectionProcessor, TriplestoreQueryProcessor

# 3) Importing the class for dealing with generic queries
from impl import GenericQueryProcessor

# Once all the classes are imported, first create the relational
# database using the related source data
rel_path = "relational.db"
ann_dp = AnnotationProcessor()
ann_dp.setDbPathOrUrl(rel_path)
ann_dp.uploadData("data/annotations.csv")

met_dp = MetadataProcessor()
met_dp.setDbPathOrUrl(rel_path)
met_dp.uploadData("data/metadata.csv")

# Then, create the RDF triplestore (remember first to run the
# Blazegraph instance) using the related source data
grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
col_dp = CollectionProcessor()
col_dp.setDbPathOrUrl(grp_endpoint)
col_dp.uploadData("data/collection-1.json")
col_dp.uploadData("data/collection-2.json")

# In the next passage, create the query processors for both
# the databases, using the related classes
rel_qp = RelationalQueryProcessor()
rel_qp.setDbPathOrUrl(rel_path)

grp_qp = TriplestoreQueryProcessor()
grp_qp.setDbPathOrUrl(grp_endpoint)

# Finally, create a generic query processor for asking
# about data
generic = GenericQueryProcessor()
generic.addQueryProcessor(rel_qp)
generic.addQueryProcessor(grp_qp)

result_q1 = generic.getAllManifests()
result_q2 = generic.getEntitiesWithCreator("Alighieri, Dante")
result_q3 = generic.getAnnotationsToCanvas(
    "https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1"
)
result_q4 = generic.getManifestContainingCanvases(
    [
        "https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1",
        "https://dl.ficlit.unibo.it/iiif/2/19428/canvas/p6",
    ]
)
# etc...

result_q5 = generic.getEntityByType("image")
print(result_q1)
#print(result_q2)
#print(type(result_q3))
#print(result_q4)
#print(result_q5)
