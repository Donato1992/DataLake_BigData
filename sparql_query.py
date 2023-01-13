import rdflib
import logging
import os
import owlrl

#Debugging
LOG_FILE='sparql.log'
if os.path.exists(LOG_FILE):
	os.remove(LOG_FILE)
logging.basicConfig(filename=LOG_FILE,level=logging.DEBUG)

GRAPH = rdflib.Graph()
GRAPH.parse("knowladge_graph.ttl",format="turtle")

type_dimension="iso2"
#y è il continent
#x è la dimensione
owlrl.DeductiveClosure(owlrl.CombinedClosure.RDFS_OWLRL_Semantics,datatype_axioms=True).expand(GRAPH)
q=GRAPH.query("""SELECT ?x ?y WHERE{{SELECT DISTINCT ?x ?y WHERE{?x property:rollup ?y. ?x property:inLevel level:country} ORDER BY ASC(?X)} UNION {SELECT DISTINCT ?x ?y WHERE{?x property:rollup ?k. ?k property:rollup ?y.?x property:inLevel level:region}ORDER BY ASC(?X)}}""")
#q=GRAPH.query("""SELECT ?x ?y  WHERE {?x ?z ?y.FILTER(?z=property:inLevel && ?x!=member:month && ?x!=member:year)}""")

temp=[]
for row in q:
    print(f"{row.x},{row.y}")
    logging.debug(f"{row.x},{row.y}")
    temp.append(str(row.y).split("#")[1])
tt=set(temp)
logging.debug(tt)
