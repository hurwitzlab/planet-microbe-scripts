# Blazegraph Scripts

## Fetch latest ontology OWL files
Fetch PMO, ENVO, GO, and NCBI Taxonomy.
 
```
fetch_owl_files.sh
```

## Load OWL files into new Blazegraph DB 
Create Blazegraph database with full-text indexing enabled. 

Note: this overwrites existing DB.

```
reload_owl_files.sh
```

## Start Blazegraph
Starts Blazegraph in the background.

http://localhost:9999/blazegraph (console)
http://localhost:9999/blazegraph/sparql (SPARQL endpoint)
```
start_server.sh
```