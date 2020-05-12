#!/bin/sh

WGET="wget -N"
$WGET http://purl.obolibrary.org/obo/go.owl
$WGET http://purl.obolibrary.org/obo/ncbitaxon.owl
$WGET http://purl.obolibrary.org/obo/envo.owl
$WGET https://raw.githubusercontent.com/hurwitzlab/planet-microbe-ontology/master/pmo.owl
