#!/bin/sh

OWLFILES="ncbitaxon.owl envo.owl go.owl pmo.owl"

rm blazegraph.jnl

java -cp blazegraph.jar com.bigdata.rdf.store.DataLoader -verbose -namespace planetmicrobe load.properties $OWLFILES
