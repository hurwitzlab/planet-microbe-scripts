#!/bin/sh

rm blazegraph.jnl

java -cp blazegraph.jar com.bigdata.rdf.store.DataLoader -verbose -namespace planetmicrobe fastload.properties ncbitaxon.owl envo.owl go.owl pmo.owl
