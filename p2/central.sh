#!/bin/bash
javac -cp "libs/*" p2/db/DBManager.java p2/central/*.java && \
java -cp ".:libs/*" p2.central.EV_Central localhost:9092