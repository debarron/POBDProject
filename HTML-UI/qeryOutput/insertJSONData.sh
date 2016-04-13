#!/bin/bash

mongoimport -d pobd -c q1 --type json --file ./query1_2.json --jsonArray
mongoimport -d pobd -c q2 --type json --file ./query3.json --jsonArray
mongoimport -d pobd -c q3 --type json --file ./query4.json --jsonArray
mongoimport -d pobd -c q4 --type json --file ./query6.json --jsonArray
mongoimport -d pobd -c q5 --type json --file ./query89.json --jsonArray
mongoimport -d pobd -c q6 --type json --file ./query10.json --jsonArray
mongoimport -d pobd -c q7 --type json --file ./query11.json --jsonArray
mongoimport -d pobd -c q8 --type json --file ./query12.json --jsonArray
mongoimport -d pobd -c q9 --type json --file ./query13.json --jsonArray

