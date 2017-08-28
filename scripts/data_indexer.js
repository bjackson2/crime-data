/*

Boston Crime Data ES bulk indexer

Usage: node data_indexer.js /path/to/stats_2012-2015.json /path/to/stats_2015-present.json

The stats for 2012-2015 are here - https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-July-2012-August-2015-Sourc/7cdf-6fgx
The stats for 2015-present are here - https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-August-2015-To-Date-Source-/fqn4-4qap

Download the JSON files for each dataset. Each set has different fields and data, but the shared
(and most interesting) stuff is indexed here.

*/

const fs = require('fs');
const elasticsearch = require('elasticsearch');
const _ = require('lodash');

const DATA_2012_TO_2015 = process.argv[2];
const DATA_2015_TO_PRESENT = process.argv[3];
const INDEX_NAME = 'crime_data';
const INDEX_OBJECT = { index: { _index: INDEX_NAME, _type: 'report' } };
const BULK_BATCH_SIZE = 20000;
const DEFAULT_REQUEST_TIMEOUT = 300000;

let client = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'trace'
});

function oldRowProcessor(row) {
  return {
    offenseDescription: row[10],
    offenseCode: row[11],
    reportingDistrict: row[12],
    reportingArea: row[13],
    shooting: row[16],
    occurredAt: row[14],
    weekday: row[21],
    streetName: row[25],
    latitude: row[27][1],
    longitude: row[27][2]
  };
};

function newRowProcessor(row) {
  return {
    offenseDescription: row[11],
    offenseCode: row[9],
    reportingDistrict: row[12],
    reportingArea: row[13],
    shooting: row[14],
    occurredAt: row[15],
    weekday: row[19],
    streetName: row[21],
    latitude: row[22],
    longitude: row[23]
  };
};

function buildRequestBody(rows, rowProcessor) {
  let body = [];

  rows.forEach((row) => {
    body.push(INDEX_OBJECT);
    body.push(rowProcessor(row));
  });

  return body;
};

function bulkIndex(rows, rowProcessor) {
  client.bulk({
    requestTimeout: DEFAULT_REQUEST_TIMEOUT,
    body: buildRequestBody(rows, rowProcessor)
  }, (err, response) => {
    if (err) throw err;
    console.log(response);
  });
};

function processData(rowProcessor) {
  return (err, data) => {
    if (err) throw err;

    let allRows = JSON.parse(data).data;

    _.chunk(allRows, BULK_BATCH_SIZE).forEach((rows) => {
      bulkIndex(rows, rowProcessor);
    });
  };
};

client.indices.delete({
  index: INDEX_NAME,
  ignore: [404]
}).then((body) => {
  console.log(body);
}, (error) => {
  console.log(error);
});

fs.readFile(DATA_2012_TO_2015, processData(oldRowProcessor));
fs.readFile(DATA_2015_TO_PRESENT, processData(newRowProcessor));
