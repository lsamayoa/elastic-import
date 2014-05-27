var config        = require('../config');
var moment        = require('moment');
var mysql         = require('mysql');
var elasticsearch = require('elasticsearch');
var program       = require('commander');

// -q   Query string- 
// -i   Index Name
// -t   Elasticsearch type

program
  .version('0.0.1')
  .option('-q, --query [query]', 'SQL Query')
  .option('-t, --type [type]', 'Elasticsearch Type')
  .option('-i, --index [index]', 'Index name')
  .option('-b, --batchSize [batchSize]', 'Batchsize')
  .option('-s, --sync', 'Wait IO Mysql/Elasticsearch')
  .parse(process.argv);
console.log(program.index)
var sqlQuery  = program.query,
  esIndexName = program.index,
  batchSize   = program.batchSize || 500,
  syncLoad    = program.sync,
  esType      = program.type;
console.log('\nQuery: ' + sqlQuery + 
            '\nIndex: '+ esIndexName +
            '\nType: ' + esType )

var conn = mysql.createConnection(config.mysql);
var esClient = new elasticsearch.Client(config.elasticsearch)
conn.connect();
try {
  var query = conn.query(sqlQuery);
  var bulkQuery = [];
  var count = 0;
  var executeBatch = function(){
    if(syncLoad){
      conn.pause();
    }
    esClient.bulk({
      body: bulkQuery
    }).then(function(){
      if(syncLoad){
        conn.resume();
      }
    });
    bulkQuery = [];
  };
  query.on('result', function(row){
    bulkQuery.push({
      index: {
        _index : esIndexName,
        _type  : esType
      }
    });
    bulkQuery.push(row);
    count++;
    if(count % batchSize == 0){
      executeBatch();
    }
  }).on('end', function(){
    executeBatch();
  });
} finally {
  conn.end();
}
