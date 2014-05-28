var speedometer   = require('speedometer');
var when          = require('when');
var es            = require('event-stream');
var mysql         = require('mysql');
var elasticsearch = require('elasticsearch');

var grouper = function(count, reduceFunc, initialValue){
  var buffer = [];
  return es.through(function(data){
    buffer.push(data);
    if(buffer.length >= count){
      if(reduceFunc) buffer = buffer.reduce(reduceFunc, initialValue);
      this.emit('data', buffer);
      buffer = [];
    }
  },function(){
    if(reduceFunc) buffer = buffer.reduce(reduceFunc, initialValue);
    if(buffer.length > 0) this.emit('data', buffer);
    this.emit('end');
  });
};

var ElasticsearchBulkQueryRunnerStream = function(esClient, sync){
  if(sync){
    return es.through(function(query){
      this.pause();
      esClient.bulk(query).finally(this.resume);
    });
  }
  return es.map(function(query, cb){
    cb(null, esClient.bulk(query));
  });
};

var ElasticsearchIndexBulkQueryBuilder = function(esIndexName, esType, batchSize){
  return grouper(batchSize, function(prev, curr){
    prev.body.push({ 
      index: {
        _index : esIndexName,
        _type  : esType
      }
    });
    prev.body.push(curr);
    return prev;
  }, {body:[]});
};

var ElasticImporter = function(mysqlConfig, elasticsearchConfig){
  var importer = {};
  var conn = mysql.createConnection(mysqlConfig);
  var esClient = new elasticsearch.Client(elasticsearchConfig);
  
  importer.close = function(){
    conn.end();
    esClient.close();
  };

  importer.doImport = function(config){
    var sqlQuery  = config.query,
      esIndexName = config.index,
      batchSize   = config.batchSize || 0,
      syncLoad    = config.sync,
      esType      = config.type;

    console.log('\nQuery: ' + sqlQuery    +
                '\nIndex: ' + esIndexName +
                '\nType: '  + esType )

    var speed = speedometer();

    var result = conn.query(sqlQuery)
      .stream()
      .pipe(new ElasticsearchIndexBulkQueryBuilder(esIndexName, esType, batchSize))
      .pipe(new ElasticsearchBulkQueryRunnerStream(esClient, true));
    result.on('data', function(data){
      var recordsPerSecond = speed(data.items.length/2); // We divide by 2 since batch-ops needs 2 objects to define
      console.log(recordsPerSecond+' records/s');
    });
    return result;
  };

  return importer;
};

module.exports = ElasticImporter;
