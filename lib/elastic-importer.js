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
}

var BatchRowSender = function(esClient, esIndexName, esType, batchSize){
    var bulkQuery     = [];
    var count         = 0;
    var batchRequests = [];
    var executeBatch  = function(){
      if(bulkQuery.length <= 0){
        return when.resolve(null);
      }
      return esClient.bulk({
        body: bulkQuery
      }).then(function(data){
        bulkQuery = [];
        return data;
      });
    };
    var batchProccessRow = function(row){
      bulkQuery.push({
        index: {
          _index : esIndexName,
          _type  : esType
        }
      });
      bulkQuery.push(row);
      count++;
      if(count % batchSize == 0){
        return executeBatch();
      }
      return when.resolve(null);
    };
    return es.through(function(data){
      var self = this;
      batchRequests.push(batchProccessRow(data)
        .then(function(data) { if(data) self.emit('data', data) })
        .finally(this.resume)
      );
      this.pause();
    }, function(){
      var self = this;
      batchRequests.push(executeBatch().then(function(data) { self.emit('data', data) }));
      when
        .all(batchRequests)
        .finally(function() { 
          self.emit('end') 
        });
    });
};

var ElasticsBulkQueryRunner = function(esClient, sync){
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

var ElasticImporter = function(mysqlConfig, elasticsearchConfig){
  var conn = mysql.createConnection(mysqlConfig);
  var esClient = new elasticsearch.Client(elasticsearchConfig);

  var doImport = function(config){
    var sqlQuery  = config.query,
      esIndexName = config.index,
      batchSize   = config.batchSize || 0,
      syncLoad    = config.sync,
      esType      = config.type;

    console.log('\nQuery: ' + sqlQuery    +
                '\nIndex: ' + esIndexName +
                '\nType: '  + esType )

    conn.connect();
    var queryStream   = conn.query(sqlQuery).stream();
    return queryStream.pipe(grouper(batchSize, function(prev, curr){
      prev.body.push({
        index: {
          _index : esIndexName,
          _type  : esType
        }
      });
      prev.body.push(curr);
      return prev;
    }, {body: []}))
    .pipe(ElasticsBulkQueryRunner(esClient, true));//pipe(BatchRowSender(esClient, esIndexName, esType, batchSize));
  };
  var close = function(){
    conn.end();
    esClient.close();
  };
  return {
    doImport: doImport,
    close: close
  };
};

module.exports = ElasticImporter;
