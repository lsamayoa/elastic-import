var when          = require('when');
var es            = require('event-stream');
var mysql         = require('mysql');
var elasticsearch = require('elasticsearch');

var isFunction = function(functionToCheck) {
 var getType = {};
 return functionToCheck && getType.toString.call(functionToCheck) === '[object Function]';
}

var grouper = function(batchSize, reduceFunc, initialValue){
  var sharedBuffer = [];
  var emitBuffer = function(stream) {
    if(isFunction(reduceFunc)) {
      stream.emit('data', sharedBuffer.reduce(reduceFunc, initialValue()));
    }else{
      stream.emit('data', sharedBuffer);
    }
  };
  return es.through(function(data){
    sharedBuffer.push(data);
    if(sharedBuffer.length >= batchSize && !this.paused){
      this.pause();
      emitBuffer(this);
      sharedBuffer = [];
      this.resume();
    }
  },function(){
    if(sharedBuffer.length > 0){
      if(isFunction(reduceFunc)) sharedBuffer = sharedBuffer.reduce(reduceFunc, initialValue());
      this.emit('data', sharedBuffer);
    }
    this.emit('end');
  });
};

var ElasticsearchBulkQueryRunnerStream = function(esClient, sync){
  if(sync){
    var bulkRequests = [];
    return es.through(function(query){
      this.pause();
      var self = this;
      var req  = esClient.bulk(query)
        .then(function(data){self.emit('data', data)})
        .finally(this.resume);
      bulkRequests.push(req);
    }, function(){
      var self = this;
      when.all(bulkRequests).finally(function(){self.emit('end')});
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
  }, function(){return {body:[]}});
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

    var result = conn.query(sqlQuery)
      .stream()
      .pipe(new ElasticsearchIndexBulkQueryBuilder(esIndexName, esType, batchSize))
      .pipe(new ElasticsearchBulkQueryRunnerStream(esClient, true));
    return result;
  };

  return importer;
};

module.exports = ElasticImporter;
