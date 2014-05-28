#!/usr/bin/env node

var es = require('event-stream');
var Liftoff         = require('liftoff');
var program         = require('commander');
var ElasticImporter = require('../lib/elastic-importer');

var ElasticImport   = new Liftoff({
  name           : 'elastic-import',
  moduleName     : 'elastic-import',
  configName     : 'elastic-import',
  configPathFlat : 'config',
  extensions     : {
    '.js'   : null,
    '.json' : null
  }
});

ElasticImport.launch(function(env){
  var config = require(env.configPath);
  program
    .version('0.0.1')
    .option('-q , --query [query]'         , 'SQL Query')
    .option('-t , --type [type]'           , 'Elasticsearch Type')
    .option('-i , --index [index]'         , 'Index name')
    .option('-b , --batchSize [batchSize]' , 'Batchsize')
    .option('-s , --sync'                  , 'Wait IO Mysql/Elasticsearch')
    .parse(env.argv);

  var importer = new ElasticImporter(config.mysql, config.elasticsearch);
  var importStream = importer.doImport(program);
  // importStream.pipe(es.stringify()).pipe(process.stdout);
  importStream.on('end', importer.close);

}, process.argv);
