#!/usr/bin/env node

var Liftoff         = require('liftoff');
var program         = require('commander');
var ElasticImporter = require('../lib/elastic-importer');
var speedometer     = require('speedometer');

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
  importStream.on('end', importer.close);
  var total = 0;
  var start = new Date().getTime();
  var speed = speedometer();
  importStream.on('data', function(data){
    var docs = data.items.length;
    var recordsPerSecond = speed(docs);
    total += docs;
    //console.log('\033[2J');
    console.log('Importing Records: ~' + recordsPerSecond +' records/s');
  });
  importStream.on('end', function(){
    var end = new Date().getTime();
    console.log('Imported ' + total + ' records. @~'+ (total / ((end-start)/1000) )+'records/s :) Happy Coding!')
  });
}, process.argv);
