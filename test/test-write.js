'use strict';
var expect = require('chai').expect;
var WritableBulk = require('..').WritableBulk;
var TransformToBulk = require('..').TransformToBulk;
var client = new require('elasticsearch').Client({log: 'warning'});
var fs = require('fs');
var split = require('split');
var streamify = require("async-stream-generator");

describe('When writing', function() {
  var docNumber = 10;
  var err;
  var ndjsonIndex = 'test';
  var transform = function(doc) {
    console.log(doc);
    var index = (doc.entityType === ndjsonIndex) ? ndjsonIndex : 'myindex2';
    if (doc._id) {
      var docId = doc._id;
      doc._id = undefined;
      return { _index: index, _id: docId };
    } else {
      return { _index: index };
    }
  };

  before(function(done) {
    var ws1 = new WritableBulk(function(bulkCmds, callback) {
      client.bulk({
        index : 'myindex2',
        type  : 'mytype',
        body  : bulkCmds
      }, callback);
    });
    ws1.on('error', function(e) {
      err = e;
    });

    // drop the index then
    client.indices.delete({index: 'myindex2'}, function() {
      var idx = 0;

      // stream 42 random docs into ES
      var contentGenerator = (async function * (max) {
        var doc = { "content": Math.random() };
        while (--max) {
          yield (doc);
        }
        return doc;
      }) (docNumber + 1);
      contentGenerator.prototype = {};

      require('util').inherits(contentGenerator, require('readable-stream').Readable);

      contentGenerator.prototype._read = function () {
        this.push(this.next());
      };

      streamify(contentGenerator)
          .on('data', d => console.log(++idx))
          .pipe(new TransformToBulk(transform)).pipe(ws1);
    });


    var ts2 = new TransformToBulk(function(doc) {
      console.log(doc);
      var index = (doc.entityType === ndjsonIndex) ? ndjsonIndex : 'test';
      if (doc._id) {
        var docId = doc._id;
        doc._id = undefined;
        return { _index: index, _id: docId };
      } else {
        return { _index: index };
      }
    });

    var ws2 = new WritableBulk(function(bulkCmds, callback) {
      client.bulk({
        index : ndjsonIndex,
        type  : 'entity',
        body  : bulkCmds
      }, callback);
    });
    ws2.on('error', function(e) {
      err = e;
    }).on('close', function() {
      done(err);
    });

    // drop the index then
    client.indices.delete({index: ndjsonIndex}, function() {
      var readStream = fs.createReadStream('test/test.ndjson');
      var idx = 0;

      readStream
          .pipe(split(JSON.parse, null, { trailing: false }))
          .on('data', d => console.log(++idx))
        .pipe(new TransformToBulk(transform)).pipe(ws2);

    });
  });

  it('Must have indexed ' + docNumber + ' docs', function(done) {
    setTimeout(
        client.indices.refresh,
        1500,
        { index: 'myindex2' },
        function() {

          client.count({
            index: 'myindex2',
            type: 'mytype'
          }, function (e, res) {
            if (e) {
              return done(e);
            }
            expect(res.count).to.equal(docNumber);
            done();
          });

      }
    );
  });


  it('Must read docs from ndJSON', function(done) {

    setTimeout(
        client.indices.refresh,
        1500,
        { index: ndjsonIndex },
        function() {

          client.count({
            index: ndjsonIndex,
            type: 'entity'
          }, function (e, res) {
            console.log(res);
            if (e) {
              return done(e);
            }
            expect(res.count).to.equal(docNumber);
            done();
          });

      }
    );
  });
});
