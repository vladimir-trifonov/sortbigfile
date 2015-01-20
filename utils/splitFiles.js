/**
 * External merge of sorted files
 *
 * It opens simultaneously multiple files, reads the lowest value, 
 * storing it in buffer and push the buffer through stream
 */

'use strict';

var fs = require('fs'),
    Q = require('q'),
    async = require('async'),
    os = require('os'),
    _ = require('underscore'),
    Readable = require('stream').Readable;

var readBufferSize = 10 * 1048576;

module.exports = splitFiles;

function splitFiles(files) {
    var chunks = files.length,
        lowestIndex = null,
        lowestValue = null,
        done = false,
        outBufferMaxLength = 1000000,
        buffer = [];

    var out = new Readable();
    out._read = function() {
        if (done === true) {
            this.push(null);
        }
    };

    openFiles(files).then(function(readers) {
        initBuffers(readers).then(function() {
            while (!done) {
                lowestIndex = -1;
                lowestValue = '';
                for (var j = 0; j < chunks; j++) {

                    if (readers[j].data !== null) {
                        if (lowestIndex < 0 || readers[j].data[0] < lowestValue) {
                            lowestIndex = j;
                            lowestValue = readers[j].data[0];
                        }
                    }
                }

                if (lowestIndex === -1) {
                    done = true;
                    break;
                }

                buffer.push(lowestValue + os.EOL);
                if (buffer.length >= outBufferMaxLength) {
                    out.push(buffer.join(''));
                    buffer = [];
                }

                readers[lowestIndex].data.shift();
                if (readers[lowestIndex].data.length === 0) {
                    loadBufferSync(readers[lowestIndex]);
                    if (readers[lowestIndex].data.length === 0) {
                        readers[lowestIndex].data = null;
                    }
                }
            }

            var fds = _.map(readers, function(reader) {
                return reader.fd;
            });

            if (buffer.length > 0) {
                out.push(buffer.join(''));
                buffer = [];
            }

            out.push(null);
            buffer = null;
            closeFiles(fds).done(function() {
                readers = null;
            });

        }, function(err) {
            console.log('Error: ' + err);
        });
    }, function(err) {
        console.log('Error: ' + err);
    });

    return out;
}

function initBuffers(readers) {
    var deferred = Q.defer(),
        queues = [];
    async.each(readers, function(reader, cb) {
        loadBuffer(reader).then(function() {
            cb();
        }, function(err) {
            cb(err);
        });
    }, function(err) {
        if (err) {
            console.log('Error: ' + err);
            deferred.reject(err);
        } else {
            deferred.resolve(queues);
        }
    });
    return deferred.promise;
}

function loadBufferSync(reader) {
    return loadBuffer(reader, true);
}

function loadBuffer(reader, isSync) {
    var deferred = Q.defer(),
        data = new Buffer(readBufferSize);

    var fsRead = (isSync === true ? fs.readSync : fs.read);

    if (isSync) {
        var bytes = fsRead(reader.fd, data, 0, readBufferSize, reader.filePosition);
        onData(null, bytes, data);

    } else {
        fsRead(reader.fd, data, 0, readBufferSize, reader.filePosition, onData);
        return deferred.promise;
    }

    function onData(err, bytesRead, data) {
        if (err) {
            console.log('Error: ' + err);
            return deferred.reject(err);
        }

        reader.filePosition += bytesRead;

        if (!data) {
            reader.data = null;
            return deferred.resolve();
        }

        data = data.toString('utf8', 0, bytesRead);

        if (reader.lastLineData !== null) {
            data = reader.lastLineData + data;
        }

        var lines = data.split(os.EOL);
        if (bytesRead !== 0) {
            var lastLineData = lines.splice(lines.length - 1, 1)[0];

            reader.lastLineData = lastLineData.trim();
            reader.data = lines;
        } else {
            if (reader.lastLineData !== null && reader.lastLineData !== '' && reader.lastLineData !== os.EOL) {
                reader.data = [reader.lastLineData];
            }
            reader.lastLineData = null;
        }
        return deferred.resolve();
    }
}

function closeFiles(fds) {
    var deferred = Q.defer(),
        readers = [];

    async.each(fds, function(fd, cb) {
        fs.close(fd, function(err, fd) {
            if (err) {
                return cb(err);
            }

            cb();
        });
    }, function(err) {
        if (err) {
            console.log('Error: ' + err);
            deferred.reject(err);
        } else {
            deferred.resolve(readers);
        }
    });

    return deferred.promise;
}

function openFiles(files) {
    var deferred = Q.defer(),
        readers = [];

    async.each(files, function(file, cb) {
        fs.open(file, 'r', function(err, fd) {
            if (err) {
                return cb(err);
            }

            var i = files.indexOf(file);

            readers[i] = {
                fd: fd,
                data: [],
                filePosition: 0,
                lastLineData: null
            };
            cb();
        });
    }, function(err) {
        if (err) {
            console.log('Error: ' + err);
            deferred.reject(err);
        } else {
            deferred.resolve(readers);
        }
    });

    return deferred.promise;
}