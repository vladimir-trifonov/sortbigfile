'use strict';

var os = require('os'),
	Readable = require('stream').Readable,
	randomString = require('../utils/randomString'),
	util = require('util');

/**
 * Readable stream which generate random strings.
 */
function StringGenerator(options, size, strLen) {
	if (!(this instanceof StringGenerator)) {
		return new StringGenerator(options);
	}

	var options = options || {};

	this.size = size || 1000000;
	this.rowsPerChunk = 1000;
	this.strLen = strLen || 64;

	Readable.call(this, options);
	return this;
}

util.inherits(StringGenerator, Readable);

/**
 * Generate random string.
 */
StringGenerator.prototype._generateChunk = function() {
	var rows = this.rowsPerChunk;
    var chunk = '';

    for (var i = 0; i < rows; i++) {
        chunk += randomString.get(this.strLen) + os.EOL;
    }

    return chunk;
};

/**
 * Send the string through pipe.
 */
StringGenerator.prototype._read = function() {
	var chunk = this._generateChunk(),
        chunkSize = Buffer.byteLength(chunk, 'utf8');

    this.size -= chunkSize;
    this.push(chunk);
    if (this.size <= 0) {
        this.push(null);
    }
};

module.exports = StringGenerator;
