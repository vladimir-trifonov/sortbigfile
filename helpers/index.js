'use strict';

var os = require('os'),
	fs = require('fs'),
	Q = require('q'),
	replaceAll = require('../utils/utils').replaceAll;

module.exports.parseFileSize = parseFileSize;

/**
 * Converts the user's input size in megabytes.
 */
function parseFileSize(sizeIn) {
	var size = 1;
	if (sizeIn.indexOf('gb') !== -1) {
		//Megabytes to gigabytes.
		size *= 1024;
	}

	//Removing the 'mb' or 'gb'  from the user's input.
	sizeIn = replaceAll(sizeIn, {
		'mb': '',
		'gb': ''
	});

	sizeIn = parseInt(sizeIn, 10);
	if (isNaN(sizeIn)) {
		return sizeIn;
	}

	return size * sizeIn;
}

module.exports.getCpus = getCpus;

/**
 * Get system CPU's count.
 */
function getCpus() {
	return os.cpus();
}

module.exports.lineCounter = lineCounter;

/**
 * Count the lines(line-by-line) in file.
 */
function lineCounter(filePath) {
	var deferred = Q.defer();
	var linesCount = 0;
	var stream = fs.createReadStream(filePath);

	stream.on('data', counter);

	function counter(chunk) {
		linesCount += chunk
			.toString('utf8')
			.split(os.EOL)
			.length - 1;
	}

	stream.on('error', function(err) {
		stream.removeListener('data', counter);
		deferred.reject(err);
	});

	stream.on('end', function() {
		stream.removeListener('data', counter);
		deferred.resolve(linesCount + 1);
	});

	return deferred.promise;
}


