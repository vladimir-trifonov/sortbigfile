/**
 * Generate file filled with random strings separated by new line.
 * Sample usage: node generate --output=sample.txt --size=1gb
 */

'use strict';

var fs = require('fs'),
	argv = require('minimist')(process.argv.slice(2)),
	StringGenerator = require('./streams/stringGenerator'),
	helper = require('./helpers');

//The default size of the generated file is 1MB.
var size = 1048576,
	fileOut = argv.output || './output.txt',
	sizeIn = (typeof argv.size !== 'undefined' ? argv.size.toLowerCase() : null);

if (sizeIn !== null) {
	sizeIn = helper.parseFileSize(sizeIn);
	if (isNaN(sizeIn)) {
		return console.log('Error: Wrong input format(size)!');
	}

	if(sizeIn > 1) {
		size *= sizeIn;
	}
}

generateFile(fileOut, size, argv.strLen);

/**
 * Generate file with random string data.
 */
function generateFile(fn, size, strLen) {
	var writeStream = fs.createWriteStream(fn),
		stringGenerator = new StringGenerator(null, size, strLen);

	stringGenerator
		.pipe(writeStream);
}

