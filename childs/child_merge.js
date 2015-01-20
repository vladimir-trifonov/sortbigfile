'use strict';

var fs = require('fs'),
	util = require('util'),
	os = require('os'),
	JobStatus = require('../utils/jobStatus'),
	splitFiles = require('../utils/splitFiles'),
	Writable = require('stream').Writable;

var processId = null,
	processState = null,
	isInited = false,
	fileName = null,
	files = null,
	jobStatus = null;

//Ready to receive messages from the parent process.
process.on('message', onNewMessage);

/**
 * Handle messages sent from parent process.
 */
function onNewMessage(message) {
	var options = message.options || {};

	if (!isInited) {
		init(message.processId, message.continueLastJob, options);
	}

	busy();
	files = options.files;

	if (message.action === 'merge') {
		doMerge();
	} else {
		idle();
	}
}

/**
 * Initialize child process
 */
function init(id, continueLastJob, options) {
	isInited = true;
	processId = id;

	jobStatus = new JobStatus(continueLastJob, processId, './state/child_state_' + processId + '.json');
	jobStatus.set('fileCounter', 0);
}

/**
 * Merge already sorted files.
 */
function doMerge() {
	//Temporary file
	fileName = './data/part_' +
		processId +
		'_' +
		jobStatus.get('fileCounter') +
		'.txt';

	//Use writeStream to write the merged data
	var writeStream = fs.createWriteStream(fileName);
	writeStream.on('finish', onFinishMerged);

	jobStatus.set('fileCounter',  jobStatus.get('fileCounter') + 1);

	//Merge sorted files
	splitFiles(files)
		.pipe(writeStream);
}

/**
 * Notify the parent that the merge is completed. Also remove source files received from
 * parent and gone in idle state waiting for new tasks.
 */
function onFinishMerged() {
	sendMessage({
		file: fileName,
		count: files.length
	});

	files.forEach(function(file) {
		fs.unlink(file, function(err) {
			if (err) {
				throw err;
			}
		});
	});

	idle();
}

/**
 * Set process state - busy.
 */
function busy() {
	processState = 'busy';
	sendMessage({
		processState: processState
	});
}

/**
 * Set the process state - idle.
 */
function idle() {
	processState = 'idle';

	sendMessage({
		processState: processState
	});
}

/**
 * Send a message to the parent.
 */
function sendMessage(prop) {
	var message = util._extend({
		id: processId,
		type: 'merge'
	}, prop);
	process.send(message);
}