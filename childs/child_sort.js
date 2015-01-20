'use strict';

var fs = require('fs'),
	util = require('util'),
	HeapSort = require('../streams/heapSort'),
	JobStatus = require('../utils/jobStatus'),
	Writable = require('stream').Writable;

var processId = null,
	processState = null,
	isInited = false,
	isCompleted = false,
	tasks = [],
	fileName = null,
	jobStatus = null,
	heapSort = null;

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

	if (message.action === 'sort' &&
		jobStatus.get('nextLine') <= jobStatus.get('endLine')) {
		doSort(jobStatus.get('fn'));
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

	var newStatus = {
		startLine: options.startLine,
		endLine: options.endLine,
		nextLine: options.startLine,
		fn: options.fn,
		partSize: options.partSize,
		fileCounter: 0
	};

	jobStatus = new JobStatus(continueLastJob, processId, './state/child_state_' + processId + '.json', newStatus);
}

/**
 * Write sorted data to file.
 */
var writeSortedData = function(chunk, enc, next) {
	fileName = './data/part_' +
		processId +
		'_' +
		jobStatus.get('fileCounter') +
		'.txt';

	fs.writeFile(fileName, chunk, onFinishWrite);

	jobStatus.set('fileCounter', jobStatus.get('fileCounter') + 1);
	jobStatus.set('nextLine', heapSort.getNextLine());

	next();
}

/**
 * Start new sort job. The process starts with divide the file on parts and sort
 * them separately.
 */
function doSort(fn) {
	var stream = fs.createReadStream(fn, 'utf8'),
		ws = new Writable();

	ws._write = writeSortedData;
	heapSort = new HeapSort(getSortOptions());

	heapSort.on('data', onSortData);
	heapSort.on('finish', onSortFinish);

	stream
		.pipe(heapSort)
		.pipe(ws);
}

/**
 * Sorted data is completed and ready to be saved on disk.
 */
function onSortData() {
	tasks.push({
		task: 'newData'
	});
}

/**
 * All sort tasks are finished.
 */
function onSortFinish() {
	isCompleted = true;

	if (tasks.length === 0) {
		idle();
	}
}

/**
 * All write tasks are finished.
 */
function onFinishWrite(err) {
	if (err) {
		throw err;
	}

	sendMessage({
		file: fileName
	});

	tasks.pop();
	if (isCompleted && tasks.length === 0) {
		idle();
	}
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
	heapSort = null;
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
		type: 'sort'
	}, prop);
	process.send(message);
}

/**
 * Get the options necessary for the main sort transorm stream.
 */
function getSortOptions() {
	return {
		fn: jobStatus.get('fn'),
		startLine: jobStatus.get('nextLine'),
		endLine: jobStatus.get('endLine'),
		partSize: jobStatus.get('partSize')
	};
}