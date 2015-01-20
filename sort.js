/**
 * Master process. Sort given file.
 * Sample usage: node sort --input=sample.txt
 */
'use strict';

var fs = require('fs'),
	Q = require('q'),
	_ = require('underscore'),
	child_process = require('child_process'),
	helper = require('./helpers');

var argv = require('minimist')(process.argv.slice(2)),
	partSize = 1048576 * 128,
	continueLastJob = argv.continueLastJob || false,
	filePath = argv.input || './input.txt',
	output = argv.output || './data/output.txt',
	JobStatus = require('./utils/jobStatus'),
	nextId = 0,
	childs = {
		sort: {
			process: {},
			processState: {}
		},
		merge: {
			process: {},
			processState: {}
		}
	},
	cpusCount = null,
	jobStatus = null,
	freeCpus = null;

if (typeof filePath === 'undefined') {
	return console.log('Error: Missing input(source file)!');
}

/**
 * initialize the main process.
 */
function init() {
	var newStatus = {
		sortedFiles: [],
		mergeJobsCount: 0,
		fn: filePath
	};

	jobStatus = new JobStatus(continueLastJob, -1, './state/parent_child_state.json', newStatus);

	splitTasks();
	checkCanMerge();
}

/**
 * Create sort tasks and send them to the child's processes.
 */
function splitTasks() {
	helper.lineCounter(filePath).done(function(linesCount) {
		var linesPerProcess = Math.ceil(linesCount / helper.getCpus().length);

		cpusCount = helper.getCpus().length;
		freeCpus = cpusCount;

		for (var i = 0; i < cpusCount; i++) {
			var startLine = linesPerProcess * i,
				endLine = (linesPerProcess * (i + 1)) - 1;

			startSortTask(startLine, endLine);

			nextId++;
			freeCpus--;
		}
	});
}

/**
 * Handles child's messages.
 */
function onChildMessage(message) {
	//console.log('Child state: ' + JSON.stringify(message));

	if (message.processState) {
		childs[message.type].processState[message.id] = message.processState;
	}

	if (message.type === 'sort') {
		if (message.processState === 'idle') {
			var process = childs[message.type].process[message.id];

			//Close the sort process and release the cpu for new processes.
			process.kill();
			process = null;

			childs[message.type].processState[message.id] = null;
		}

		if (message.file) {
			jobStatus.set('mergeJobsCount', jobStatus.get('mergeJobsCount') + 1/2);
		}
	}

	if (message.file) {
		jobStatus.push('sortedFiles', message.file);

		if (message.type === 'merge') {
			jobStatus.set('mergeJobsCount', jobStatus.get('mergeJobsCount') - message.count / 2);
		}
	}

	checkCanMerge();
}

/**
 * Releases the cpu.
 */
function onChildClose() {
	freeCpus++;
	checkCanMerge();
}

/**
 * Check are there enought sorted files for merging.
 */
function checkCanMerge() {
	if (jobStatus.get('sortedFiles').length === 0) {
		return;
	}

	//When all merge tasks are completed notify at the console and close all child processes.
	if (jobStatus.get('sortedFiles').length === 1 && jobStatus.get('mergeJobsCount') <= 0) {
		var mergedFile = jobStatus.pop('sortedFiles');
		if (mergedFile) {
			fs.rename(mergedFile, output, function(err) {
				if (err) {
					throw err;
				}

				console.log('Sorted file path: ' + output);

				tryKillAllProcesses();
			});

			jobStatus.set('mergeJobsCount', 0);
		}

		return;
	}

	//Check is there free merge process.
	Object.keys(childs.merge.processState).forEach(function(id) {
		if (childs.merge.processState[id] === 'idle') {
			startMergeTask(id);
		}
	});

	//Check is there free cpu for new merge process.
	for (var i = 0, cpus = freeCpus; i < cpus; i++) {
		if (startMergeTask()) {
			freeCpus--;
			nextId++;
		}
	}
}

/**
 * Close all child processes which are not in busy state.
 */
function tryKillAllProcesses() {

	Object.keys(childs).forEach(function(type) {
		Object.keys(childs[type].process).forEach(function(id) {

			if (childs[type].processState[id] === 'idle') {
				childs[type].process[id].kill();
			}

		});
	});
}

/**
 * Start new sort tasks.
 */
function startSortTask(startLine, endLine) {
	//Create new child process and dedicate it for sort tasks.
	var child = child_process.fork('./childs/child_sort.js');

	var message = {
		action: 'sort',
		processId: nextId,
		continueLastJob: continueLastJob,
		options: {
			startLine: startLine,
			endLine: endLine,
			fn: filePath,
			partSize: partSize
		}
	};

	childs.sort.process[nextId] = child;
	child.on('message', onChildMessage);
	child.on('close', onChildClose);

	child.send(message);
}

/**
 * Start new merge tasks. Take two already sorted files and sent them to the child which have to
 * merge them and returns the result file.
 */
function startMergeTask(id) {
	if (jobStatus.get('sortedFiles').length < 2) {
		return;
	}

	var length = jobStatus.get('sortedFiles').length;
	var files = [];
	for(var i = 0; i < length; i++) {
		files.push(jobStatus.pop('sortedFiles'));
	}

	var child = null;
	var message = {
		action: 'merge',
		options: {
			files: files
		}
	};

	if (id) {
		//Use already dedicated for merge tasks child process.
		child = childs.merge.process[id];
	} else {
		//Create new child process and dedicate it for merge tasks.
		child = child_process.fork('./childs/child_merge.js');

		message.processId = nextId;
		message.continueLastJob = continueLastJob;

		childs.merge.process[nextId] = child;
		child.on('message', onChildMessage);
		child.on('close', onChildClose);
	}

	child.send(message);

	return true;
}

init();