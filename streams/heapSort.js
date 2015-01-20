'use strict';

var stream = require('stream'),
	util = require('util'),
	os = require('os'),
	heapSort = require('../utils/heapSort');

var Transform = stream.Transform || require('readable-stream').Transform;
util.inherits(HeapSort, Transform);

/**
 * Transform stream which create buffer, sort it and send it through pipe.
 */
function HeapSort(options) {
	if (!(this instanceof HeapSort)) {
		return new HeapSort(null);
	}

	this._lines = [];
	this._linesCount = 0;
	this._startLine = options.startLine || 0;
	this._endLine = options.endLine;
	this._partSize = options.partSize;
	this._currPartSize = this._partSize;
	this._lastLineData = null;

	Transform.call(this, null);
	return this;
}

/**
 * Transform stream which create buffer, sort it and send it through pipe.
 */
HeapSort.prototype._transform = function(chunk, enc, cb) {
	var _self = this;

	var data = chunk.toString('utf8');
	var chunkSize = Buffer.byteLength(data, 'utf8');

	if (this._lastLineData) {
		data = this._lastLineData + data;
	}

	var lines = data.split(os.EOL);

	//In order to avoid chunk of data to get cut off in the middle of a line,
	//we splice out the last line we find so it does not push to the consumer.
	this._lastLineData = lines.splice(lines.length - 1, 1)[0];

	var _isCompleted = lines.some(function(line) {
		//Check if all lines separated for this stream are readed.
		if (_self._linesCount > _self._endLine) {
			_self.push(_self._getPushData.call(_self));
			_self._lines = [];

			return true;
		}

		//Check if the lines received from the streams are in bounds and are ready for buffering.
		if (_self._linesCount >= _self._startLine) {
			_self._lines.push(line);
		}

		_self._linesCount++;
	});

	//Close stream if line index of out of the bounds.
	if (_isCompleted) {
		this.push(null);
	}

	this._currPartSize -= chunkSize;

	//Check if the bufer is full.
	if (this._currPartSize <= 0) {
		this.push(this._getPushData());

		this._lines = [];
		this._currPartSize = this._partSize;
	}

	cb();
};

/**
 * When the last call to _transform happens, we have a _lastLineData value sitting around that never got pushed.
 * After all the source data has been read and transformed, the _flush method will be called.
 */
HeapSort.prototype._flush = function() {
	if (this._lastLineData) {
		this._lines.push(this._lastLineData);
		this._lastLineData = null;
	}

	if (this._lines.length > 0) {
		this.push(this._getPushData());
	}
	this._lines = null;
};

/**
 * Heap Sort.
 * Sort the lines in the bufer and returns them prepared to be sent through pipe.
 */
HeapSort.prototype._getPushData = function() {
	return heapSort(this._lines).join(os.EOL);
};

/**
 * Returns the current line from the processed file.
 */
HeapSort.prototype.getNextLine = function() {
	return this._linesCount;
};

module.exports = HeapSort;