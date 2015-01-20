'use strict';

var fs = require('fs'),
    _ = require('underscore');

module.exports = JobStatus;

function JobStatus(restore, id, fn, newStatus) {
    this.id = id;
    this.state = {};

    if(newStatus) {
        this.state[this.id] = _.clone(newStatus);
    } else {
        this.state[this.id] = {};
    }


    this.fn = fn;

    if (restore) {
        this._restore();
    }

    return this;
}

/**
 * Read the job status from the file system.
 */
JobStatus.prototype._read = function() {
    var state = {};

    try {
        state = require('./state/' + this.fn);
    } catch (e) {
        state = {};
        state[this.id] = {};
    }

    return state;
};

JobStatus.prototype.save = function() {
    this._save();
};

/**
 * Write the job status to the storage.
 */
JobStatus.prototype._save = function() {
    fs.writeFile(this.fn, JSON.stringify(this.state), function(err, res) {
        if (err) {
            return console.log(err);
        }
    });
};

/**
 * Restore previous(not completed) job status.
 */
JobStatus.prototype._restore = function() {
    var _self = this,
        previousStates = this._read();
    if (previousStates) {
        Object.keys(previousStates).forEach(function(previousState) {
            var state = previousStates[previousState];
            Object.keys(state).forEach(function(subkey) {
                 _self.state[previousState][subkey] = previousStates[previousState][subkey];
            });
        });
    }
};

JobStatus.prototype.get = function(key) {
    return this.state[this.id][key];
};

JobStatus.prototype.set = function(key, value) {
    this.state[this.id][key] = value;
    this._save();
};

JobStatus.prototype.push = function(key, value) {
    this.state[this.id][key].push(value);
    this._save();
};

JobStatus.prototype.pop = function(key, value) {
    var val = this.state[this.id][key].pop();
    this._save();
    return val;
};