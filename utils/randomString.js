'use strict';

var _ = require('underscore')._;

var strSrc = 'abcdefghijklmnopqrstuvwxyz0123456789',
	srcArr = strSrc.split("");

module.exports.get = get;

/**
 * Get random string.
 */
function get(length) {
	var length = length || 16;
	srcArr = _.shuffle(srcArr);
	var srcSample = _.sample(srcArr, length);
	return srcSample.join('');
}