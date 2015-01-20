'use strict';

module.exports.replaceAll = replaceAll;

/**
 * Replace all occurrences in string.
 */
function replaceAll(str, mapObj) {
	var regExp = new RegExp(Object.keys(mapObj).join('|'), 'gi');

	return str.replace(regExp, function(matched) {
		return mapObj[matched.toLowerCase()];
	});
}

