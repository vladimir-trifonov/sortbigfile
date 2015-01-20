'use strict';


module.exports = heapSort;

/**
 * Heap sort algorithm.
 */
function heapSort(array) {
    var size = array.length,
        temp = null;

    //Put the elements of the array in heap order.
    heapify(array, size);

    //Repeatedly extracts the maximum and restores the heap order.
    for (var i = size - 1; i >= 1; i--) {
        temp = array[0];
        array[0] = array[i];
        array[i] = temp;
        siftDown(array, 0, i - 1);
    }

    return array;
}

function siftDown(array, root, bottom) {
    var done = 0,
        maxChild = null,
        temp = null;

    while (root * 2 <= bottom && !done) {
        if (root * 2 == bottom) {
            maxChild = root * 2;
        }
        else if (array[root * 2] > array[root * 2 + 1]) {
            maxChild = root * 2;
        }
        else {
            maxChild = root * 2 + 1;
        }

        if (array[root] < array[maxChild]) {
            temp = array[root];
            array[root] = array[maxChild];
            array[maxChild] = temp;
            root = maxChild;
        } else {
            done = 1;
        }
    }
}

function heapify(array, size) {
	for (var i = (size / 2) - 1; i >= 0; i--) {
		siftDown(array, i, size);
	}
}