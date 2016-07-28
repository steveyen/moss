//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package moss

import (
	"io"
	"math"
	"sync"
)

// STORE_ITERATOR_CONCURRENT_PAGES_PER_READ is the default, maximum
// number of pages that will be retrieved per Read() if an iterator
// uses explicit Read()'s instead of mmap()'ed memory.  See also:
// STORE_PAGE_SIZE.
var STORE_ITERATOR_CONCURRENT_PAGES_PER_READ = 32

// STORE_ITERATOR_CONCURRENT_NUM_READERS is the default number of
// concurrent reader goroutines that will be used per iterator if an
// iterator uses explicit Read()'s instead of mmap()'ed memory.
var STORE_ITERATOR_CONCURRENT_NUM_READERS = 2

// --------------------------------------------------------------

// An iteratorConcurrent implements the Iterator interface, and is
// based on multiple, concurrent, reader goroutines that split up the
// overall iteration into multiple units of work.  Each unit of work
// results in an iterator that is sequenced together back into the
// overall result that the application will see.
type iteratorConcurrent struct {
	options *IteratorOptions

	startKeyInclusive []byte
	endKeyExclusive   []byte

	closer io.Closer

	stopCh     chan struct{}                // Will be closed when Close()'ed.
	readCh     chan *iteratorConcurrentWork // Readers recv read requests from here.
	readDoneCh chan *iteratorConcurrentWork // Readers send ready iterators to here.
	seq        []*iteratorConcurrentWork    // Used to order the ready iterators.
	seqCh      chan *iteratorConcurrentWork // Ordered sequence of ready iterators.

	readersWaitGroup sync.WaitGroup // Waits for all reader goroutines to finish.

	currWork *iteratorConcurrentWork
}

type iteratorConcurrentWork struct {
	seg *segment // The resulting segment when the Read() is done.

	op uint64
	k  []byte
	v  []byte

	idx    int // An index into the iteratorConcurrent.seq array.
	pos    int
	posEnd int
	err    error // The error from the Read().
}

// --------------------------------------------------------------

func (it *iteratorConcurrent) Close() error {
	close(it.stopCh)

	if it.closer != nil {
		it.closer.Close()
		it.closer = nil
	}

	return nil
}

func (it *iteratorConcurrent) Current() ([]byte, []byte, error) {
	work := it.currWork
	if work == nil {
		return nil, nil, ErrIteratorDone
	}
	return work.k, work.v, nil
}

func (it *iteratorConcurrent) CurrentEx() (EntryEx, []byte, []byte, error) {
	work := it.currWork
	if work == nil {
		return EntryEx{}, nil, nil, ErrIteratorDone
	}
	return EntryEx{Operation: work.op}, work.k, work.v, nil
}

func (it *iteratorConcurrent) Next() error {
	work := it.currWork
	if work == nil {
		return ErrIteratorDone
	}

	work.pos++
	if work.pos < work.posEnd {
		work.op, work.k, work.v = work.seg.GetOperationKeyVal(work.pos)
		if work.op != 0 {
			return nil
		}
	}

	it.currWork = nil

	return it.nextIterator()
}

func (it *iteratorConcurrent) nextIterator() error {
	select {
	case <-it.stopCh:
		return ErrIteratorDone

	case work, ok := <-it.seqCh:
		if !ok || work == nil {
			return ErrIteratorDone
		}

		if work.err != nil {
			return work.err
		}

		it.currWork = work
	}

	return nil
}

// --------------------------------------------------------------

func (f *Footer) optimizeIter(iter Iterator, mref *mmapRef) Iterator {
	itr, ok := iter.(*iterator)
	if !ok || len(itr.cursors) != 1 || itr.ss.lowerLevelSnapshot != nil {
		return nil
	}

	sio, ok := itr.iteratorOptions.Extra.(*StoreIteratorOptions)
	if !ok || sio == nil {
		sio = &StoreIteratorOptions{}
	}

	cur := itr.cursors[0]
	if cur.ssIndex < 0 {
		return nil
	}

	numItems := cur.posEnd - cur.pos
	if numItems <= sio.MaxItemsForMMap {
		return nil
	}

	sloc := &f.SegmentLocs[cur.ssIndex]
	if sloc.Kind != BASIC_SEGMENT_KIND {
		return nil
	}

	seg, ok := itr.ss.a[cur.ssIndex].(*segment)
	if !ok {
		return nil
	}

	_, begKStart, _, _, _ :=
		seg.GetOperationKeyValOffsets(cur.pos)

	_, _, _, endVStart, endVLen :=
		seg.GetOperationKeyValOffsets(cur.posEnd - 1)

	totBufLen := endVStart + endVLen - begKStart

	avgSizePerItem := int(totBufLen / uint64(numItems))

	pagesPerRead := sio.PagesPerRead
	if pagesPerRead <= 0 {
		pagesPerRead = STORE_ITERATOR_CONCURRENT_PAGES_PER_READ
	}

	maxReadSize := pagesPerRead * STORE_PAGE_SIZE

	avgItemsPerRead := maxReadSize / avgSizePerItem
	if avgItemsPerRead <= 0 {
		avgItemsPerRead = 1
	}

	numReads := int(math.Ceil(float64(numItems) / float64(avgItemsPerRead)))

	numReaders := sio.NumReaders
	if numReaders <= 0 {
		numReaders = STORE_ITERATOR_CONCURRENT_NUM_READERS
	}
	if numReaders > numReads {
		numReaders = numReads
	}

	it := &iteratorConcurrent{
		options:           &itr.iteratorOptions,
		startKeyInclusive: itr.startKeyInclusive,
		endKeyExclusive:   itr.endKeyExclusive,
		closer:            mref, // Takes over mref from caller.
		stopCh:            make(chan struct{}),
		readCh:            make(chan *iteratorConcurrentWork),
		readDoneCh:        make(chan *iteratorConcurrentWork, numReaders),
	}

	if numReaders > 1 {
		it.seq = make([]*iteratorConcurrentWork, numReads)
		it.seqCh = make(chan *iteratorConcurrentWork, numReaders)

		go it.runReadDone()
	} else {
		it.seqCh = it.readDoneCh
	}

	for i := 0; i < numReaders; i++ {
		it.readersWaitGroup.Add(1)

		mref.fref.AddRef()

		go it.runReader(mref.fref, sloc, seg, itr.ss.options)
	}

	go it.runReadRequests(cur.pos, cur.posEnd, avgItemsPerRead)

	it.nextIterator()

	return it
}

// --------------------------------------------------------------

// runReadRequests feeds the readCh with read requests.
func (it *iteratorConcurrent) runReadRequests(
	fullPosBeg, fullPosEnd, posIncr int) {
	idx := 0
	pos := fullPosBeg

	for pos < fullPosEnd {
		posEnd := pos + posIncr
		if posEnd > fullPosEnd {
			posEnd = fullPosEnd
		}

		it.readCh <- &iteratorConcurrentWork{
			idx:    idx,
			pos:    pos,
			posEnd: posEnd,
		}

		idx++
		pos = posEnd
	}

	close(it.readCh)

	it.readersWaitGroup.Wait()

	close(it.readDoneCh)
}

// runReadDone handles the readDoneCh by sorting the incoming ready
// iterators and feeding the seqCh.
func (it *iteratorConcurrent) runReadDone() {
	defer close(it.seqCh)

	readDoneCh := it.readDoneCh

	var numRecv int
	var numSent int

	for numSent < len(it.seq) {
		var seqCh chan *iteratorConcurrentWork
		if it.seq[numSent] != nil {
			seqCh = it.seqCh
		}

		select {
		case <-it.stopCh:
			return

		case work, ok := <-readDoneCh:
			if !ok || work == nil {
				readDoneCh = nil
			} else {
				it.seq[work.idx] = work

				numRecv++
			}

		case seqCh <- it.seq[numSent]:
			it.seq[numSent] = nil // Allows GC.

			numSent++
		}
	}
}

// runReader implements the reader goroutine loop.
func (it *iteratorConcurrent) runReader(fref *FileRef,
	sloc *SegmentLoc, seg *segment, options *CollectionOptions) {
	defer fref.DecRef()

	defer it.readersWaitGroup.Done()

	for {
		select {
		case <-it.stopCh:
			return

		case work, ok := <-it.readCh:
			if !ok || work == nil {
				return
			}

			_, begKStart, _, _, _ :=
				seg.GetOperationKeyValOffsets(work.pos)

			_, _, _, endVStart, endVLen :=
				seg.GetOperationKeyValOffsets(work.posEnd - 1)

			bufLen := endVStart + endVLen - begKStart

			buf := make([]byte, bufLen)

			bufOffset := int64(sloc.BufOffset + begKStart)

			nread, err := fref.file.ReadAt(buf, bufOffset)
			if err != nil || nread != len(buf) {
				work.err = err
			} else {
				work.seg = &segment{
					kvs:       seg.kvs,
					buf:       buf,
					bufOffset: -int64(begKStart),
				}

				work.op, work.k, work.v = work.seg.GetOperationKeyVal(work.pos)
			}

			select {
			case <-it.stopCh:
				return
			case it.readDoneCh <- work:
				// NO-OP.
			}
		}
	}
}
