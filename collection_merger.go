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
	"fmt"
	"sync/atomic"
	"time"
)

// A merging represents a concurrent, background merging of a subset
// of a segmentStack.
type merging struct {
	ss         *segmentStack // The segmentStack being merged.
	base       *segmentStack // The base being merged, possibly nil.
	begLevel   int           // The beginning level of ss's levels being merged.
	endHeight  int           // The ending height of the ss's levels being merged.
	outSegment *segment      // The output of the merging.
	cancelCh   chan error    // Closed when the merging is canceled.
}

// NotifyMerger sends a message (optionally synchronously) to the merger
// to run another cycle.  Providing a kind of "mergeAll" forces a full
// merge and can be useful for applications that are no longer
// performing mutations and that want to optimize for retrievals.
func (m *collection) NotifyMerger(kind string, synchronous bool) error {
	atomic.AddUint64(&m.stats.TotNotifyMergerBeg, 1)

	var pongCh chan struct{}
	if synchronous {
		pongCh = make(chan struct{})
	}

	m.pingMergerCh <- ping{
		kind:   kind,
		pongCh: pongCh,
	}

	if pongCh != nil {
		<-pongCh
	}

	atomic.AddUint64(&m.stats.TotNotifyMergerEnd, 1)
	return nil
}

// ------------------------------------------------------

// runMerger() implements the background merger task.
func (m *collection) runMerger() {
	defer func() {
		close(m.doneMergerCh)

		atomic.AddUint64(&m.stats.TotMergerEnd, 1)
	}()

	pings := []ping{}

	defer func() {
		replyToPings(pings)
		pings = pings[0:0]
	}()

OUTER:
	for {
		atomic.AddUint64(&m.stats.TotMergerLoop, 1)

		// ---------------------------------------------
		// Notify ping'ers from the previous loop.

		replyToPings(pings)
		pings = pings[0:0]

		// ---------------------------------------------
		// Wait for new stackDirtyTop entries and/or pings.

		var stopped, mergeAll bool
		stopped, mergeAll, pings = m.mergerWaitForWork(pings)
		if stopped {
			return
		}

		// ---------------------------------------------
		// Atomically ingest stackDirtyTop into stackDirtyMid.

		var stackDirtyTopPrev *segmentStack
		var stackDirtyMidPrev *segmentStack
		var stackDirtyBase *segmentStack

		stackDirtyMid, _, _, _, _ :=
			m.snapshot(snapshotSkipClean|snapshotSkipDirtyBase,
				func(ss *segmentStack) {
					// m.stackDirtyMid takes 1 refs, and
					// stackDirtyMid takes 1 refs.
					ss.refs++

					stackDirtyTopPrev = m.stackDirtyTop
					m.stackDirtyTop = nil

					stackDirtyMidPrev = m.stackDirtyMid
					m.stackDirtyMid = ss

					stackDirtyBase = m.stackDirtyBase
					if stackDirtyBase != nil {
						stackDirtyBase.addRef()
					}

					// Awake all writers that are waiting for more space
					// in stackDirtyTop.
					m.stackDirtyTopCond.Broadcast()
				})

		stackDirtyTopPrev.Close()
		stackDirtyMidPrev.Close()

		// ---------------------------------------------
		// Merge multiple stackDirtyMid layers.

		startTime := time.Now()

		mergerWasOk := m.mergerMain(stackDirtyMid, stackDirtyBase, mergeAll)
		if !mergerWasOk {
			continue OUTER
		}

		// ---------------------------------------------
		// Notify persister.

		m.mergerNotifyPersister()

		// ---------------------------------------------

		atomic.AddUint64(&m.stats.TotMergerLoopRepeat, 1)

		m.fireEvent(EventKindMergerProgress, time.Now().Sub(startTime))
	}

	// TODO: Concurrent merging of disjoint slices of stackDirtyMid
	// instead of the current, single-threaded merger?
	//
	// TODO: A busy merger means no feeding of the persister?
	//
	// TODO: Delay merger until lots of deletion tombstones?
	//
	// TODO: The base layer is likely the largest, so instead of heap
	// merging the base layer entries, treat the base layer with
	// special case to binary search to find better start points?
	//
	// TODO: Dynamically calc'ed soft max dirty top height, for
	// read-heavy (favor lower) versus write-heavy (favor higher)
	// situations?
}

// ------------------------------------------------------

// mergerWaitForWork() is a helper method that blocks until there's
// either pings or incoming segments (from ExecuteBatch()) of work for
// the merger.
func (m *collection) mergerWaitForWork(pings []ping) (
	stopped, mergeAll bool, pingsOut []ping) {
	var waitDirtyIncomingCh chan struct{}

	m.m.Lock()

	if m.stackDirtyTop == nil || len(m.stackDirtyTop.a) <= 0 {
		m.waitDirtyIncomingCh = make(chan struct{})
		waitDirtyIncomingCh = m.waitDirtyIncomingCh
	}

	m.m.Unlock()

	if waitDirtyIncomingCh != nil {
		atomic.AddUint64(&m.stats.TotMergerWaitIncomingBeg, 1)

		select {
		case <-m.stopCh:
			atomic.AddUint64(&m.stats.TotMergerWaitIncomingStop, 1)
			return true, mergeAll, pings

		case ping := <-m.pingMergerCh:
			pings = append(pings, ping)
			if ping.kind == "mergeAll" {
				mergeAll = true
			}

		case <-waitDirtyIncomingCh:
			// NO-OP.
		}

		atomic.AddUint64(&m.stats.TotMergerWaitIncomingEnd, 1)
	} else {
		atomic.AddUint64(&m.stats.TotMergerWaitIncomingSkip, 1)
	}

	pings, mergeAll = receivePings(m.pingMergerCh, pings, "mergeAll", mergeAll)

	return false, mergeAll, pings
}

// ------------------------------------------------------

// mergerMain() is a helper method that performs the merging work on
// the stackDirtyMid and swaps the merged result into the collection.
func (m *collection) mergerMain(stackDirtyMid, stackDirtyBase *segmentStack,
	mergeAll bool) (ok bool) {
	if stackDirtyMid != nil {
		if stackDirtyBase != nil {
			fmt.Printf("  mergerMain: %d, %d, %t\n",
				len(stackDirtyMid.a), len(stackDirtyBase.a), mergeAll)
		} else {
			fmt.Printf("  mergerMain: %d, 0, %t\n",
				len(stackDirtyMid.a), mergeAll)
		}
	}

	if stackDirtyMid != nil && len(stackDirtyMid.a) > 1 {
		if !mergeAll && len(stackDirtyMid.a) > 12 {
			m.mergerStartChildren(stackDirtyMid, stackDirtyBase)

			stackDirtyMid.Close()
			stackDirtyBase.Close()

			return true
		}

		newTopLevel := 0

		if !mergeAll {
			// If we have not been asked to merge all segments,
			// then heuristically calc a newTopLevel.
			newTopLevel = stackDirtyMid.calcTargetTopLevel(0, len(stackDirtyMid.a))
		}

		if newTopLevel <= 0 {
			atomic.AddUint64(&m.stats.TotMergerAll, 1)
		}

		atomic.AddUint64(&m.stats.TotMergerInternalBeg, 1)

		mergedStackDirtyMid, err := stackDirtyMid.merge(newTopLevel, stackDirtyBase)
		if err != nil {
			atomic.AddUint64(&m.stats.TotMergerInternalErr, 1)

			m.Logf("collection: mergerMain stackDirtyMid.merge,"+
				" newTopLevel: %d, err: %v", newTopLevel, err)

			m.OnError(err)

			stackDirtyMid.Close()
			stackDirtyBase.Close()

			return false
		}

		atomic.AddUint64(&m.stats.TotMergerInternalEnd, 1)

		stackDirtyMid.Close()

		mergedStackDirtyMid.addRef()
		stackDirtyMid = mergedStackDirtyMid

		m.m.Lock()
		stackDirtyMidPrev := m.stackDirtyMid
		m.stackDirtyMid = mergedStackDirtyMid
		m.m.Unlock()

		stackDirtyMidPrev.Close()
	} else {
		atomic.AddUint64(&m.stats.TotMergerInternalSkip, 1)
	}

	stackDirtyBase.Close()

	lenDirtyMid := len(stackDirtyMid.a)
	if lenDirtyMid > 0 {
		topDirtyMid := stackDirtyMid.a[lenDirtyMid-1]

		m.Logf("collection: mergerMain, dirtyMid height: %2d,"+
			" dirtyMid top # entries: %d", lenDirtyMid, topDirtyMid.Len())
	}

	stackDirtyMid.Close()

	return true
}

// ------------------------------------------------------

// mergerNotifyPersister() is a helper method that notifies the
// optional persister goroutine that there's a dirty segment stack
// that needs persistence.
func (m *collection) mergerNotifyPersister() {
	if m.options.LowerLevelUpdate == nil {
		return
	}

	m.m.Lock()

	if m.stackDirtyBase == nil &&
		m.stackDirtyMid != nil && len(m.stackDirtyMid.a) > 0 {
		atomic.AddUint64(&m.stats.TotMergerLowerLevelNotify, 1)

		// Cancelling the mergings as the lower level will be
		// logically incorporating the would-have-been-merged data.
		m.cancelMergingsLOCKED(0, len(m.stackDirtyMid.a))

		m.stackDirtyBase = m.stackDirtyMid
		m.stackDirtyMid = nil

		prevLowerLevelSnapshot := m.stackDirtyBase.lowerLevelSnapshot
		m.stackDirtyBase.lowerLevelSnapshot = m.lowerLevelSnapshot.addRef()
		if prevLowerLevelSnapshot != nil {
			prevLowerLevelSnapshot.decRef()
		}

		if m.waitDirtyOutgoingCh != nil {
			close(m.waitDirtyOutgoingCh)
		}
		m.waitDirtyOutgoingCh = make(chan struct{})

		m.stackDirtyBaseCond.Broadcast()
	} else {
		atomic.AddUint64(&m.stats.TotMergerLowerLevelNotifySkip, 1)
	}

	var waitDirtyOutgoingCh chan struct{}

	if m.options.MaxDirtyOps > 0 || m.options.MaxDirtyKeyValBytes > 0 {
		cs := CollectionStats{}

		m.statsSegmentsLOCKED(&cs)

		if cs.CurDirtyOps > m.options.MaxDirtyOps ||
			cs.CurDirtyBytes > m.options.MaxDirtyKeyValBytes {
			waitDirtyOutgoingCh = m.waitDirtyOutgoingCh
		}
	}

	m.m.Unlock()

	if waitDirtyOutgoingCh != nil {
		atomic.AddUint64(&m.stats.TotMergerWaitOutgoingBeg, 1)

		select {
		case <-m.stopCh:
			atomic.AddUint64(&m.stats.TotMergerWaitOutgoingStop, 1)
			return

		case <-waitDirtyOutgoingCh:
			// NO-OP.
		}

		atomic.AddUint64(&m.stats.TotMergerWaitOutgoingEnd, 1)
	} else {
		atomic.AddUint64(&m.stats.TotMergerWaitOutgoingSkip, 1)
	}
}

// ------------------------------------------------------

func (m *collection) cancelMergingsLOCKED(start, end int) {
	if end > len(m.mergings) {
		return
	}

	for i := start; i < end; i++ {
		merging := m.mergings[i]
		close(merging.cancelCh)
		fmt.Printf("  canceled merging: [%d, %d)\n",
			merging.begLevel, merging.endHeight)
	}

	copy(m.mergings[start:], m.mergings[end:])
	for k, n := len(m.mergings)-end+start, len(m.mergings); k < n; k++ {
		m.mergings[k] = nil
	}
	m.mergings = m.mergings[:len(m.mergings)-end+start]
}

func (m *collection) shiftMergingsLOCKED(start, end, shift int) {
	for i := start; i < end; i++ {
		merging := m.mergings[i]
		merging.begLevel += shift
		merging.endHeight += shift
	}
}

// ------------------------------------------------------

func (m *collection) mergerStartChildren(ss, base *segmentStack) (err error) {
	m.m.Lock()

	mergerConcurrency := 4 // TODO.

	stride := len(ss.a) / mergerConcurrency
	if stride < 2 {
		stride = 2
	}

	for i := 0; i < len(ss.a); i += stride {
		err = m.mergerStartChildLOCKED(ss, base, i, i+stride)
		if err != nil {
			return err
		}
	}

	fmt.Printf("    mergings: %d, err: %v\n", len(m.mergings), err)

	m.m.Unlock()

	return err
}

func (m *collection) mergerStartChildLOCKED(ss, base *segmentStack,
	from, to int) error {
	if ss != nil {
		fmt.Printf("    startChildren [%d, %d), ss: %d\n",
			from, to, len(ss.a))
	}

	if ss == nil || len(ss.a) <= 1 || from >= to-1 || to > len(ss.a) {
		return nil
	}

	insertAt := 0
	for i, merging := range m.mergings {
		if rangeOverlaps(from, to, merging.begLevel, merging.endHeight) {
			fmt.Printf("      rangeOverlaps [%d, %d) vs [%d, %d)\n",
				merging.begLevel, merging.endHeight,
				from, to)
			return nil // Return if we overlap with any existing merging.
		}
		if from < merging.begLevel {
			insertAt = i
		}
	}

	outSegment, err := ss.mergeAllocate(from, to)
	if err != nil {
		return err
	}

	merging := &merging{
		ss:         ss,
		base:       base,
		begLevel:   from,
		endHeight:  to,
		outSegment: outSegment,
		cancelCh:   make(chan error, 1),
	}

	if merging.ss != nil {
		merging.ss.addRef()
	}
	if merging.base != nil {
		merging.base.addRef()
	}

	m.mergings = append(m.mergings, nil)
	copy(m.mergings[insertAt+1:], m.mergings[insertAt:])
	m.mergings[insertAt] = merging

	ss.addRef() // Owned by the child goroutine.

	go m.runMergerChild(merging, from, to)

	return nil
}

func rangeOverlaps(fromA, toA, fromB, toB int) bool {
	return (fromA >= fromB && fromA < toB) || (toA > fromB && toA <= toB) ||
		(fromB >= fromA && fromB < toA) || (toB > fromA && toB <= toA)
}

func (m *collection) runMergerChild(merging *merging, from, to int) {
	defer merging.ss.Close()

	err := merging.ss.mergeInto(from, to, merging.outSegment, merging.base,
		true, merging.cancelCh)

	m.m.Lock()
	defer m.m.Unlock()

	if merging.cancelCh != nil {
		select {
		case cancelErr := <-merging.cancelCh:
			if err == nil {
				err = cancelErr
			}
			if err == nil {
				err = ErrCanceled
			}
		default:
			// NO-OP.
		}
	}

	for i, mx := range m.mergings {
		if mx != merging {
			continue
		}

		copy(m.mergings[i:], m.mergings[i+1:])
		m.mergings[len(m.mergings)-1] = nil
		m.mergings = m.mergings[:len(m.mergings)-1]

		if err != nil {
			break
		}

		stackDirtyMidPrev := m.stackDirtyMid
		if stackDirtyMidPrev == nil ||
			len(stackDirtyMidPrev.a) < merging.endHeight {
			err = fmt.Errorf("collection_merger: missing stackDirtyMidPrev")
			break
		}

		ssNew := &segmentStack{
			options:            merging.ss.options,
			a:                  make([]Segment, 0, len(merging.ss.a)),
			refs:               1,
			lowerLevelSnapshot: merging.ss.lowerLevelSnapshot.addRef(),
		}

		ssNew.a = append(ssNew.a, stackDirtyMidPrev.a[0:merging.begLevel]...)
		ssNew.a = append(ssNew.a, merging.outSegment)
		ssNew.a = append(ssNew.a, stackDirtyMidPrev.a[merging.endHeight:]...)

		m.stackDirtyMid = ssNew

		m.shiftMergingsLOCKED(i, len(m.mergings),
			merging.endHeight-merging.begLevel)

		stackDirtyMidPrev.Close()

		fmt.Printf("      child merging done [%d, %d)\n",
			merging.begLevel, merging.endHeight)

		return
	}

	// TODO: log & propagate error?
}
