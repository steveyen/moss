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
	"sync/atomic"
)

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

		var waitDirtyIncomingCh chan struct{}

		m.m.Lock()

		if m.stackDirtyTop == nil || len(m.stackDirtyTop.a) <= 0 {
			m.waitDirtyIncomingCh = make(chan struct{})
			waitDirtyIncomingCh = m.waitDirtyIncomingCh
		}

		m.m.Unlock()

		mergeAll := false

		if waitDirtyIncomingCh != nil {
			atomic.AddUint64(&m.stats.TotMergerWaitIncomingBeg, 1)

			select {
			case <-m.stopCh:
				atomic.AddUint64(&m.stats.TotMergerWaitIncomingStop, 1)
				return

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

		pings, mergeAll =
			receivePings(m.pingMergerCh, pings, "mergeAll", mergeAll)

		// ---------------------------------------------
		// Atomically ingest stackDirtyTop into stackDirtyMid.

		var stackDirtyTopPrev *segmentStack
		var stackDirtyMidPrev *segmentStack
		var stackDirtyBase *segmentStack

		stackDirtyMid, _, _, prevLenDirtyMid, prevLenDirtyTop :=
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

		if len(stackDirtyMid.a) > 1 {
			newTopLevel := 0

			if !mergeAll {
				// If we have not been asked to merge all segments,
				// then heuristically calc a newTopLevel.
				newTopLevel = stackDirtyMid.calcTargetTopLevel()
			}

			if newTopLevel <= 0 {
				atomic.AddUint64(&m.stats.TotMergerAll, 1)
			}

			atomic.AddUint64(&m.stats.TotMergerInternalBeg, 1)

			mergedStackDirtyMid, err :=
				stackDirtyMid.merge(newTopLevel, stackDirtyBase)
			if err != nil {
				atomic.AddUint64(&m.stats.TotMergerInternalErr, 1)

				m.Logf("collection: runMerger stackDirtyMid.merge,"+
					" newTopLevel: %d, err: %v", newTopLevel, err)

				m.OnError(err)

				continue OUTER
			}

			atomic.AddUint64(&m.stats.TotMergerInternalEnd, 1)

			stackDirtyMid.Close()

			mergedStackDirtyMid.addRef()
			stackDirtyMid = mergedStackDirtyMid

			m.m.Lock()
			stackDirtyMidPrev = m.stackDirtyMid
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

			m.Logf("collection: runMerger,"+
				" dirtyTop prev height: %2d,"+
				" dirtyMid height: %2d (%2d),"+
				" dirtyMid top # entries: %d",
				prevLenDirtyTop, lenDirtyMid, lenDirtyMid-prevLenDirtyMid,
				topDirtyMid.Len())
		}

		stackDirtyMid.Close()

		// ---------------------------------------------
		// Notify persister.

		if m.options.LowerLevelUpdate != nil {
			m.m.Lock()

			if m.stackDirtyBase == nil &&
				m.stackDirtyMid != nil && len(m.stackDirtyMid.a) > 0 {
				atomic.AddUint64(&m.stats.TotMergerLowerLevelNotify, 1)

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

			if m.options.MaxDirtyOps > 0 ||
				m.options.MaxDirtyKeyValBytes > 0 {
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

		// ---------------------------------------------

		atomic.AddUint64(&m.stats.TotMergerLoopRepeat, 1)

		if m.options.OnEvent != nil {
			m.options.OnEvent(Event{
				Kind:       EventKindMergerProgress,
				Collection: m,
			})
		}
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
