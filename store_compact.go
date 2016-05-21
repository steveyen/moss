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
	"os"
	"path"
	"reflect"
	"unsafe"
)

type CompactionConcern int // See StorePersistOptions.CompactionConcern.

// CompactionDisabled means no compaction.
var CompactionDisabled = CompactionConcern(0)

// CompactionAllowed means compaction decision is automated and based
// on the configed policy and parameters.
var CompactionAllowed = CompactionConcern(1)

// CompactionForce means compaction should be performed immediately.
var CompactionForce = CompactionConcern(2)

// COMPACTION_BUFFER_SIZE is the buffer size used for compaction
// buffers, where writes are buffered before flushing to disk.
var COMPACTION_BUFFER_SIZE = STORE_PAGE_SIZE * 64

// --------------------------------------------------------

func (s *Store) compactMaybe(higher Snapshot, persistOptions StorePersistOptions) (
	bool, error) {
	compactionConcern := persistOptions.CompactionConcern
	if compactionConcern <= 0 {
		return false, nil
	}

	footer, err := s.snapshot()
	if err != nil {
		return false, err
	}
	defer footer.Close()

	if compactionConcern == CompactionAllowed {
		if footer.ss.calcTargetTopLevel() <= 0 {
			compactionConcern = CompactionForce
		}
	}

	if compactionConcern != CompactionForce {
		return false, nil
	}

	err = s.compact(footer, higher)
	if err != nil {
		return false, err
	}

	if footer.fref != nil {
		finfo, err := footer.fref.file.Stat()
		if err == nil && len(finfo.Name()) > 0 {
			footer.fref.OnAfterClose(func() {
				os.Remove(path.Join(s.dir, finfo.Name()))
			})
		}
	}

	return true, nil
}

func (s *Store) compact(footer *Footer, higher Snapshot) error {
	ss := &footer.ss
	if higher != nil {
		ssHigher, ok := higher.(*segmentStack)
		if !ok {
			return fmt.Errorf("store: can only compact higher that's a segmentStack")
		}
		ssHigher.ensureSorted(0, len(ssHigher.a)-1)

		ss = &segmentStack{options: ss.options}
		ss.a = append(ss.a, footer.ss.a...)
		ss.a = append(ss.a, ssHigher.a...)
	}

	s.m.Lock()
	fref, file, err := s.startFileLOCKED()
	s.m.Unlock()
	if err != nil {
		return err
	}

	stats := ss.Stats()

	kvsBegPos := pageAlign(int64(STORE_PAGE_SIZE))
	bufBegPos := pageAlign(kvsBegPos + 1 + (int64(8+8) * int64(stats.CurOps)))

	compactWriter := &compactWriter{
		kvsWriter: NewBufferedSectionWriter(file, kvsBegPos, -1, COMPACTION_BUFFER_SIZE),
		bufWriter: NewBufferedSectionWriter(file, bufBegPos, -1, COMPACTION_BUFFER_SIZE),
	}

	err = ss.mergeInto(0, compactWriter, nil)
	if err != nil {
		fref.DecRef()
		return err
	}

	if err = compactWriter.kvsWriter.Flush(); err != nil {
		fref.DecRef()
		return err
	}
	if err = compactWriter.bufWriter.Flush(); err != nil {
		fref.DecRef()
		return err
	}

	compactFooter := &Footer{
		SegmentLocs: []SegmentLoc{
			SegmentLoc{
				KvsOffset:  uint64(kvsBegPos),
				KvsBytes:   uint64(compactWriter.kvsWriter.Offset() - kvsBegPos),
				BufOffset:  uint64(bufBegPos),
				BufBytes:   uint64(compactWriter.bufWriter.Offset() - bufBegPos),
				TotOpsSet:  compactWriter.totOperationSet,
				TotOpsDel:  compactWriter.totOperationDel,
				TotKeyByte: compactWriter.totKeyByte,
				TotValByte: compactWriter.totValByte,
			},
		},
		fref: fref,
	}

	if err = s.persistFooter(file, compactFooter); err != nil {
		fref.DecRef()
		return err
	}

	s.m.Lock()
	footerPrev := s.footer
	s.footer = compactFooter // Owns the ref-count.
	s.m.Unlock()

	if footerPrev != nil {
		footerPrev.fref.DecRef()
	}

	return nil
}

type compactWriter struct {
	file      File
	kvsWriter *bufferedSectionWriter
	bufWriter *bufferedSectionWriter

	totOperationSet   uint64
	totOperationDel   uint64
	totOperationMerge uint64
	totKeyByte        uint64
	totValByte        uint64
}

func (cw *compactWriter) Mutate(operation uint64, key, val []byte) error {
	keyStart := cw.bufWriter.Offset()
	_, err := cw.bufWriter.Write(key)
	if err != nil {
		return err
	}

	cw.bufWriter.Offset()
	_, err = cw.bufWriter.Write(val)
	if err != nil {
		return err
	}

	keyLen := len(key)
	valLen := len(val)

	opKlVl := encodeOpKeyLenValLen(operation, keyLen, valLen)

	if keyLen <= 0 && valLen <= 0 {
		keyStart = 0
	}

	pair := []uint64{opKlVl, uint64(keyStart)}
	pairSliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&pair))

	var kvsBuf []byte
	kvsBufSliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&kvsBuf))
	kvsBufSliceHeader.Data = pairSliceHeader.Data
	kvsBufSliceHeader.Len = pairSliceHeader.Len * 8
	kvsBufSliceHeader.Cap = pairSliceHeader.Cap * 8

	_, err = cw.kvsWriter.Write(kvsBuf)
	if err != nil {
		return err
	}

	switch operation {
	case OperationSet:
		cw.totOperationSet++
	case OperationDel:
		cw.totOperationDel++
	case OperationMerge:
		cw.totOperationMerge++
	default:
	}

	cw.totKeyByte += uint64(keyLen)
	cw.totValByte += uint64(valLen)

	return nil
}
