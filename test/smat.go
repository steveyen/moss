//  Copyright (c) 2016 Marty Schoch

//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package test

import (
	"fmt"

	"github.com/couchbase/moss"

	"github.com/mschoch/smat"
)

// TODO: Test pre-allocated batches and AllocSet/Del/Merge().
// TODO: Test snapshots / iterators against a mirror data-structure for correctness.

// Fuzz using state machine driven by byte stream.
func Fuzz(data []byte) int {
	return smat.Fuzz(&context{}, smat.ActionID('S'), smat.ActionID('T'),
		actionMap, data)
}

type context struct {
	coll moss.Collection // Initialized in setupFunc().

	curBatch    int
	curSnapshot int
	curIterator int
	curKey      int

	batches      []moss.Batch
	batchMirrors []map[string]bool // Mirrors the entries in batches.
	snapshots    []moss.Snapshot
	iterators    []moss.Iterator
	keys         []string
}

// ------------------------------------------------------------------

var actionMap = smat.ActionMap{
	smat.ActionID('.'): delta(func(c *context) { c.curBatch++ }),
	smat.ActionID(','): delta(func(c *context) { c.curBatch-- }),
	smat.ActionID('{'): delta(func(c *context) { c.curSnapshot++ }),
	smat.ActionID('}'): delta(func(c *context) { c.curSnapshot-- }),
	smat.ActionID('['): delta(func(c *context) { c.curIterator++ }),
	smat.ActionID(']'): delta(func(c *context) { c.curIterator-- }),
	smat.ActionID(':'): delta(func(c *context) { c.curKey++ }),
	smat.ActionID(';'): delta(func(c *context) { c.curKey-- }),
	smat.ActionID('s'): opSetFunc,
	smat.ActionID('d'): opDelFunc,
	smat.ActionID('m'): opMergeFunc,
	smat.ActionID('g'): opGetFunc,
	smat.ActionID('B'): batchCreateFunc,
	smat.ActionID('b'): batchExecuteFunc,
	smat.ActionID('H'): snapshotCreateFunc,
	smat.ActionID('h'): snapshotCloseFunc,
	smat.ActionID('I'): iteratorCreateFunc,
	smat.ActionID('i'): iteratorCloseFunc,
	smat.ActionID('>'): iteratorNextFunc,
	smat.ActionID('K'): keyRegisterFunc,
	smat.ActionID('k'): keyUnregisterFunc,
}

var runningPercentActions []smat.PercentAction

func init() {
	pct := 100 / len(actionMap)
	for actionId := range actionMap {
		runningPercentActions = append(runningPercentActions,
			smat.PercentAction{pct, actionId})
	}

	actionMap[smat.ActionID('S')] = setupFunc
	actionMap[smat.ActionID('T')] = teardownFunc
}

// We only have one state: running.
func running(next byte) smat.ActionID {
	// Code coverage climbs maybe slightly faster if we do a simple modulus instead.
	//
	// return smat.PercentExecute(next, runningPercentActions...)
	//
	return runningPercentActions[int(next)%len(runningPercentActions)].Action
}

func delta(cb func(c *context)) func(ctx smat.Context) (next smat.State, err error) {
	return func(ctx smat.Context) (next smat.State, err error) {
		c := ctx.(*context)
		cb(c)
		if c.curBatch < 0 {
			c.curBatch = 1000
		}
		if c.curSnapshot < 0 {
			c.curSnapshot = 1000
		}
		if c.curIterator < 0 {
			c.curIterator = 1000
		}
		if c.curKey < 0 {
			c.curKey = 1000
		}
		return running, nil
	}
}

// ------------------------------------------------------------------

func setupFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)

	coll, err := moss.NewCollection(moss.CollectionOptions{
		MergeOperator: &moss.MergeOperatorStringAppend{Sep: ":"},
	})
	if err != nil {
		return nil, err
	}
	c.coll = coll
	c.coll.Start()

	return running, nil
}

func teardownFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	c.coll.Close()

	return nil, nil
}

// ------------------------------------------------------------------

func opSetFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	b, mirror, err := c.getCurBatch()
	if err != nil {
		return nil, err
	}
	k := c.getCurKey()
	if !mirror[k] {
		mirror[k] = true
		err := b.Set([]byte(k), []byte(k))
		if err != nil {
			return nil, err
		}
	}
	return running, nil
}

func opDelFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	b, mirror, err := c.getCurBatch()
	if err != nil {
		return nil, err
	}
	k := c.getCurKey()
	if !mirror[k] {
		mirror[k] = true
		err := b.Del([]byte(k))
		if err != nil {
			return nil, err
		}
	}
	return running, nil
}

func opMergeFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	b, mirror, err := c.getCurBatch()
	if err != nil {
		return nil, err
	}
	k := c.getCurKey()
	if !mirror[k] {
		mirror[k] = true
		err := b.Merge([]byte(k), []byte(k))
		if err != nil {
			return nil, err
		}
	}
	return running, nil
}

func opGetFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	ss, err := c.getCurSnapshot()
	if err != nil {
		return nil, err
	}
	k := c.getCurKey()
	_, err = ss.Get([]byte(k), moss.ReadOptions{})
	if err != nil {
		return nil, err
	}
	return running, nil
}

func batchCreateFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	b, err := c.coll.NewBatch(0, 0)
	if err != nil {
		return nil, err
	}
	c.batches = append(c.batches, b)
	c.batchMirrors = append(c.batchMirrors, map[string]bool{})
	return running, nil
}

func batchExecuteFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	if len(c.batches) <= 0 {
		return running, nil
	}
	b := c.batches[c.curBatch%len(c.batches)]
	err = c.coll.ExecuteBatch(b, moss.WriteOptions{})
	if err != nil {
		return nil, err
	}
	err = b.Close()
	if err != nil {
		return nil, err
	}
	i := c.curBatch % len(c.batches)
	c.batches = append(c.batches[:i], c.batches[i+1:]...)
	i = c.curBatch % len(c.batchMirrors)
	c.batchMirrors = append(c.batchMirrors[:i], c.batchMirrors[i+1:]...)
	return running, nil
}

func snapshotCreateFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	ss, err := c.coll.Snapshot()
	if err != nil {
		return nil, err
	}
	c.snapshots = append(c.snapshots, ss)
	return running, nil
}

func snapshotCloseFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	if len(c.snapshots) <= 0 {
		return running, nil
	}
	i := c.curSnapshot % len(c.snapshots)
	ss := c.snapshots[i]
	err = ss.Close()
	if err != nil {
		return nil, err
	}
	c.snapshots = append(c.snapshots[:i], c.snapshots[i+1:]...)
	return running, nil
}

func iteratorCreateFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	ss, err := c.getCurSnapshot()
	if err != nil {
		return nil, err
	}
	iter, err := ss.StartIterator(nil, nil, moss.IteratorOptions{})
	if err != nil {
		return nil, err
	}
	c.iterators = append(c.iterators, iter)
	return running, nil
}

func iteratorCloseFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	if len(c.iterators) <= 0 {
		return running, nil
	}
	i := c.curIterator % len(c.iterators)
	iter := c.iterators[i]
	err = iter.Close()
	if err != nil {
		return nil, err
	}
	c.iterators = append(c.iterators[:i], c.iterators[i+1:]...)
	return running, nil
}

func iteratorNextFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	if len(c.iterators) <= 0 {
		return running, nil
	}
	i := c.curIterator % len(c.iterators)
	iter := c.iterators[i]
	err = iter.Next()
	if err != nil && err != moss.ErrIteratorDone {
		return nil, err
	}
	c.iterators = append(c.iterators[:i], c.iterators[i+1:]...)
	return running, nil
}

func keyRegisterFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	c.keys = append(c.keys, fmt.Sprintf("%d", c.curKey+len(c.keys)))
	return running, nil
}

func keyUnregisterFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	if len(c.keys) <= 0 {
		return running, nil
	}
	i := c.curKey % len(c.keys)
	c.keys = append(c.keys[:i], c.keys[i+1:]...)
	return running, nil
}

// ------------------------------------------------------

func (c *context) getCurKey() string {
	if len(c.keys) <= 0 {
		return "x"
	}
	return c.keys[c.curKey%len(c.keys)]
}

func (c *context) getCurBatch() (moss.Batch, map[string]bool, error) {
	if len(c.batches) <= 0 {
		_, err := batchCreateFunc(c)
		if err != nil {
			return nil, nil, err
		}
	}
	return c.batches[c.curBatch%len(c.batches)], c.batchMirrors[c.curBatch%len(c.batchMirrors)], nil
}

func (c *context) getCurSnapshot() (moss.Snapshot, error) {
	if len(c.snapshots) <= 0 {
		_, err := snapshotCreateFunc(c)
		if err != nil {
			return nil, err
		}
	}
	return c.snapshots[c.curSnapshot%len(c.snapshots)], nil
}
