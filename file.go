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
	"os"
	"sync"
)

// The File interface is implemented by os.File.  App specific
// implementations may add concurrency, caching, stats, fuzzing, etc.
type File interface {
	io.ReaderAt
	io.WriterAt
	io.Closer
	Stat() (os.FileInfo, error)
	Truncate(size int64) error
}

// The OpenFile func signature is similar to os.OpenFile().
type OpenFile func(name string, flag int, perm os.FileMode) (File, error)

// FileRef provides a ref-counting wrapper around a File.
type FileRef struct {
	file File
	m    sync.Mutex // Protects the fields that follow.
	refs int

	beforeCloseCallbacks []func() // Optional callbacks invoked before final close.
	afterCloseCallbacks  []func() // Optional callbacks invoked after final close.
}

// --------------------------------------------------------

// OnBeforeClose registers event callback func's that are invoked before the
// file is closed.
func (r *FileRef) OnBeforeClose(cb func()) {
	r.m.Lock()
	r.beforeCloseCallbacks = append(r.beforeCloseCallbacks, cb)
	r.m.Unlock()
}

// OnAfterClose registers event callback func's that are invoked after the
// file is closed.
func (r *FileRef) OnAfterClose(cb func()) {
	r.m.Lock()
	r.afterCloseCallbacks = append(r.afterCloseCallbacks, cb)
	r.m.Unlock()
}

// AddRef increases the ref-count on the file ref.
func (r *FileRef) AddRef() File {
	if r == nil {
		return nil
	}

	r.m.Lock()
	r.refs++
	file := r.file
	r.m.Unlock()

	return file
}

// DecRef decreases the ref-count on the file ref, and closing the
// underlying file when the ref-count reaches zero.
func (r *FileRef) DecRef() (err error) {
	if r == nil {
		return nil
	}

	r.m.Lock()

	r.refs--
	if r.refs <= 0 {
		for _, cb := range r.beforeCloseCallbacks {
			cb()
		}
		r.beforeCloseCallbacks = nil

		err = r.file.Close()

		for _, cb := range r.afterCloseCallbacks {
			cb()
		}
		r.afterCloseCallbacks = nil

		r.file = nil
	}

	r.m.Unlock()

	return err
}

func (r *FileRef) Close() error {
	return r.DecRef()
}

// --------------------------------------------------------

// OsFile interface let's one convert from a File to an os.File.
type OsFile interface {
	OsFile() *os.File
}

// ToOsFile provides the underlying os.File for a File, if available.
func ToOsFile(f File) *os.File {
	if osFile, ok := f.(*os.File); ok {
		return osFile
	}
	if osFile2, ok := f.(OsFile); ok {
		return osFile2.OsFile()
	}
	return nil
}

// --------------------------------------------------------

func NewBufferedSectionWriter(w io.WriterAt, begPos, maxBytes int64,
	bufSize int) *bufferedSectionWriter {
	return &bufferedSectionWriter{
		w:   w,
		beg: begPos,
		cur: begPos,
		max: maxBytes,
		buf: make([]byte, bufSize),
	}
}

type bufferedSectionWriter struct {
	err error
	w   io.WriterAt
	beg int64 // Start position where we started writing in file.
	cur int64 // Current write-at position in file.
	max int64 // When > 0, max number of bytes we can write.
	buf []byte
	n   int
}

func (b *bufferedSectionWriter) Offset() int64 { return b.cur + int64(b.n) }

func (b *bufferedSectionWriter) Available() int { return len(b.buf) - b.n }

func (b *bufferedSectionWriter) Write(p []byte) (nn int, err error) {
	if b.max >= 0 && b.cur-b.beg+int64(b.n+len(p)) > b.max {
		return 0, io.ErrShortBuffer // Would go over b.max.
	}
	for len(p) > b.Available() && b.err == nil {
		var n int
		if b.n <= 0 {
			// Avoid copy by direct write of large p when empty buffer.
			n, b.err = b.w.WriteAt(p, b.cur)
			b.cur += int64(n)
		} else {
			n = copy(b.buf[b.n:], p)
			b.n += n
			b.err = b.Flush()
		}
		nn += n
		p = p[n:]
	}
	if b.err != nil {
		return nn, b.err
	}
	n := copy(b.buf[b.n:], p)
	b.n += n
	nn += n
	return nn, nil
}

func (b *bufferedSectionWriter) Flush() error {
	if b.err != nil {
		return b.err
	}
	if b.n <= 0 {
		return nil
	}
	if b.max >= 0 && b.cur-b.beg+int64(b.n) > b.max {
		return io.ErrShortBuffer // Would go over b.max.
	}
	n, err := b.w.WriteAt(b.buf[0:b.n], b.cur)
	if err != nil {
		b.err = err
		return err
	}
	if n > 0 {
		if n < b.n {
			copy(b.buf[0:b.n-n], b.buf[n:b.n])
			err = io.ErrShortWrite
		}
		b.n -= n
		b.cur += int64(n)
	}
	return err
}
