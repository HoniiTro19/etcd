// Copyright 2015 CoreOS, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import (
	"io"
	"log"
	"os"
	"path"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/coreos/etcd/wal/walpb"
)

// Repair tries to repair the unexpectedEOF error in the
// last wal file by truncating.
func Repair(dirpath string) bool {
	f, err := openLast(dirpath)
	if err != nil {
		return false
	}
	defer f.Close()

	n := 0
	rec := &walpb.Record{}

	decoder := newDecoder(f)
	defer decoder.close()
	for {
		err := decoder.decode(rec)
		switch err {
		case nil:
			n += 8 + rec.Size()
			continue
		case io.EOF:
			return true
		case io.ErrUnexpectedEOF:
			log.Printf("wal: repairing %v", f.Name())
			bf, bferr := os.Create(f.Name() + ".broken")
			if bferr != nil {
				log.Printf("wal: could not repair %v, failed to create backup file", f.Name())
				return false
			}
			defer bf.Close()

			if _, err = f.Seek(0, os.SEEK_SET); err != nil {
				log.Printf("wal: could not repair %v, failed to read file", f.Name())
				return false
			}

			if _, err = io.Copy(bf, f); err != nil {
				log.Printf("wal: could not repair %v, failed to copy file", f.Name())
				return false
			}

			if err = f.Truncate(int64(n)); err != nil {
				log.Printf("wal: could not repair %v, failed to truncate file", f.Name())
				return false
			}
			if err = f.Sync(); err != nil {
				log.Printf("wal: could not repair %v, failed to sync file", f.Name())
				return false
			}
			return true
		}
	}
}

// openLast opens the last wal file for read and write.
func openLast(dirpath string) (*os.File, error) {
	names, err := fileutil.ReadDir(dirpath)
	if err != nil {
		return nil, err
	}
	names = checkWalNames(names)
	if len(names) == 0 {
		return nil, ErrFileNotFound
	}
	last := path.Join(dirpath, names[len(names)-1])
	return os.OpenFile(last, os.O_RDWR, 0)
}
