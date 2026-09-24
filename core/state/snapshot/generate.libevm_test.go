// Copyright 2026 the libevm authors.
//
// The libevm additions to go-ethereum are free software: you can redistribute
// them and/or modify them under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation, either version 3 of the License,
// or (at your option) any later version.
//
// The libevm additions are distributed in the hope that they will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
// General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see
// <http://www.gnu.org/licenses/>.

package snapshot

import (
	"bytes"
	"encoding/binary"
	"sync/atomic"
	"testing"

	"github.com/holiman/uint256"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/ethdb"
)

// pausingDB hands out account snapshot iterators that pause on a chosen step,
// so a test can ask the generator to stop partway through an iteration.
type pausingDB struct {
	ethdb.KeyValueStore
	pauseAt int64
	paused  chan struct{}
	resume  chan struct{}
	steps   atomic.Int64
}

func (db *pausingDB) NewIterator(prefix, start []byte) ethdb.Iterator {
	it := db.KeyValueStore.NewIterator(prefix, start)
	if !bytes.Equal(prefix, rawdb.SnapshotAccountPrefix) {
		return it
	}
	return &pausingIterator{Iterator: it, db: db}
}

type pausingIterator struct {
	ethdb.Iterator
	db *pausingDB
}

func (it *pausingIterator) Next() bool {
	if it.db.steps.Add(1) == it.db.pauseAt {
		close(it.db.paused)
		<-it.db.resume
	}
	return it.Iterator.Next()
}

func TestGenerateStopsWhileSkippingKeys(t *testing.T) {
	helper := newHelper(rawdb.HashScheme)
	helper.addTrieAccount("acc-1", &types.StateAccount{Balance: uint256.NewInt(1), Root: types.EmptyRootHash, CodeHash: types.EmptyCodeHash.Bytes()})
	root := helper.Commit()

	// A hash-scheme database stores trie nodes under their bare 32-byte hash,
	// so the nodes whose hash starts with the account snapshot prefix fall
	// inside the range the generator iterates, and it has to step over them.
	const skipped = 10_000
	for i := uint64(0); i < skipped; i++ {
		key := make([]byte, common.HashLength)
		key[0] = rawdb.SnapshotAccountPrefix[0]
		binary.BigEndian.PutUint64(key[1:], i)
		if err := helper.diskdb.Put(key, []byte{1}); err != nil {
			t.Fatal(err)
		}
	}

	const pauseAt = 100
	db := &pausingDB{
		KeyValueStore: helper.diskdb,
		pauseAt:       pauseAt,
		paused:        make(chan struct{}),
		resume:        make(chan struct{}),
	}
	snap := generateSnapshot(db, helper.triedb, 16, root)

	<-db.paused
	stopped := make(chan struct{})
	go func() {
		snap.stopGeneration()
		close(stopped)
	}()
	<-snap.cancel // closed by stopGeneration before it waits for the generator
	close(db.resume)
	<-stopped

	if got := db.steps.Load(); got > pauseAt {
		t.Errorf("generator took %d iteration steps; want it to stop at step %d, where it was asked to", got, pauseAt)
	}
	if len(snap.genMarker) != 0 {
		t.Errorf("generator marker = %#x after stopping mid-iteration; want no progress recorded", snap.genMarker)
	}
}
