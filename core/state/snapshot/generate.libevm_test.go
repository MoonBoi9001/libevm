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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/ethdb"
)

// countingDB counts the steps taken by its snapshot iterators and, when every
// is set, pauses on each multiple of it so a test can stop generation there.
type countingDB struct {
	ethdb.KeyValueStore
	every  atomic.Int64
	paused chan struct{}
	resume chan struct{}
	steps  atomic.Int64
}

func newCountingDB(db ethdb.KeyValueStore, every int64) *countingDB {
	c := &countingDB{KeyValueStore: db, paused: make(chan struct{}), resume: make(chan struct{})}
	c.every.Store(every)
	return c
}

func (db *countingDB) NewIterator(prefix, start []byte) ethdb.Iterator {
	it := db.KeyValueStore.NewIterator(prefix, start)
	if !bytes.Equal(prefix, rawdb.SnapshotAccountPrefix) && !bytes.Equal(prefix, rawdb.SnapshotStoragePrefix) {
		return it
	}
	return &countingIterator{Iterator: it, db: db}
}

type countingIterator struct {
	ethdb.Iterator
	db *countingDB
}

func (it *countingIterator) Next() bool {
	if n, every := it.db.steps.Add(1), it.db.every.Load(); every > 0 && n%every == 0 {
		it.db.paused <- struct{}{}
		<-it.db.resume
	}
	return it.Iterator.Next()
}

// waitForPause fails the test instead of hanging if the pause never comes.
func (db *countingDB) waitForPause(t *testing.T) {
	t.Helper()
	select {
	case <-db.paused:
	case <-time.After(time.Minute):
		t.Fatalf("iterators never reached step %d", db.every.Load())
	}
}

// putSkippedKeys stores n keys of a hash-scheme trie node's length that start
// with prefix, spread over the range the way node hashes are. The snapshot
// iterators cover that range, so they have to step over every one of them.
func putSkippedKeys(t *testing.T, db ethdb.KeyValueWriter, prefix []byte, n uint64) {
	t.Helper()
	for i := uint64(0); i < n; i++ {
		var seed [8]byte
		binary.BigEndian.PutUint64(seed[:], i)
		key := append(common.CopyBytes(prefix), crypto.Keccak256(prefix, seed[:])[:common.HashLength-len(prefix)]...)
		if err := db.Put(key, []byte{1}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestGenerateStopsWhileSkippingKeys(t *testing.T) {
	helper := newHelper(rawdb.HashScheme)
	helper.addTrieAccount("acc-1", &types.StateAccount{Balance: uint256.NewInt(1), Root: types.EmptyRootHash, CodeHash: types.EmptyCodeHash.Bytes()})
	root := helper.Commit()
	putSkippedKeys(t, helper.diskdb, rawdb.SnapshotAccountPrefix, 10_000)

	const pauseAt = 100
	db := newCountingDB(helper.diskdb, pauseAt)
	snap := generateSnapshot(db, helper.triedb, 16, root)

	db.waitForPause(t)
	stopped := make(chan struct{})
	go func() {
		snap.stopGeneration()
		close(stopped)
	}()
	<-snap.cancel // closed by stopGeneration before it waits for the generator
	db.resume <- struct{}{}
	<-stopped

	if got := db.steps.Load(); got > pauseAt {
		t.Errorf("generator took %d iteration steps; want it to stop at step %d, where it was asked to", got, pauseAt)
	}
	if len(snap.genMarker) != 0 {
		t.Errorf("generator marker = %#x after stopping mid-iteration; want no progress recorded", snap.genMarker)
	}
}

// restartUntilDone restarts generation on a fresh disk layer, as a node does on
// each block, every time db pauses, until the snapshot is complete.
func restartUntilDone(t *testing.T, db *countingDB, layer *diskLayer, root common.Hash, maxRestarts int) *diskLayer {
	t.Helper()
	for restarts := 0; ; restarts++ {
		select {
		case <-layer.genPending:
			db.every.Store(0) // the checks that follow iterate the snapshot too
			t.Logf("finished after %d restarts and %d iteration steps", restarts, db.steps.Load())
			return layer
		case <-db.paused:
		case <-time.After(time.Minute):
			t.Fatalf("generation neither paused nor finished after %d restarts", restarts)
		}
		if restarts == maxRestarts {
			close(db.resume)
			t.Fatalf("generation still unfinished after %d restarts and %d iteration steps", restarts, db.steps.Load())
		}
		next := make(chan *diskLayer)
		go func(base *diskLayer) {
			next <- diffToDisk(newDiffLayer(base, root, nil, nil, nil))
		}(layer)
		<-layer.cancel
		db.resume <- struct{}{}
		layer = <-next
	}
}

// TestGenerateKeepsProgressWhenStoppedMidRange stops generation while it reads
// a range of existing snapshot entries, after it has finished earlier ranges.
func TestGenerateKeepsProgressWhenStoppedMidRange(t *testing.T) {
	helper := newHelper(rawdb.HashScheme)
	for i := uint64(0); i < 300; i++ {
		acc := &types.StateAccount{Balance: uint256.NewInt(i), Root: types.EmptyRootHash, CodeHash: types.EmptyCodeHash.Bytes()}
		helper.addAccount(fmt.Sprintf("acc-%d", i), acc)
	}
	root := helper.Commit()
	putSkippedKeys(t, helper.diskdb, rawdb.SnapshotAccountPrefix, 3_000)

	db := newCountingDB(helper.diskdb, 2_000)
	layer := restartUntilDone(t, db, generateSnapshot(db, helper.triedb, 16, root), root, 20)
	checkSnapRoot(t, layer, root)
}
