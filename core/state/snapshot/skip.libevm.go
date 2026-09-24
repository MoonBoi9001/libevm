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
	"slices"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	"github.com/ava-labs/libevm/ethdb"
)

const (
	// minSkipKeys is the fewest keys a stretch must hold to be remembered, as
	// reading a shorter one again costs less than tracking it.
	minSkipKeys = 64
	// maxSkipRanges bounds the stretches remembered for each kind of entry.
	maxSkipRanges = 256
)

// keyRange is an inclusive stretch of raw database keys, holding keys of them;
// it is empty when either end is missing or from sorts after to.
type keyRange struct {
	from, to []byte
	keys     int
}

func (r keyRange) empty() bool {
	return r.from == nil || r.to == nil || bytes.Compare(r.from, r.to) > 0
}

// after returns the part of r that sorts after key.
func (r keyRange) after(key []byte) keyRange {
	if r.empty() || bytes.Compare(r.from, key) > 0 {
		return r
	}
	return keyRange{from: append(common.CopyBytes(key), 0), to: r.to, keys: r.keys}
}

// keyRanges are disjoint stretches sorted by key. Methods return new slices and
// never modify the receiver, so a copy handed to an iterator stays valid.
type keyRanges []keyRange

// with returns rs plus r, merged with any stretch it overlaps, keeping only the
// maxSkipRanges stretches holding the most keys.
func (rs keyRanges) with(r keyRange) keyRanges {
	if r.empty() || r.keys < minSkipKeys {
		return rs
	}
	r = keyRange{from: common.CopyBytes(r.from), to: common.CopyBytes(r.to), keys: r.keys}
	out := make(keyRanges, 0, len(rs)+1)
	for _, o := range rs {
		if bytes.Compare(o.to, r.from) < 0 || bytes.Compare(o.from, r.to) > 0 {
			out = append(out, o)
			continue
		}
		if bytes.Compare(o.from, r.from) < 0 {
			r.from = o.from
		}
		if bytes.Compare(o.to, r.to) > 0 {
			r.to = o.to
		}
		r.keys = max(r.keys, o.keys)
	}
	out = append(out, r)
	slices.SortFunc(out, func(a, b keyRange) int { return bytes.Compare(a.from, b.from) })
	if len(out) > maxSkipRanges {
		fewest := 0
		for i := range out {
			if out[i].keys < out[fewest].keys {
				fewest = i
			}
		}
		out = slices.Delete(out, fewest, fewest+1)
	}
	return out
}

// after returns the parts of rs that sort after key.
func (rs keyRanges) after(key []byte) keyRanges {
	out := make(keyRanges, 0, len(rs))
	for _, r := range rs {
		if r = r.after(key); !r.empty() {
			out = append(out, r)
		}
	}
	return out
}

// generatorSkips holds, for each kind of snapshot entry, stretches of raw keys
// known to hold none of them. Nothing writes entries ahead of the generator, so
// a resumed generator can step over them instead of reading them again.
type generatorSkips struct {
	account, storage keyRanges
}

// skippingIterator iterates the raw keys under prefix, jumping over the known
// stretches, and records in found each stretch it reads that holds no key of
// keyLen.
type skippingIterator struct {
	db     ethdb.KeyValueStore
	prefix []byte
	keyLen int
	known  keyRanges
	next   int // Index of the first known stretch not yet behind the iterator
	found  *keyRanges
	it     ethdb.Iterator
	run    keyRange // Read since the last key of keyLen
}

func newSkippingIterator(db ethdb.KeyValueStore, prefix, start []byte, keyLen int, known keyRanges, found *keyRanges) *skippingIterator {
	return &skippingIterator{
		db:     db,
		prefix: prefix,
		keyLen: keyLen,
		known:  known,
		found:  found,
		it:     db.NewIterator(prefix, start),
		run:    keyRange{from: append(common.CopyBytes(prefix), start...)},
	}
}

func (it *skippingIterator) Next() bool {
	for it.it.Next() {
		key := it.it.Key()
		for it.next < len(it.known) && bytes.Compare(it.known[it.next].to, key) < 0 {
			it.next++
		}
		if it.next < len(it.known) && bytes.Compare(it.known[it.next].from, key) <= 0 {
			// The run so far and the whole known stretch hold no key of keyLen,
			// so the run carries on to the end of the stretch.
			skip := it.known[it.next]
			it.next++
			it.run.to = common.CopyBytes(skip.to)
			it.run.keys += skip.keys
			it.it.Release()
			it.it = it.db.NewIterator(it.prefix, skip.to[len(it.prefix):])
			continue
		}
		if len(key) == it.keyLen {
			it.keepRun()
			it.run = keyRange{from: append(common.CopyBytes(key), 0)}
			return true
		}
		it.run.to = append(it.run.to[:0], key...)
		it.run.keys++
		return true
	}
	return false
}

// keepRun records the stretch read since the last key of keyLen.
func (it *skippingIterator) keepRun() {
	if it.found != nil {
		*it.found = it.found.with(it.run)
	}
}

func (it *skippingIterator) Error() error  { return it.it.Error() }
func (it *skippingIterator) Key() []byte   { return it.it.Key() }
func (it *skippingIterator) Value() []byte { return it.it.Value() }
func (it *skippingIterator) Release()      { it.it.Release() }

// snapshotIterator opens a raw iterator over one kind of snapshot entry that
// steps over the stretches known to hold none.
func (ctx *generatorContext) snapshotIterator(kind string, prefix, start []byte, keyLen int) ethdb.Iterator {
	skips := &ctx.skips.account
	if kind == snapStorage {
		skips = &ctx.skips.storage
	}
	it := newSkippingIterator(ctx.db, prefix, start, keyLen, *skips, skips)
	if kind == snapAccount {
		ctx.accountRead = it
	} else {
		ctx.storageRead = it
	}
	return it
}

// keepRead records the stretch the iterator of kind has read since its last
// entry.
func (ctx *generatorContext) keepRead(kind string) {
	it := ctx.accountRead
	if kind == snapStorage {
		it = ctx.storageRead
	}
	if it != nil {
		it.keepRun()
	}
}

// noteProgress trims every known stretch to start after current, the
// generator's position, since it writes snapshot entries up to there.
func (ctx *generatorContext) noteProgress(current []byte) {
	account := append(common.CopyBytes(rawdb.SnapshotAccountPrefix), current[:min(len(current), common.HashLength)]...)
	storage := append(common.CopyBytes(rawdb.SnapshotStoragePrefix), current...)

	ctx.skips.account = ctx.skips.account.after(account)
	ctx.skips.storage = ctx.skips.storage.after(storage)
	if ctx.accountRead != nil {
		ctx.accountRead.run = ctx.accountRead.run.after(account)
	}
	if ctx.storageRead != nil {
		ctx.storageRead.run = ctx.storageRead.run.after(storage)
	}
}

// keepSkips hands what this run learnt about empty stretches to the disk layer,
// which passes it on to the next run when generation restarts on a new layer.
func (dl *diskLayer) keepSkips(ctx *generatorContext) {
	ctx.keepRead(snapAccount)
	ctx.keepRead(snapStorage)

	dl.lock.Lock()
	dl.genSkips = ctx.skips
	dl.lock.Unlock()
}
