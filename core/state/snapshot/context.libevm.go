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

	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/libevm/options"
	"github.com/ava-labs/libevm/log"
)

// generatorPausing is embedded in [generatorContext] to add libevm-specific
// fields that allow [diskLayer.stopGeneration] to unblock more frequently.
type generatorPausing struct {
	cancel   <-chan struct{} // Closed when the generation is asked to stop
	progress []byte          // Last position the generation finished, as passed to checkAndFlush
}

type generatorContextOption = options.Option[generatorContext]

// withCancelFromDiskLayer configures [newGeneratorContext] to use the same
// cancellation channel as the [diskLayer].
func withCancelFromDiskLayer(dl *diskLayer) generatorContextOption {
	return options.Func[generatorContext](func(ctx *generatorContext) {
		ctx.generatorPausing.cancel = dl.cancel
	})
}

// abortableIterator stops with [errAborted] once cancel is closed. On a hash-scheme
// database the snapshot iterators skip every trie node whose hash starts with the
// snapshot prefix, so one Next can run for minutes while stopGeneration waits.
type abortableIterator struct {
	ethdb.Iterator
	cancel <-chan struct{}
	err    error
}

func newAbortableIterator(it ethdb.Iterator, cancel <-chan struct{}) ethdb.Iterator {
	return &abortableIterator{Iterator: it, cancel: cancel}
}

func (it *abortableIterator) Next() bool {
	if it.err != nil {
		return false
	}
	select {
	case <-it.cancel:
		it.err = errAborted
		return false
	default:
		return it.Iterator.Next()
	}
}

func (it *abortableIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	return it.Iterator.Error()
}

// keepProgress saves the work an aborted run finished, as checkAndFlush does
// when it is the one to see the stop request. Past ctx.done the batch holds only
// deletions of entries missing from the trie, which are safe to keep.
func (dl *diskLayer) keepProgress(ctx *generatorContext) {
	if ctx.progress == nil {
		return
	}
	if ctx.batch.ValueSize() == 0 && bytes.Equal(ctx.progress, dl.genMarker) {
		return // checkAndFlush saw the stop and has saved everything already
	}
	journalProgress(ctx.batch, ctx.progress, ctx.stats)
	if err := ctx.batch.Write(); err != nil {
		log.Error("Failed to flush batch", "err", err)
		return
	}
	ctx.batch.Reset()

	dl.lock.Lock()
	dl.genMarker = ctx.progress
	dl.lock.Unlock()
}
