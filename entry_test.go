package eventual

import (
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestEntry(t *testing.T) {
	t.Run("older_version_never_wins", testEntryOlderVersionNeverWins)
	t.Run("concurrent_highest_version_wins", testEntryConcurrentHighestVersionWins)
	t.Run("without_versions_last_write_wins", testEntryWithoutVersions)
	t.Run("size", testEntrySize)
}

func testUpdate(name string, version int64) update[testItem] {
	return update[testItem]{value: &testItem{Name: name}, version: version}
}

// A slow writer must not overwrite a value stored by a writer with a higher
// version. `go test -race` cannot catch this - every operation on its own is
// atomic, only their order would be wrong.
func testEntryOlderVersionNeverWins(t *testing.T) {
	t.Parallel()

	e := newEntry(testUpdate("v100", 100))

	_, applied := e.apply(testUpdate("v102", 102))
	assert.True(t, applied)

	_, applied = e.apply(testUpdate("v101", 101))
	assert.False(t, applied)

	assert.Equal(t, "v102", e.value.Load().Name)
	assert.Equal(t, int64(102), e.version.Load())
}

func testEntryConcurrentHighestVersionWins(t *testing.T) {
	t.Parallel()

	const (
		rounds  = 200
		writers = 32
	)

	for round := 0; round < rounds; round++ {
		e := newEntry(testUpdate("v0", 0))
		e.version.Store(0)

		wg := sync.WaitGroup{}
		for v := 1; v <= writers; v++ {
			wg.Add(1)

			go func(version int64) {
				defer wg.Done()

				e.apply(update[testItem]{value: &testItem{ID: version}, version: version})
			}(int64(v))
		}
		wg.Wait()

		assert.Equal(t, int64(writers), e.version.Load(), "round %d", round)
		assert.Equal(t, int64(writers), e.value.Load().ID, "round %d", round)
	}
}

func testEntryWithoutVersions(t *testing.T) {
	t.Parallel()

	e := newEntry(testUpdate("first", 0))

	_, applied := e.apply(testUpdate("second", 0))
	assert.True(t, applied)
	assert.Equal(t, "second", e.value.Load().Name)
}

func testEntrySize(t *testing.T) {
	t.Parallel()

	t.Logf("entry=%d B (independent of the value type)", unsafe.Sizeof(entry[testItem]{}))
	assert.Equal(t, unsafe.Sizeof(entry[testItem]{}), unsafe.Sizeof(entry[[512]byte]{}))
}
