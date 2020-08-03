package roaring

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

func TestImmutableBitmap_FromBuffer(t *testing.T) {
	bitmap := NewBitmap()
	val := 0
	for i := 0; i < 100000; i++ {
		val += rand.Intn(50)
		bitmap.Add(uint32(val))
	}
	bitmap.RunOptimize()
	bytes, err := bitmap.ToBytes()
	assert.NoError(t, err)
	immutable := &ImmutableBitmap{}
	err = immutable.FromBuffer(bytes)
	assert.NoError(t, err)

	assert.Equal(t, bitmap.GetCardinality(), immutable.GetCardinality())
	orredBitmap := NewBitmap()
	orredBitmap.OrAgainstImmutable(immutable)
	orredBitmap.RunOptimize()
	for _, val := range bitmap.ToArray() {
		if !orredBitmap.Contains(val) {
			assert.True(t, orredBitmap.Contains(val))
		}
	}
	assert.Equal(t, bitmap.ToArray(), orredBitmap.ToArray())
}
