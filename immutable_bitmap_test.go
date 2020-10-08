package roaring

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func generateRandomRunContainer(minRunLength, runLengthVariance, minGapRunLength int) *runContainer16 {
	resultContainer := newRunContainer16()
	for i := 0; i < (1 << 16); {
		posIncrement := minRunLength + rand.Intn(runLengthVariance)
		endRange := i + posIncrement
		if endRange >= (1 << 16) {
			endRange = 1 << 16
		}
		resultContainer.iaddRange(i, endRange)
		i += posIncrement
		negIncrement := minGapRunLength + rand.Intn(runLengthVariance)
		i += negIncrement
	}
	return resultContainer
}

func containerToBytes(t *testing.T, cont container) []byte {
	buff := &bytes.Buffer{}
	_, err := cont.writeTo(buff)
	require.NoError(t, err)
	return buff.Bytes()
}

func TestContainerByteOps(t *testing.T) {
	containerCount := 100
	containers := make([]container, containerCount)
	for i := 0; i < containerCount; i++ {
		containers[i] = generateRandomContainer()
	}

	for i := 0; i < containerCount; i++ {
		for j := 0; j < containerCount; j++ {
			checkByteOperations(t, containers[i].clone(), containers[j].clone())
		}
	}
}

func generateRandomContainer() container {
	switch rand.Intn(3) {
	case 0:
		return generateRandomRunContainer(10+rand.Intn(20), 10+rand.Intn(20), 10+rand.Intn(20)).toEfficientContainer()
	case 1:
		terms := 1 + rand.Intn(arrayDefaultMaxSize)
		result := newArrayContainerCapacity(terms)
		for i := 0; i < terms; {
			if result.iadd(uint16(rand.Intn(1 << 16))) {
				i++
			}
		}
		return result.toEfficientContainer()
	case 2:
		p := rand.Float64()
		result := newBitmapContainer()
		for i := 0; i < (1 << 16); i++ {
			if rand.Float64() < p {
				result.iadd(uint16(i))
			}
		}
		if result.cardinality == 0 {
			return generateRandomContainer()
		}
		return result.toEfficientContainer()
	}
	panic("unreachable")
}

func checkByteOperations(t *testing.T, left, right container) {
	expected := left.or(right)
	rightBytes := containerToBytes(t, right)
	_, isRunContainer := right.(*runContainer16)
	rightCardMinusOne := uint16(right.getCardinality() - 1)
	byteOrResult := left.orBytes(isRunContainer, rightCardMinusOne, rightBytes)

	assert.True(t, expected.equals(byteOrResult))

	assert.Equal(t, left.andCardinality(right),
		left.byteAndCardinality(isRunContainer, rightCardMinusOne, rightBytes))

	byteIorResult := left.iorBytes(isRunContainer, rightCardMinusOne, rightBytes)
	if byteIorResult == nil {
		byteIorResult = left
	}
	assert.True(t, expected.equals(byteIorResult))
}

func generateTestBitmap(minContainer, increment int, posContainerRate, copyOnWritePercentage float64) *Bitmap {
	result := NewBitmap()
	for key := minContainer; key < minContainer+increment+1; key++ {
		if rand.Float64() < posContainerRate {
			result.highlowcontainer.appendContainer(uint16(key), generateRandomContainer().toEfficientContainer(), rand.Float64() < copyOnWritePercentage)
		}
	}
	return result
}

func TestOrAgainstImmutableWithFilter(t *testing.T) {
	for i := 0; i < 100; i++ {
		containerVar := rand.Intn(50) + 1
		testOrAgainstImmutableWithFilterOnTriple(t,
			generateTestBitmap(rand.Intn(containerVar), rand.Intn(containerVar), rand.Float64(), rand.Float64()),
			generateTestBitmap(rand.Intn(containerVar), rand.Intn(containerVar), rand.Float64(), rand.Float64()),
			generateTestBitmap(rand.Intn(containerVar), rand.Intn(containerVar), rand.Float64(), rand.Float64()),
		)
	}
}

func testOrAgainstImmutableWithFilterOnTriple(t *testing.T, immutable, receiver, filter *Bitmap) {
	expected := And(filter, Or(receiver, immutable))
	bytesToRead, err := immutable.ToBytes()
	require.NoError(t, err)
	immutableBitmap := &ImmutableBitmap{}
	immutableBitmap.FromBuffer(bytesToRead)

	assert.Equal(t, int(receiver.AndCardinality(immutable)), int(receiver.AndCardinalityAgainstImmutable(immutableBitmap)))
	receiver.OrAgainstImmutableWithFilter(immutableBitmap, filter)
	receiver.And(filter)
	assert.True(t, expected.Equals(receiver))

	//Also check Or against Immutable. Can use the filter to call the inplace function.
	expectedOr := Or(immutable, filter)
	filter.OrAgainstImmutable(immutableBitmap)
	assert.True(t, expectedOr.Equals(filter))
}
