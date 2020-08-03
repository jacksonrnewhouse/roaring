package roaring

import (
	"fmt"
)

type ImmutableBitmap struct {
	data        []byte
	containers  int
	isRunBitmap []byte
	header      []uint16
	offsets     []uint32
}

func (bitmap *ImmutableBitmap) FromBuffer(bytes []byte) error {
	pointer := uint32(0)
	cookie := ReadSingleInt(bytes, pointer)
	pointer += 4

	if cookie&0x0000FFFF == serialCookie {
		bitmap.containers = int(uint16(cookie>>16) + 1)
		// create is-run-container bitmap
		isRunBitmapSize := (int(bitmap.containers) + 7) / 8
		bitmap.isRunBitmap = bytes[pointer : isRunBitmapSize+int(pointer)]
		pointer += uint32(isRunBitmapSize)
	} else if cookie == serialCookieNoRunContainer {
		bitmap.containers = int(ReadSingleInt(bytes, 4))
		pointer += 4
	} else {
		return fmt.Errorf("error in roaringArray.readFrom: did not find expected serialCookie in header")
	}

	if bitmap.containers > (1 << 16) {
		return fmt.Errorf("it is logically impossible to have more than (1<<16) containers")
	}

	// descriptive header
	buf := bytes[pointer : int(pointer)+4*bitmap.containers]
	pointer += 4 * uint32(bitmap.containers)

	bitmap.header = byteSliceAsUint16Slice(buf)

	if bitmap.isRunBitmap == nil || bitmap.containers >= noOffsetThreshold {
		bitmap.offsets = byteSliceAsUint32Slice(bytes[pointer : pointer+4*uint32(bitmap.containers)])
	} else {
		// we traverse once to calculate offsets.
		for i := 0; i < bitmap.containers; i++ {
			bitmap.offsets = append(bitmap.offsets, pointer)
			if bitmap.isRunBitmap[i/8]&(1<<(i%8)) != 0 {
				// run container
				nr := ReadSingleShort(bytes, pointer)
				pointer += 2 + 4*uint32(nr)
			} else {
				card := uint32(bitmap.header[2*i+1]) + 1
				if card > arrayDefaultMaxSize {
					pointer += 2 * arrayDefaultMaxSize
				} else {
					pointer += 2 * card
				}
			}
		}
	}
	bitmap.data = bytes
	return nil
}

func (bitmap *ImmutableBitmap) getContainerCount() int {
	return bitmap.containers
}

func (bitmap *ImmutableBitmap) IsEmpty() bool {
	return bitmap.containers == 0
}

func (bitmap *ImmutableBitmap) getOffsetForKeyAtPosition(pos int) uint32 {
	return bitmap.offsets[pos]
}

func (bitmap *ImmutableBitmap) getCardinalityMinusOneFromContainerIndex(pos int) uint16 {
	return bitmap.header[2*pos+1]
}

func (bitmap *ImmutableBitmap) isRunAtIndex(pos int) bool {
	return bitmap.isRunBitmap != nil && bitmap.isRunBitmap[pos/8]&(1<<(pos%8)) != 0
}

//TODO: see if we can use copyOnWrite to not make copies here.
func (bitmap *ImmutableBitmap) getContainerClone(pos int) container {
	pointer := bitmap.getOffsetForKeyAtPosition(pos)
	if bitmap.isRunAtIndex(pos) {
		// run container
		nr := ReadSingleShort(bitmap.data, pointer)
		pointer += 2
		return newRunContainer16CopyIv(byteSliceAsInterval16Slice(bitmap.data[pointer : 4*nr]))
	} else {
		cardMinusOne := bitmap.header[2*pos+1]
		if cardMinusOne < arrayDefaultMaxSize {
			copySlice := make([]uint16, cardMinusOne+1)
			copy(copySlice, byteSliceAsUint16Slice(bitmap.data[pointer:pointer+2+2*uint32(cardMinusOne)]))
			return &arrayContainer{content: copySlice}
		} else {
			// bitmap container
			copySlice := make([]uint64, bitmapLongCount)
			copy(copySlice, byteSliceAsUint64Slice(bitmap.data[pointer:pointer+containerStorageBytes]))

			return &bitmapContainer{
				cardinality: int(cardMinusOne + 1),
				bitmap:      copySlice,
			}
		}
	}
}

// this method must not leak the container into other bitmaps.
// the primary (maybe only?) purpose is to pass to the inplace methods
// of other containers.
func (bitmap *ImmutableBitmap) getContainer(pos int) container {
	pointer := bitmap.getOffsetForKeyAtPosition(pos)
	if bitmap.isRunAtIndex(pos) {
		// run container
		nr := ReadSingleShort(bitmap.data, pointer)
		pointer += 2
		//this is dangerous, but this
		return newRunContainer16TakeOwnership(byteSliceAsInterval16Slice(bitmap.data[pointer : pointer+4*uint32(nr)]))
	} else {
		cardMinusOne := bitmap.header[2*pos+1]
		if cardMinusOne < arrayDefaultMaxSize {
			copySlice := make([]uint16, cardMinusOne+1)
			copy(copySlice, byteSliceAsUint16Slice(bitmap.data[pointer:pointer+2+2*uint32(cardMinusOne)]))
			return &arrayContainer{content: copySlice}
		} else {
			// bitmap container
			copySlice := make([]uint64, bitmapLongCount)
			copy(copySlice, byteSliceAsUint64Slice(bitmap.data[pointer:pointer+containerStorageBytes]))

			return &bitmapContainer{
				cardinality: int(cardMinusOne + 1),
				bitmap:      copySlice,
			}
		}
	}
}

func (bitmap *ImmutableBitmap) getKeyAtContainerIndex(index int) uint16 {
	return bitmap.header[2*index]
}

func (bitmap *ImmutableBitmap) GetCardinality() uint64 {
	var result uint64
	for i := 0; i < bitmap.containers; i++ {
		result += uint64(bitmap.header[2*i+1]) + 1
	}
	return result
}

func (bitmap *Bitmap) OrAgainstImmutable(x2 *ImmutableBitmap) {
	pos1 := 0
	pos2 := 0
	length1 := bitmap.highlowcontainer.size()
	length2 := x2.getContainerCount()
main:
	for (pos1 < length1) && (pos2 < length2) {
		s1 := bitmap.highlowcontainer.getKeyAtIndex(pos1)
		s2 := x2.getKeyAtContainerIndex(pos2)

		for {
			if s1 < s2 {
				pos1++
				if pos1 == length1 {
					break main
				}
				s1 = bitmap.highlowcontainer.getKeyAtIndex(pos1)
			} else if s1 > s2 {
				bitmap.highlowcontainer.insertNewKeyValueAt(pos1, s2, x2.getContainer(pos2))
				bitmap.highlowcontainer.needCopyOnWrite[pos1] = true
				pos1++
				length1++
				pos2++
				if pos2 == length2 {
					break main
				}
				s2 = x2.getKeyAtContainerIndex(pos2)
			} else {
				writableContainer := bitmap.highlowcontainer.getWritableContainerAtIndex(pos1)
				newContainer := writableContainer.iorBytes(x2.isRunAtIndex(pos2), x2.getCardinalityMinusOneFromContainerIndex(pos2), x2.getBytesFromContainerIndex(pos2))
				if newContainer != nil {
					bitmap.highlowcontainer.replaceKeyAndContainerAtIndex(pos1, s1, newContainer, false)
				}
				pos1++
				pos2++
				if (pos1 == length1) || (pos2 == length2) {
					break main
				}
				s1 = bitmap.highlowcontainer.getKeyAtIndex(pos1)
				s2 = x2.getKeyAtContainerIndex(pos2)
			}
		}
	}
	if pos1 == length1 {
		for pos2 < length2 {
			s2 := x2.getKeyAtContainerIndex(pos2)
			bitmap.highlowcontainer.insertNewKeyValueAt(pos1, s2, x2.getContainer(pos2))
			bitmap.highlowcontainer.needCopyOnWrite[pos1] = true
			pos1++
			length1++
			pos2++
		}
	}
}

/*

TODO: decide to implement functons on Bitmap for ImmutableBitmap, or delete this commented-out code.

// returns the cardinality of the xor.
// this allows for the caller to remove it from the header if necessary.
func (bitmap *RoaringBitmap) computeAndAgainst(other *RoaringBitmap) {
	pos1 := uint32(0)
	pos2 := uint32(0)
	intersectionsize := uint32(0)
	length1 := bitmap.getContainerCount()
	length2 := other.getContainerCount()

main:
	for {
		if pos1 < length1 && pos2 < length2 {
			s1 := bitmap.getKeyAtContainerIndex(pos1)
			s2 := other.getKeyAtContainerIndex(pos2)
			for {
				if s1 == s2 {
					cardShort := other.getCardinalityMinusOneFromContainerIndex(pos2)
					offset := other.getOffsetForKeyAtPosition(uint32(s2), pos2)
					intersectionCard := bitmap.andContainerAtIndex(uint32(s1), pos1, other.data, offset, cardShort)
					if intersectionCard > 0 {
						// the  offset never changes, just the cardinality.
						if intersectionsize < pos1 {
							// the headers moved, write the new key
							WriteShort(bitmap.header, bytesPerContainer*intersectionsize, s1)
						}
						WriteShort(bitmap.header, bytesPerContainer*intersectionsize+cardinalityIncrement, uint16(intersectionCard-1))
						intersectionsize++
					}
					pos1++
					pos2++
					if pos1 == length1 || pos2 == length2 {
						break main
					}
					s1 = bitmap.getKeyAtContainerIndex(pos1)
					s2 = other.getKeyAtContainerIndex(pos2)
				} else if s1 < s2 {
					// TODO:  this isn't as fast as highlowcontainer.advanceUntil()
					//        which does a fancy binary search. Port that over.
					for s1 < s2 {
						pos1++
						if pos1 == length1 {
							break main
						}
						s1 = bitmap.getKeyAtContainerIndex(pos1)
					}
				} else {
					for s1 > s2 {
						pos2++
						if pos2 == length2 {
							break main
						}
						s2 = other.getKeyAtContainerIndex(pos2)
					}
				}
			}
		} else {
			break
		}
	}
	bitmap.containers = intersectionsize
}

func (bitmap *RoaringBitmap) computeAndNotAgainst(other *RoaringBitmap) {
	pos1 := uint32(0)
	pos2 := uint32(0)
	intersectionsize := uint32(0)
	length1 := bitmap.getContainerCount()
	length2 := other.getContainerCount()
main:
	for {
		if pos1 < length1 && pos2 < length2 {
			s1 := bitmap.getKeyAtContainerIndex(pos1)
			s2 := other.getKeyAtContainerIndex(pos2)
			for {
				if s1 == s2 {
					cardShort := other.getCardinalityMinusOneFromContainerIndex(pos2)
					offset := other.getOffsetForKeyAtPosition(uint32(s2), pos2)
					intersectionCard := bitmap.andNotContainerAtIndex(pos1, other.data, offset, cardShort)
					if intersectionCard > 0 {
						// the  offset never changes, just the cardinality.
						if intersectionsize != pos1 {
							// the headers moved, write the new key
							WriteShort(bitmap.header, bytesPerContainer*intersectionsize, s1)
						}
						WriteShort(bitmap.header, bytesPerContainer*intersectionsize+cardinalityIncrement, uint16(intersectionCard-1))
						intersectionsize++
					}
					pos1++
					pos2++
					if pos1 == length1 || pos2 == length2 {
						break main
					}
					s1 = bitmap.getKeyAtContainerIndex(pos1)
					s2 = other.getKeyAtContainerIndex(pos2)
				} else if s1 < s2 {
					if pos1 != intersectionsize {
						// need to copy 4 bytes. Is this faster than copy?
						WriteInt(bitmap.header, bytesPerContainer*intersectionsize,
							ReadSingleInt(bitmap.header, bytesPerContainer*pos1))
					}
					intersectionsize++
					pos1++
					if pos1 == length1 {
						break main
					}
					s1 = bitmap.getKeyAtContainerIndex(pos1)
				} else {
					for s1 > s2 {
						pos2++
						if pos2 == length2 {
							break main
						}
						s2 = other.getKeyAtContainerIndex(pos2)
					}
				}
			}
		} else {
			break
		}
	}
	if pos1 < length1 {
		if intersectionsize != pos1 {
			copy(bitmap.header[bytesPerContainer*intersectionsize:bytesPerContainer*(intersectionsize+length1-pos1)],
				bitmap.header[bytesPerContainer*pos1:bytesPerContainer*length1])
		}
		intersectionsize += length1 - pos1
	}
	bitmap.containers = intersectionsize
}

// Xor computes the symmetric difference between two bitmaps and stores the result in the current bitmap
func (rb *RoaringBitmap) computeXor(x2 *RoaringBitmap) {
	pos1 := uint32(0)
	pos2 := uint32(0)
	length1 := rb.getContainerCount()
	length2 := x2.getContainerCount()
	for {
		if (pos1 < length1) && (pos2 < length2) {
			s1 := rb.getKeyAtContainerIndex(pos1)
			s2 := x2.getKeyAtContainerIndex(pos2)
			if s1 < s2 {

				pos1++
				if pos1 == length1 {
					break
				}
				// TODO: binary advance
				s1 = rb.getKeyAtContainerIndex(pos1)
			} else if s1 > s2 {
				cardShort := x2.getCardinalityMinusOneFromContainerIndex(pos2)
				length := lengthFromCardinalityShort(cardShort)
				offset := x2.getOffsetForKeyAtPosition(uint32(s2), pos2)
				rb.insertNewContainerAtIndex(pos1, s2, cardShort, x2.data, offset, length)
				pos1++
				length1++
				pos2++
			} else {
				cardShort := x2.getCardinalityMinusOneFromContainerIndex(pos2)
				offset := x2.getOffsetForKeyAtPosition(uint32(s2), pos2)
				intersectionCard := rb.xOrContainerAtIndex(uint32(s1), pos1, x2.data, offset, cardShort)
				if intersectionCard > 0 {
					WriteShort(rb.header, bytesPerContainer*pos1+cardinalityIncrement, uint16(intersectionCard-1))
					pos1++
				} else {
					length1--
				}
				pos2++
			}
		} else {
			break
		}
	}
	if pos1 == length1 {
		for pos2 < length2 {
			s2 := x2.getKeyAtContainerIndex(pos2)
			cardShort := x2.getCardinalityMinusOneFromContainerIndex(pos2)
			length := lengthFromCardinalityShort(cardShort)
			offset := x2.getOffsetForKeyAtPosition(uint32(s2), pos2)
			rb.insertNewContainerAtIndex(pos1, s2, cardShort, x2.data, offset, length)
			pos1++
			length1++
			pos2++
		}
	}
	rb.containers = length1
}*/

func (bitmap *ImmutableBitmap) getBytesFromContainerIndex(pos2 int) []byte {
	offset := bitmap.offsets[pos2]
	if bitmap.isRunAtIndex(pos2) {
		nr := ReadSingleShort(bitmap.data, offset)
		return bitmap.data[offset : offset+2+4*uint32(nr)]
	} else {
		card := bitmap.getCardinalityMinusOneFromContainerIndex(pos2)
		if card < arrayDefaultMaxSize {
			return bitmap.data[offset : offset+2+2*uint32(card)]
		} else {
			return bitmap.data[offset : offset+containerStorageBytes]
		}
	}
}
