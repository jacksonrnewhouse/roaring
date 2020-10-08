package roaring

import (
	"fmt"
	"unsafe"
)

const (
	containerStorageBytes = 8192
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
		bitmap.isRunBitmap = nil
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
		bitmap.offsets = make([]uint32, 0, bitmap.containers)
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

func (bitmap *ImmutableBitmap) getContainer(pos int) container {
	pointer := bitmap.getOffsetForKeyAtPosition(pos)
	if bitmap.isRunAtIndex(pos) {
		// run container
		nr := ReadSingleShort(bitmap.data, pointer)
		pointer += 2
		return newRunContainer16TakeOwnership(byteSliceAsInterval16Slice(bitmap.data[pointer : pointer+4*uint32(nr)]))
	} else {
		cardMinusOne := bitmap.header[2*pos+1]
		if cardMinusOne < arrayDefaultMaxSize {
			return &arrayContainer{content: byteSliceAsUint16Slice(bitmap.data[pointer : pointer+2+2*uint32(cardMinusOne)])}
		} else {
			// bitmap container
			return &bitmapContainer{
				cardinality: int(cardMinusOne + 1),
				bitmap:      byteSliceAsUint64Slice(bitmap.data[pointer : pointer+containerStorageBytes]),
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

func (rb *Bitmap) AndCardinalityAgainstImmutable(x2 *ImmutableBitmap) uint64 {
	pos1 := 0
	pos2 := 0
	answer := uint64(0)
	length1 := rb.highlowcontainer.size()
	length2 := x2.getContainerCount()

main:
	for {
		if pos1 < length1 && pos2 < length2 {
			s1 := rb.highlowcontainer.getKeyAtIndex(pos1)
			s2 := x2.getKeyAtContainerIndex(pos2)
			for {
				if s1 == s2 {
					c1 := rb.highlowcontainer.getContainerAtIndex(pos1)
					answer += uint64(c1.byteAndCardinality(x2.isRunAtIndex(pos2), x2.getCardinalityMinusOneFromContainerIndex(pos2), x2.getBytesFromContainerIndex(pos2)))
					pos1++
					pos2++
					if (pos1 == length1) || (pos2 == length2) {
						break main
					}
					s1 = rb.highlowcontainer.getKeyAtIndex(pos1)
					s2 = x2.getKeyAtContainerIndex(pos2)
				} else if s1 < s2 {
					pos1 = rb.highlowcontainer.advanceUntil(s2, pos1)
					if pos1 == length1 {
						break main
					}
					s1 = rb.highlowcontainer.getKeyAtIndex(pos1)
				} else { //s1 > s2
					for s1 > s2 {
						pos2++
						if pos2 == length2 {
							break main
						}
						s2 = x2.getKeyAtContainerIndex(pos2)
					}
				}
			}
		} else {
			break
		}
	}
	return answer
}

func (bitmap *Bitmap) OrAgainstImmutableWithFilter(x2 *ImmutableBitmap, filter *Bitmap) {
	pos1 := 0
	pos2 := 0
	pos3 := 0
	length1 := bitmap.highlowcontainer.size()
	length2 := x2.getContainerCount()
	length3 := filter.highlowcontainer.size()
main:
	for (pos1 < length1) && (pos2 < length2) && (pos3 < length3) {
		s1 := bitmap.highlowcontainer.getKeyAtIndex(pos1)
		s2 := x2.getKeyAtContainerIndex(pos2)
		s3 := filter.highlowcontainer.getKeyAtIndex(pos3)

		for {
			if s2 < s3 {
				pos2++
				if pos2 == length2 {
					break main
				}
				s2 = x2.getKeyAtContainerIndex(pos2)
			} else if s3 < s2 {
				pos3++
				if pos3 == length3 {
					break
				}
				s3 = filter.highlowcontainer.getKeyAtIndex(pos3)
			} else if s1 < s2 {
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
				if bitmap.highlowcontainer.needCopyOnWrite[pos1] {
					bitmap.highlowcontainer.containers[pos1] = bitmap.highlowcontainer.containers[pos1].orBytes(
						x2.isRunAtIndex(pos2), x2.getCardinalityMinusOneFromContainerIndex(pos2), x2.getBytesFromContainerIndex(pos2))
					bitmap.highlowcontainer.needCopyOnWrite[pos1] = false
				} else {
					writableContainer := bitmap.highlowcontainer.containers[pos1]
					newContainer := writableContainer.iorBytes(x2.isRunAtIndex(pos2), x2.getCardinalityMinusOneFromContainerIndex(pos2), x2.getBytesFromContainerIndex(pos2))
					if newContainer != nil {
						bitmap.highlowcontainer.replaceKeyAndContainerAtIndex(pos1, s1, newContainer, false)
					}
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
				if bitmap.highlowcontainer.needCopyOnWrite[pos1] {
					bitmap.highlowcontainer.containers[pos1] = bitmap.highlowcontainer.containers[pos1].orBytes(
						x2.isRunAtIndex(pos2), x2.getCardinalityMinusOneFromContainerIndex(pos2), x2.getBytesFromContainerIndex(pos2))
					bitmap.highlowcontainer.needCopyOnWrite[pos1] = false
				} else {
					writableContainer := bitmap.highlowcontainer.containers[pos1]
					newContainer := writableContainer.iorBytes(x2.isRunAtIndex(pos2), x2.getCardinalityMinusOneFromContainerIndex(pos2), x2.getBytesFromContainerIndex(pos2))
					if newContainer != nil {
						bitmap.highlowcontainer.replaceKeyAndContainerAtIndex(pos1, s1, newContainer, false)
					}
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

func ReadSingleShort(data []byte, pointer uint32) uint16 {
	return *(*uint16)(unsafe.Pointer(&data[pointer]))
}

func ReadSingleInt(data []byte, pointer uint32) uint32 {
	return *(*uint32)(unsafe.Pointer(&data[pointer]))
}

func ReadSingleLong(data []byte, pointer uint32) uint64 {
	return *(*uint64)(unsafe.Pointer(&data[pointer]))
}
