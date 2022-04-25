//go:build !appengine && !windows
// +build !appengine,!windows

package fastcache

import (
	"fmt"
	"sync"
	"unsafe"

	"golang.org/x/sys/unix"
)

const chunksPerAlloc = 1024

var (
	freeChunks     []*[chunkSize]byte // 空闲的chunk内存块
	freeChunksLock sync.Mutex
)

// chunk内存快分配
func getChunk() []byte {
	freeChunksLock.Lock()
	if len(freeChunks) == 0 {
		// Allocate offheap memory, so GOGC won't take into account cache size.
		// This should reduce free memory waste.
		// 分配堆外内存，这样GOGC不会考虑缓存大小。
		// 这将减少可用内存的浪费。
		// fd可以指定为-1，此时须指定flags参数中的MAP_ANON，表明进行的是匿名映射
		// 一次分配chunksPerAlloc个chunk
		data, err := unix.Mmap(-1, 0, chunkSize*chunksPerAlloc, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
		if err != nil {
			panic(fmt.Errorf("cannot allocate %d bytes via mmap: %s", chunkSize*chunksPerAlloc, err))
		}
		for len(data) > 0 {
			p := (*[chunkSize]byte)(unsafe.Pointer(&data[0]))
			freeChunks = append(freeChunks, p)
			data = data[chunkSize:] // 跳出for
		}
	}
	n := len(freeChunks) - 1
	p := freeChunks[n]
	freeChunks[n] = nil         //
	freeChunks = freeChunks[:n] // 返回最后一个空闲chunk
	freeChunksLock.Unlock()
	return p[:]
}

// 初始化chunk的内存块
func putChunk(chunk []byte) {
	// 没数据直接返回
	if chunk == nil {
		return
	}

	// 已经有数据，获取首地址
	chunk = chunk[:chunkSize]
	p := (*[chunkSize]byte)(unsafe.Pointer(&chunk[0]))

	// 添加到空闲的内存块
	freeChunksLock.Lock()
	freeChunks = append(freeChunks, p)
	freeChunksLock.Unlock()
}
