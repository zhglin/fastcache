// Package fastcache implements fast in-memory cache.
//
// The package has been extracted from https://victoriametrics.com/
package fastcache

import (
	"fmt"
	"sync"
	"sync/atomic"

	xxhash "github.com/cespare/xxhash/v2"
)

// bucket的数量
const bucketsCount = 512

// 每个chunk中的内存片段大小
const chunkSize = 64 * 1024

// bucket内存的最大位数
const bucketSizeBits = 40

// 循环写的最大位数
const genSizeBits = 64 - bucketSizeBits

const maxGen = 1<<genSizeBits - 1

// 每个bucket的最大内存
const maxBucketSize uint64 = 1 << bucketSizeBits

// Stats represents cache stats.
//
// Use Cache.UpdateStats for obtaining fresh stats from the cache.
type Stats struct {
	// GetCalls is the number of Get calls.
	GetCalls uint64

	// SetCalls is the number of Set calls.
	SetCalls uint64

	// Misses is the number of cache misses.
	Misses uint64

	// Collisions is the number of cache collisions.
	//
	// Usually the number of collisions must be close to zero.
	// High number of collisions suggest something wrong with cache.
	Collisions uint64

	// Corruptions is the number of detected corruptions of the cache.
	//
	// Corruptions may occur when corrupted cache is loaded from file.
	Corruptions uint64

	// EntriesCount is the current number of entries in the cache.
	EntriesCount uint64

	// BytesSize is the current size of the cache in bytes.
	BytesSize uint64

	// MaxBytesSize is the maximum allowed size of the cache in bytes (aka capacity).
	MaxBytesSize uint64

	// BigStats contains stats for GetBig/SetBig methods.
	BigStats
}

// Reset resets s, so it may be re-used again in Cache.UpdateStats.
func (s *Stats) Reset() {
	*s = Stats{}
}

// BigStats contains stats for GetBig/SetBig methods.
type BigStats struct {
	// GetBigCalls is the number of GetBig calls.
	GetBigCalls uint64

	// SetBigCalls is the number of SetBig calls.
	SetBigCalls uint64

	// TooBigKeyErrors is the number of calls to SetBig with too big key.
	TooBigKeyErrors uint64

	// InvalidMetavalueErrors is the number of calls to GetBig resulting
	// to invalid metavalue.
	InvalidMetavalueErrors uint64

	// InvalidValueLenErrors is the number of calls to GetBig resulting
	// to a chunk with invalid length.
	InvalidValueLenErrors uint64

	// InvalidValueHashErrors is the number of calls to GetBig resulting
	// to a chunk with invalid hash value.
	InvalidValueHashErrors uint64
}

func (bs *BigStats) reset() {
	atomic.StoreUint64(&bs.GetBigCalls, 0)
	atomic.StoreUint64(&bs.SetBigCalls, 0)
	atomic.StoreUint64(&bs.TooBigKeyErrors, 0)
	atomic.StoreUint64(&bs.InvalidMetavalueErrors, 0)
	atomic.StoreUint64(&bs.InvalidValueLenErrors, 0)
	atomic.StoreUint64(&bs.InvalidValueHashErrors, 0)
}

// Cache is a fast thread-safe inmemory cache optimized for big number
// of entries.
//
// It has much lower impact on GC comparing to a simple `map[string][]byte`.
//
// Use New or LoadFromFile* for creating new cache instance.
// Concurrent goroutines may call any Cache methods on the same cache instance.
//
// Call Reset when the cache is no longer needed. This reclaims the allocated
// memory.
// Cache是一个快速的线程安全的内存缓存优化了大量的条目。
// 与简单的map[string][]byte相比，它对GC的影响要小得多。
// 使用New或LoadFromFile*创建新的缓存实例。
// 并发的goroutines可以调用同一个Cache实例上的任何Cache方法。
// 当缓存不再需要时调用Reset。这将回收已分配的内存。
type Cache struct {
	buckets [bucketsCount]bucket

	bigStats BigStats
}

// New returns new cache with the given maxBytes capacity in bytes.
//
// maxBytes must be smaller than the available RAM size for the app,
// since the cache holds data in memory.
//
// If maxBytes is less than 32MB, then the minimum cache capacity is 32MB.
// New返回给定maxBytes容量的新缓存(以字节为单位)。
// maxBytes必须小于应用程序的可用RAM大小，因为缓存在内存中保存数据。
// 如果maxBytes小于32MB，则最小缓存容量为32MB。
func New(maxBytes int) *Cache {
	if maxBytes <= 0 {
		panic(fmt.Errorf("maxBytes must be greater than 0; got %d", maxBytes))
	}
	var c Cache
	// 每个bucket的内存大小
	maxBucketBytes := uint64((maxBytes + bucketsCount - 1) / bucketsCount)
	for i := range c.buckets[:] {
		c.buckets[i].Init(maxBucketBytes)
	}
	return &c
}

// Set stores (k, v) in the cache.
//
// Get must be used for reading the stored entry.
//
// The stored entry may be evicted at any time either due to cache
// overflow or due to unlikely hash collision.
// Pass higher maxBytes value to New if the added items disappear
// frequently.
//
// (k, v) entries with summary size exceeding 64KB aren't stored in the cache.
// SetBig can be used for storing entries exceeding 64KB.
//
// k and v contents may be modified after returning from Set.
// 在缓存中设置存储(k, v)。
// Get必须用于读取存储的条目。
// 存储的条目可能在任何时候由于缓存溢出或由于不太可能的哈希冲突而被删除。
// 如果添加的项经常消失，则将更高的maxBytes值传递给New。
// (k, v)汇总大小超过64KB的条目不存储在缓存中。
// SetBig可用于存储超过64KB的条目。
// k和v的内容可以在返回Set后修改。
func (c *Cache) Set(k, v []byte) {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	c.buckets[idx].Set(k, v, h)
}

// Get appends value by the key k to dst and returns the result.
//
// Get allocates new byte slice for the returned value if dst is nil.
//
// Get returns only values stored in c via Set.
//
// k contents may be modified after returning from Get.
// Get将值按键k追加到dst并返回结果。
// 如果dst为nil, Get将为返回值分配新的字节片。
// Get只返回通过Set存储在c中的值。
// 从Get返回后可以修改k个内容。
func (c *Cache) Get(dst, k []byte) []byte {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	dst, _ = c.buckets[idx].Get(dst, k, h, true)
	return dst
}

// HasGet works identically to Get, but also returns whether the given key
// exists in the cache. This method makes it possible to differentiate between a
// stored nil/empty value versus and non-existing value.
func (c *Cache) HasGet(dst, k []byte) ([]byte, bool) {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	return c.buckets[idx].Get(dst, k, h, true)
}

// Has returns true if entry for the given key k exists in the cache.
func (c *Cache) Has(k []byte) bool {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	_, ok := c.buckets[idx].Get(nil, k, h, false)
	return ok
}

// Del deletes value for the given k from the cache.
//
// k contents may be modified after returning from Del.
func (c *Cache) Del(k []byte) {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	c.buckets[idx].Del(h)
}

// Reset removes all the items from the cache.
func (c *Cache) Reset() {
	for i := range c.buckets[:] {
		c.buckets[i].Reset()
	}
	c.bigStats.reset()
}

// UpdateStats adds cache stats to s.
//
// Call s.Reset before calling UpdateStats if s is re-used.
func (c *Cache) UpdateStats(s *Stats) {
	for i := range c.buckets[:] {
		c.buckets[i].UpdateStats(s)
	}
	s.GetBigCalls += atomic.LoadUint64(&c.bigStats.GetBigCalls)
	s.SetBigCalls += atomic.LoadUint64(&c.bigStats.SetBigCalls)
	s.TooBigKeyErrors += atomic.LoadUint64(&c.bigStats.TooBigKeyErrors)
	s.InvalidMetavalueErrors += atomic.LoadUint64(&c.bigStats.InvalidMetavalueErrors)
	s.InvalidValueLenErrors += atomic.LoadUint64(&c.bigStats.InvalidValueLenErrors)
	s.InvalidValueHashErrors += atomic.LoadUint64(&c.bigStats.InvalidValueHashErrors)
}

type bucket struct {
	mu sync.RWMutex

	// chunks is a ring buffer with encoded (k, v) pairs.
	// It consists of 64KB chunks.
	// chunks是一个带有编码(k, v)对的循环缓冲区。它由64KB的块组成。
	chunks [][]byte

	// m maps hash(k) to idx of (k, v) pair in chunks.
	// hash值对应的idx(起始长度)|gen
	// hash冲突时会覆盖，相同的key写入会存多份，读取返回最新的
	m map[uint64]uint64

	// idx points to chunks for writing the next (k, v) pair.
	// Idx指向写下一个(k, v)对的块。
	// 记录的是所有kv的总长度，未发生循环写就一直累加
	idx uint64

	// gen is the generation of chunks.
	// chunks循环写的次数
	gen uint64

	getCalls    uint64 // get调用次数
	setCalls    uint64 // set调用次数
	misses      uint64 // 未查到次数
	collisions  uint64 // 碰撞次数
	corruptions uint64
}

// Init bucket的初始化
func (b *bucket) Init(maxBytes uint64) {
	if maxBytes == 0 {
		panic(fmt.Errorf("maxBytes cannot be zero"))
	}
	if maxBytes >= maxBucketSize {
		panic(fmt.Errorf("too big maxBytes=%d; should be smaller than %d", maxBytes, maxBucketSize))
	}
	maxChunks := (maxBytes + chunkSize - 1) / chunkSize
	b.chunks = make([][]byte, maxChunks) // maxChunks个[]byte，每个[]byte的大小为chunkSize
	b.m = make(map[uint64]uint64)
	b.Reset() // 重置下chunk
}

// Reset 重置chunk
func (b *bucket) Reset() {
	b.mu.Lock()
	chunks := b.chunks
	// 初始化内存块
	for i := range chunks {
		putChunk(chunks[i])
		chunks[i] = nil
	}
	b.m = make(map[uint64]uint64)
	b.idx = 0
	b.gen = 1
	atomic.StoreUint64(&b.getCalls, 0)
	atomic.StoreUint64(&b.setCalls, 0)
	atomic.StoreUint64(&b.misses, 0)
	atomic.StoreUint64(&b.collisions, 0)
	atomic.StoreUint64(&b.corruptions, 0)
	b.mu.Unlock()
}

func (b *bucket) cleanLocked() {
	bGen := b.gen & ((1 << genSizeBits) - 1)
	bIdx := b.idx
	bm := b.m
	for k, v := range bm {
		gen := v >> bucketSizeBits
		idx := v & ((1 << bucketSizeBits) - 1)
		if (gen+1 == bGen || gen == maxGen && bGen == 1) && idx >= bIdx || gen == bGen && idx < bIdx {
			continue
		}
		delete(bm, k)
	}
}

func (b *bucket) UpdateStats(s *Stats) {
	s.GetCalls += atomic.LoadUint64(&b.getCalls)
	s.SetCalls += atomic.LoadUint64(&b.setCalls)
	s.Misses += atomic.LoadUint64(&b.misses)
	s.Collisions += atomic.LoadUint64(&b.collisions)
	s.Corruptions += atomic.LoadUint64(&b.corruptions)

	b.mu.RLock()
	s.EntriesCount += uint64(len(b.m))
	bytesSize := uint64(0)
	for _, chunk := range b.chunks {
		bytesSize += uint64(cap(chunk))
	}
	s.BytesSize += bytesSize
	s.MaxBytesSize += uint64(len(b.chunks)) * chunkSize
	b.mu.RUnlock()
}

func (b *bucket) Set(k, v []byte, h uint64) {
	atomic.AddUint64(&b.setCalls, 1)
	if len(k) >= (1<<16) || len(v) >= (1<<16) {
		// Too big key or value - its length cannot be encoded
		// with 2 bytes (see below). Skip the entry.
		// 太大的键或值-其长度不能用2个字节编码(见下文)。跳过的条目。
		return
	}
	// 四个字节buf，
	var kvLenBuf [4]byte
	kvLenBuf[0] = byte(uint16(len(k)) >> 8)
	kvLenBuf[1] = byte(len(k))
	kvLenBuf[2] = byte(uint16(len(v)) >> 8)
	kvLenBuf[3] = byte(len(v))
	// 不要存储太大的键和值，因为它们不适合一个块。
	kvLen := uint64(len(kvLenBuf) + len(k) + len(v))
	if kvLen >= chunkSize {
		// Do not store too big keys and values, since they do not
		// fit a chunk.
		return
	}

	chunks := b.chunks
	needClean := false
	b.mu.Lock()
	idx := b.idx                      // 现有长度
	idxNew := idx + kvLen             // 最新长度
	chunkIdx := idx / chunkSize       // 之前的chunk下标
	chunkIdxNew := idxNew / chunkSize // 最新的chunk下标
	// 因为idxNew比idx大，所以chunkIdxNew可能等于或者大于chunkIdx
	// chunkIdx是idx与chunkSize的整数倍，保证每个chunk的长度为chunkSize
	if chunkIdxNew > chunkIdx {
		// chunkIndNew不在0-len(chunks1)-1的范围，从0开始重新写
		if chunkIdxNew >= uint64(len(chunks)) {
			// 重置idx，idxNew,chunkIdx
			idx = 0
			idxNew = kvLen
			chunkIdx = 0
			// 增加循环次数
			b.gen++
			// 当 gen 等于 1<<genSizeBits时，才会等于0
			// 也就是用来限定 gen 的边界为1<<genSizeBits
			// genSizeBits=2
			//  1，2，3，
			//  5，6，7
			//  9,10,11
			if b.gen&((1<<genSizeBits)-1) == 0 {
				b.gen++
			}
			needClean = true
		} else {
			// 未超过chunks数量，从下一个chunk开始
			idx = chunkIdxNew * chunkSize // 重写旧址
			idxNew = idx + kvLen          // 新值
			chunkIdx = chunkIdxNew        // 设置chunkIdx
		}

		// 重置chunks[chunkIdx]数据为空
		chunks[chunkIdx] = chunks[chunkIdx][:0]
	}

	// 如果chunk[chunkIdx]内存为分配，分配内存
	chunk := chunks[chunkIdx]
	if chunk == nil {
		chunk = getChunk()
		chunk = chunk[:0] // 清空数据
	}
	// 写入长度
	chunk = append(chunk, kvLenBuf[:]...)
	// 写入key
	chunk = append(chunk, k...)
	// 写入value
	chunk = append(chunk, v...)
	chunks[chunkIdx] = chunk
	// 因为 idx 不能超过bucketSizeBits，所以用一个 uint64 同时表示gen和idx
	// 所以高于bucketSizeBits位置表示gen
	// 低于bucketSizeBits位置表示idx
	b.m[h] = idx | (b.gen << bucketSizeBits)
	b.idx = idxNew
	if needClean {
		b.cleanLocked()
	}
	b.mu.Unlock()
}

func (b *bucket) Get(dst, k []byte, h uint64, returnDst bool) ([]byte, bool) {
	atomic.AddUint64(&b.getCalls, 1)
	found := false
	chunks := b.chunks
	b.mu.RLock()
	v := b.m[h]
	bGen := b.gen & ((1 << genSizeBits) - 1)
	// hash值存在
	if v > 0 {
		// 高于bucketSizeBits位置表示gen
		gen := v >> bucketSizeBits
		// 低于bucketSizeBits位置表示idx
		idx := v & ((1 << bucketSizeBits) - 1)
		// 虽然循环写了，但是还没写满
		if gen == bGen && idx < b.idx ||
			// 循环写满了，但是数据还没被覆盖
			gen+1 == bGen && idx >= b.idx ||
			// 这里是边界条件gen已是最大，并且chunks已被写满bGen从1开始，并且当前数据没有被覆盖
			gen == maxGen && bGen == 1 && idx >= b.idx {
			chunkIdx := idx / chunkSize
			if chunkIdx >= uint64(len(chunks)) {
				// Corrupted data during the load from file. Just skip it.
				// 从文件加载期间损坏的数据。直接跳过它。
				atomic.AddUint64(&b.corruptions, 1)
				goto end
			}
			chunk := chunks[chunkIdx]
			idx %= chunkSize // chunk内的起始位置
			if idx+4 >= chunkSize {
				// Corrupted data during the load from file. Just skip it.
				// 从文件加载期间损坏的数据。直接跳过它。
				atomic.AddUint64(&b.corruptions, 1)
				goto end
			}
			kvLenBuf := chunk[idx : idx+4]
			keyLen := (uint64(kvLenBuf[0]) << 8) | uint64(kvLenBuf[1])
			valLen := (uint64(kvLenBuf[2]) << 8) | uint64(kvLenBuf[3])
			idx += 4 // 跳过keyLen,valLen
			if idx+keyLen+valLen >= chunkSize {
				// Corrupted data during the load from file. Just skip it.
				// 从文件加载期间损坏的数据。直接跳过它。
				atomic.AddUint64(&b.corruptions, 1)
				goto end
			}
			// key相等
			if string(k) == string(chunk[idx:idx+keyLen]) {
				idx += keyLen
				// 对应的值
				if returnDst {
					dst = append(dst, chunk[idx:idx+valLen]...)
				}
				found = true
			} else {
				atomic.AddUint64(&b.collisions, 1)
			}
		}
	}
end:
	b.mu.RUnlock()
	if !found {
		atomic.AddUint64(&b.misses, 1)
	}
	return dst, found
}

func (b *bucket) Del(h uint64) {
	b.mu.Lock()
	delete(b.m, h)
	b.mu.Unlock()
}
