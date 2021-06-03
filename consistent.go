// Copyright (C) 2012 Numerotron Inc.
// Use of this source code is governed by an MIT-style license
// that can be found in the LICENSE file.

// Package consistent provides a consistent hashing function.
//
// Consistent hashing is often used to distribute requests to a changing set of servers.  For example,
// say you have some cache servers cacheA, cacheB, and cacheC.  You want to decide which cache server
// to use to look up information on a user.
//
// You could use a typical hash table and hash the user id
// to one of cacheA, cacheB, or cacheC.  But with a typical hash table, if you add or remove a server,
// almost all keys will get remapped to different results, which basically could bring your service
// to a grinding halt while the caches get rebuilt.
//
// With a consistent hash, adding or removing a server drastically reduces the number of keys that
// get remapped.
//
// Read more about consistent hashing on wikipedia:  http://en.wikipedia.org/wiki/Consistent_hashing
//
package consistent // import "stathat.com/c/consistent"

import (
	"errors"
	"hash/crc32"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

type uints []uint32

// Len returns the length of the uints array.
func (x uints) Len() int { return len(x) }

// Less returns true if element i is less than element j.
func (x uints) Less(i, j int) bool { return x[i] < x[j] }

// Swap exchanges elements i and j.
func (x uints) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

// ErrEmptyCircle is the error returned when trying to get an element when nothing has been added to hash.
var ErrEmptyCircle = errors.New("empty circle")

// Consistent holds the information about the members of the consistent hash circle.
type Consistent struct {
	circle                  map[uint32]string // key: [hash(i+elt)], the number of specific elt(number of i) depends on NumberOfReplicas
	members                 map[string]bool
	membersReplicas         map[string]int
	sortedHashes            uints //key of circle store here, for quick sort
	defaultNumberOfReplicas int
	count                   int64
	scratch                 [64]byte
	customHasher            Hasher
	useFnv                  bool
	sync.RWMutex
}
type Config struct {
	DefaultNumberOfReplicas int
	UseFnv                  bool
	CustomHasher            Hasher
}
type Hasher interface {
	HashFunc(key string) uint32
}

// New creates a new Consistent object with a default setting of 20 replicas for each entry.
//
// To change the number of replicas, set NumberOfReplicas before adding entries.
func New(conf Config) *Consistent {
	c := new(Consistent)
	c.defaultNumberOfReplicas = conf.DefaultNumberOfReplicas
	if c.defaultNumberOfReplicas == 0 {
		c.defaultNumberOfReplicas = 43
	}
	c.useFnv = conf.UseFnv
	c.customHasher = conf.CustomHasher
	c.circle = make(map[uint32]string)
	c.members = make(map[string]bool)
	c.membersReplicas = make(map[string]int)
	return c
}

// eltKey generates a string key for an element with an index.
func (c *Consistent) eltKey(elt string, idx int) string {
	// return elt + "|" + strconv.Itoa(idx)
	return strconv.Itoa(idx) + elt
}

// Add inserts a string element in the consistent hash.
func (c *Consistent) Add(elt string, numbersOfReplicas ...int) {
	c.Lock()
	defer c.Unlock()
	if _, ok := c.members[elt]; ok {
		return
	}
	numberOfReplicas := c.defaultNumberOfReplicas
	if len(numbersOfReplicas) > 0 {
		numberOfReplicas = numbersOfReplicas[0]
	}
	c.add(elt, numberOfReplicas)
}

// need c.Lock() before calling
func (c *Consistent) add(elt string, numberOfReplicas int) {
	for i := 0; i < numberOfReplicas; i++ {
		c.circle[c.hashKey(c.eltKey(elt, i))] = elt
	}
	c.members[elt] = true
	c.membersReplicas[elt] = numberOfReplicas
	c.updateSortedHashes()
	c.count++
}

// Remove removes an element from the hash.
// return true for Remove success, false for Remove does not work
func (c *Consistent) Remove(elt string) bool {
	c.Lock()
	defer c.Unlock()
	if _, ok := c.members[elt]; !ok {
		return false
	}
	numberOfReplicas, ok := c.membersReplicas[elt]
	if !ok {
		return false
	}
	c.remove(elt, numberOfReplicas)
	return true
}

// need c.Lock() before calling
func (c *Consistent) remove(elt string, numberOfReplicas int) {
	for i := 0; i < numberOfReplicas; i++ {
		delete(c.circle, c.hashKey(c.eltKey(elt, i)))
	}
	delete(c.members, elt)
	delete(c.membersReplicas, elt)
	c.updateSortedHashes()
	c.count--
	return
}

// Set sets all the elements in the hash.  If there are existing elements not
// present in elts, they will be removed.
// defaultNumberOfReplicas will be used to add member
func (c *Consistent) Set(elts []string) {
	c.Lock()
	defer c.Unlock()
	for k := range c.members {
		found := false
		for _, v := range elts {
			if k == v {
				found = true
				break
			}
		}
		if !found {
			if v, ok := c.membersReplicas[k]; ok {
				c.remove(k, v)
			}
		}
	}
	for _, v := range elts {
		_, exists := c.members[v]
		if exists {
			continue
		}
		c.add(v, c.defaultNumberOfReplicas)
	}
}

type SetElt struct {
	Elt              string
	NumberOfReplicas int
}

// SetWithReplicas sets all the elements in the hash with NumberOfReplicas.  If there are existing elements not
// present in elts, they will be removed.
func (c *Consistent) SetWithReplicas(elts []SetElt) {
	c.Lock()
	defer c.Unlock()
	for k := range c.members {
		found := false
		for _, v := range elts {
			if k == v.Elt {
				found = true
				break
			}
		}
		if !found {
			if v, ok := c.membersReplicas[k]; ok {
				c.remove(k, v)
			}
		}
	}
	for _, v := range elts {
		_, exists := c.members[v.Elt]
		if exists {
			continue
		}
		if v.NumberOfReplicas == 0 {
			v.NumberOfReplicas = c.defaultNumberOfReplicas
		}
		c.add(v.Elt, v.NumberOfReplicas)
	}
}

func (c *Consistent) Members() []string {
	c.RLock()
	defer c.RUnlock()
	var m []string
	for k := range c.members {
		m = append(m, k)
	}
	return m
}
func (c *Consistent) MemberReplicas() map[string]int {
	c.RLock()
	defer c.RUnlock()
	m := make(map[string]int, len(c.membersReplicas))
	for k, v := range c.membersReplicas {
		m[k] = v
	}
	return m
}

// Get returns an element close to where name hashes to in the circle.
func (c *Consistent) Get(name string) (string, error) {
	c.RLock()
	defer c.RUnlock()
	if len(c.circle) == 0 {
		return "", ErrEmptyCircle
	}
	key := c.hashKey(name)
	i := c.search(key)
	return c.circle[c.sortedHashes[i]], nil
}

func (c *Consistent) search(key uint32) (i int) {
	f := func(x int) bool {
		return c.sortedHashes[x] > key
	}
	i = sort.Search(len(c.sortedHashes), f)
	if i >= len(c.sortedHashes) {
		i = 0
	}
	return
}

// GetTwo returns the two closest distinct elements to the name input in the circle.
func (c *Consistent) GetTwo(name string) (string, string, error) {
	c.RLock()
	defer c.RUnlock()
	if len(c.circle) == 0 {
		return "", "", ErrEmptyCircle
	}
	key := c.hashKey(name)
	i := c.search(key)
	a := c.circle[c.sortedHashes[i]]

	if c.count == 1 {
		return a, "", nil
	}

	start := i
	var b string
	for i = start + 1; i != start; i++ {
		if i >= len(c.sortedHashes) {
			i = 0
		}
		b = c.circle[c.sortedHashes[i]]
		if b != a {
			break
		}
	}
	return a, b, nil
}

// GetN returns the N closest distinct elements to the name input in the circle.
func (c *Consistent) GetN(name string, n int) ([]string, error) {
	c.RLock()
	defer c.RUnlock()

	if len(c.circle) == 0 {
		return nil, ErrEmptyCircle
	}

	if c.count < int64(n) {
		n = int(c.count)
	}

	var (
		key   = c.hashKey(name)
		i     = c.search(key)
		start = i
		res   = make([]string, 0, n)
		elem  = c.circle[c.sortedHashes[i]]
	)

	res = append(res, elem)

	if len(res) == n {
		return res, nil
	}

	for i = start + 1; i != start; i++ {
		if i >= len(c.sortedHashes) {
			i = 0
		}
		elem = c.circle[c.sortedHashes[i]]
		if !sliceContainsMember(res, elem) {
			res = append(res, elem)
		}
		if len(res) == n {
			break
		}
	}

	return res, nil
}

func (c *Consistent) hashKey(key string) uint32 {
	if c.customHasher != nil {
		return c.customHasher.HashFunc(key)
	}
	if c.useFnv {
		return c.hashKeyFnv(key)
	}
	return c.hashKeyCRC32(key)
}

func (c *Consistent) hashKeyCRC32(key string) uint32 {
	if len(key) < 64 {
		var scratch [64]byte
		copy(scratch[:], key)
		return crc32.ChecksumIEEE(scratch[:len(key)])
	}
	return crc32.ChecksumIEEE([]byte(key))
}

func (c *Consistent) hashKeyFnv(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func (c *Consistent) updateSortedHashes() {
	hashes := c.sortedHashes[:0]
	//reallocate if we're holding on to too much (1/4th)
	if cap(c.sortedHashes)/(c.defaultNumberOfReplicas*4) > len(c.circle) {
		hashes = nil
	}
	for k := range c.circle {
		hashes = append(hashes, k)
	}
	sort.Sort(hashes)
	c.sortedHashes = hashes
}

func sliceContainsMember(set []string, member string) bool {
	for _, m := range set {
		if m == member {
			return true
		}
	}
	return false
}
