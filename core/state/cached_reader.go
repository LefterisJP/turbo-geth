package state

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/turbo/shards"
	"github.com/ledgerwatch/turbo-geth/turbo/trie"
)

// CachedReader is a wrapper for an instance of type StateReader
// This wrapper only makes calls to the underlying reader if the item is not in the cache
type CachedReader struct {
	r     StateReader
	cache *shards.StateCache
}

// NewCachedReader wraps a given state reader into the cached reader
func NewCachedReader(r StateReader, cache *shards.StateCache) *CachedReader {
	return &CachedReader{r: r, cache: cache}
}

const ReadStateByPrefixes = true

// ReadAccountData is called when an account needs to be fetched from the state
func (cr *CachedReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
	addrBytes := address.Bytes()
	if a, ok := cr.cache.GetAccount(addrBytes); ok {
		return a, nil
	}

	if !ReadStateByPrefixes {
		a, err := cr.r.ReadAccountData(address)
		if err != nil {
			return nil, err
		}
		if a == nil {
			cr.cache.SetAccountAbsentPlain(addrBytes)
		} else {
			cr.cache.SetAccountReadPlain(addrBytes, a)
		}
		return a, nil
	}

	var addrHash common.Hash
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	_, _ = h.Sha.Write(addrBytes)
	_, _ = h.Sha.Read(addrHash[:])
	return OnAccountMiss(cr.r.(*PlainStateReader).db, cr.cache, addrHash)
}

func OnAccountMiss(db ethdb.Database, cache *shards.StateCache, addrHash common.Hash) (*accounts.Account, error) {
	var hashedNibbles []byte
	hexutil.DecompressNibbles(addrHash[:], &hashedNibbles)
	ihK, hasState, alreadyLoaded, trieMiss := cache.FindDeepestAccountTrie(hashedNibbles[:])
	if trieMiss {
		if err := db.Walk(dbutils.TrieOfAccountsBucket, ihK, len(ihK)*8, func(k, v []byte) (bool, error) {
			hasState, hasTree, hasHash, newV := trie.UnmarshalTrieNodeTyped(v)
			cache.SetAccountTrieRead(k, hasState, hasTree, hasHash, newV)
			return true, nil
		}); err != nil {
			return nil, err
		}
		ihK, hasState, alreadyLoaded, _ = cache.FindDeepestAccountTrie(hashedNibbles[:])
	}
	if ihK == nil { // when Trie table is empty - can load individual records to cache - not by prefixes
		readAcc := func(addrHash common.Hash) (*accounts.Account, error) {
			enc, err := db.Get(dbutils.HashedAccountsBucket, addrHash.Bytes())
			if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
				return nil, err
			}
			if len(enc) == 0 {
				return nil, nil
			}
			a := &accounts.Account{}
			if err = a.DecodeForStorage(enc); err != nil {
				return nil, err
			}
			return a, nil
		}
		a, err := readAcc(addrHash)
		if err != nil {
			return nil, err
		}
		if a == nil {
			cache.SetAccountAbsent(addrHash)
		} else {
			cache.SetAccountRead(addrHash, a)
		}
		return a, nil
	}
	if !hasState || alreadyLoaded {
		cache.SetAccountAbsent(addrHash)
		return nil, nil
	}
	buf := common.CopyBytes(ihK)
	fixedBits := len(buf) * 4
	if len(buf)%2 == 1 {
		buf = append(buf, 0)
	}
	hexutil.CompressNibbles(buf, &buf)
	found := false
	a := &accounts.Account{}
	if err := db.Walk(dbutils.HashedAccountsBucket, buf, fixedBits, func(k, v []byte) (bool, error) {
		if _, ok := cache.GetAccountByHashedAddress(common.BytesToHash(k)); ok {
			return true, nil
		}
		acc := new(accounts.Account)
		if err := acc.DecodeForStorage(v); err != nil {
			return false, err
		}
		cache.SetAccountRead(common.BytesToHash(k), acc)
		if bytes.Equal(k, addrHash[:]) {
			found = true
			a.Copy(acc)
		}
		return true, nil
	}); err != nil {
		return nil, err
	}
	if !found {
		cache.SetAccountAbsent(addrHash)
	}
	cache.MarkAccountTrieAsLoaded(ihK)
	return a, nil
}

// ReadAccountStorage is called when a storage item needs to be fetched from the state
func (cr *CachedReader) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	addrBytes := address.Bytes()
	locBytes := key.Bytes()
	if s, ok := cr.cache.GetStorage(addrBytes, incarnation, locBytes); ok {
		return s, nil
	}
	if !ReadStateByPrefixes {
		v, err := cr.r.ReadAccountStorage(address, incarnation, key)
		if err != nil {
			return nil, err
		}
		if len(v) == 0 {
			cr.cache.SetStorageAbsentPlain(addrBytes, incarnation, locBytes)
		} else {
			cr.cache.SetStorageReadPlain(addrBytes, incarnation, locBytes, v)
		}
		return v, nil
	}

	var addrHash common.Hash
	var locHash common.Hash
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	_, _ = h.Sha.Write(addrBytes)
	_, _ = h.Sha.Read(addrHash[:])
	h.Sha.Reset()
	_, _ = h.Sha.Write(locBytes)
	_, _ = h.Sha.Read(locHash[:])
	return OnStorageMiss(cr.r.(*PlainStateReader).db, cr.cache, addrHash, incarnation, locHash)
}

func OnStorageMiss(db ethdb.Database, cache *shards.StateCache, addrHash common.Hash, incarnation uint64, locHash common.Hash) ([]byte, error) {
	var hashedNibbles []byte
	hexutil.DecompressNibbles(addrHash[:], &hashedNibbles)
	ihK, hasState, alreadyLoaded, trieMiss := cache.FindDeepestStorageTrie(addrHash, incarnation, hashedNibbles[:])
	if trieMiss {
		if err := db.Walk(dbutils.TrieOfAccountsBucket, ihK, len(ihK)*8, func(k, v []byte) (bool, error) {
			hasState, hasTree, hasHash, newV := trie.UnmarshalTrieNodeTyped(v)
			cache.SetAccountTrieRead(k, hasState, hasTree, hasHash, newV)
			return true, nil
		}); err != nil {
			return nil, err
		}
		ihK, hasState, alreadyLoaded, _ = cache.FindDeepestStorageTrie(addrHash, incarnation, hashedNibbles[:])
	}

	if ihK == nil { // when Trie table is empty - can load individual records to cache - not by prefixes
		readOne := func(addrHash common.Hash, incarnation uint64, locHash common.Hash) ([]byte, error) {
			compositeKey := dbutils.GenerateCompositeStorageKey(addrHash, incarnation, locHash)
			enc, err := db.Get(dbutils.HashedStorageBucket, compositeKey)
			if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
				return nil, err
			}
			if len(enc) == 0 {
				return nil, nil
			}
			return enc, nil
		}
		v, err := readOne(addrHash, incarnation, locHash)
		if err != nil {
			return nil, err
		}
		if len(v) == 0 {
			cache.SetStorageAbsent(addrHash, incarnation, locHash)
		} else {
			cache.SetStorageRead(addrHash, incarnation, locHash, v)
		}
		return v, nil
	}
	if !hasState || alreadyLoaded {
		cache.SetStorageAbsent(addrHash, incarnation, locHash)
		return nil, nil
	}
	buf := make([]byte, 40+len(ihK))
	copy(buf, addrHash.Bytes())
	binary.BigEndian.PutUint64(buf[32:], incarnation)
	lastPart := buf[40:]
	copy(lastPart, ihK)
	fixedBits := 40*8 + len(ihK)*4
	if len(ihK)%2 == 1 {
		lastPart = append(lastPart, 0)
	}
	hexutil.CompressNibbles(lastPart, &lastPart)
	buf = buf[:40+len(ihK)/2+len(ihK)%2]
	found := false
	var v []byte
	if err := db.Walk(dbutils.HashedStorageBucket, buf, fixedBits, func(k, vv []byte) (bool, error) {
		if _, ok := cache.GetStorageByHashedAddress(addrHash, incarnation, common.BytesToHash(k[40:])); ok {
			return true, nil
		}
		cache.DeprecatedSetStorageRead(addrHash, incarnation, common.BytesToHash(k[40:]), vv)
		if bytes.Equal(k, addrHash[:]) {
			found = true
			v = vv
		}
		return true, nil
	}); err != nil {
		return nil, err
	}
	if !found {
		cache.SetStorageAbsent(addrHash, incarnation, locHash)
	}
	cache.MarkStorageTrieAsLoaded(addrHash, incarnation, ihK)
	return v, nil
}

// ReadAccountCode iws called when code of an account needs to be fetched from the state
// Usually, one of (address;incarnation) or codeHash is enough to uniquely identify the code
func (cr *CachedReader) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	if c, ok := cr.cache.GetCode(address.Bytes(), incarnation); ok {
		return c, nil
	}
	c, err := cr.r.ReadAccountCode(address, incarnation, codeHash)
	if err != nil {
		return nil, err
	}
	if cr.cache != nil && len(c) <= 1024 {
		cr.cache.SetCodeRead(address.Bytes(), incarnation, c)
	}
	return c, nil
}

func (cr *CachedReader) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	c, err := cr.ReadAccountCode(address, incarnation, codeHash)
	return len(c), err
}

// ReadAccountIncarnation is called when incarnation of the account is required (to create and recreate contract)
func (cr *CachedReader) ReadAccountIncarnation(address common.Address) (uint64, error) {
	deleted := cr.cache.GetDeletedAccount(address.Bytes())
	if deleted != nil {
		return deleted.Incarnation, nil
	}
	return cr.r.ReadAccountIncarnation(address)
}
