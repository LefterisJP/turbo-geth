package state

import (
	"encoding/binary"

	"github.com/google/btree"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/turbo/shards"
)

func CommitCache(tx ethdb.DbWithPendingMutations, writes [5]*btree.BTree) error {
	return shards.WalkWrites(writes,
		func(address []byte, addrHash common.Hash, account *accounts.Account) error { // accountWrite
			//fmt.Printf("account write %x: balance %d, nonce %d\n", address, account.Balance.ToBig(), account.Nonce)
			value := make([]byte, account.EncodingLengthForStorage())
			account.EncodeForStorage(value)
			if err := tx.Put(dbutils.PlainStateBucket, address, value); err != nil {
				return err
			}
			if ReadStateByPrefixes {
				if err := tx.Put(dbutils.HashedAccountsBucket, addrHash.Bytes(), value); err != nil {
					return err
				}
			}
			return nil
		},
		func(address []byte, addrHash common.Hash, original *accounts.Account) error { // accountDelete
			//fmt.Printf("account delete %x\n", address)
			if err := tx.Delete(dbutils.PlainStateBucket, address[:], nil); err != nil {
				return err
			}
			if ReadStateByPrefixes {
				if err := tx.Put(dbutils.HashedStorageBucket, addrHash.Bytes(), nil); err != nil {
					return err
				}
			}
			if original != nil && original.Incarnation > 0 {
				var b [8]byte
				binary.BigEndian.PutUint64(b[:], original.Incarnation)
				if err := tx.Put(dbutils.IncarnationMapBucket, address, b[:]); err != nil {
					return err
				}
			}
			return nil
		},
		func(address []byte, incarnation uint64, location []byte, value []byte) error { // storageWrite
			//fmt.Printf("storage write %x %d %x => %x\n", address, incarnation, location, value)
			compositeKey := dbutils.PlainGenerateCompositeStorageKey(address, incarnation, location)
			return tx.Put(dbutils.PlainStateBucket, compositeKey, value)
		},
		func(address []byte, incarnation uint64, location []byte) error { // storageDelete
			//fmt.Printf("storage delete %x %d %x\n", address, incarnation, location)
			compositeKey := dbutils.PlainGenerateCompositeStorageKey(address, incarnation, location)
			return tx.Delete(dbutils.PlainStateBucket, compositeKey, nil)
		},
		func(address []byte, incarnation uint64, code []byte) error { // codeWrite
			//fmt.Printf("code write %x %d\n", address, incarnation)
			h := common.NewHasher()
			h.Sha.Reset()
			//nolint:errcheck
			h.Sha.Write(code)
			var codeHash common.Hash
			//nolint:errcheck
			h.Sha.Read(codeHash[:])
			if err := tx.Put(dbutils.CodeBucket, codeHash.Bytes(), code); err != nil {
				return err
			}
			return tx.Put(dbutils.PlainContractCodeBucket, dbutils.PlainGenerateStoragePrefix(address, incarnation), codeHash.Bytes())
		},
		func(address []byte, incarnation uint64) error { // codeDelete
			return nil
		},
	)
}
