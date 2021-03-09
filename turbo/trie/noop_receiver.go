package trie

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
)

type NoopReceiver struct{}

func NewNoopReceiver() *NoopReceiver {
	return &NoopReceiver{}
}

func (*NoopReceiver) Receive(
	itemType StreamItem,
	accountKey []byte,
	storageKey []byte,
	accountValue *accounts.Account,
	storageValue []byte,
	hash []byte,
	hasTree bool,
	cutoff int,
) error {
	return nil
}
func (*NoopReceiver) Root() common.Hash { return EmptyRoot }
func (*NoopReceiver) Result() SubTries  { return SubTries{} }
