// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package lease

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

func makeNameCache() nameCache {
	return nameCache{
		historical: make(map[historicalCacheKey][]historicalEntry),
	}
}

// historicalCacheKey identifies a descriptor name at a point in time.
type historicalCacheKey struct {
	parentID       descpb.ID
	parentSchemaID descpb.ID
	name           string
}

// historicalEntry records a descriptor's ID and the time range which
// it held a given time.
type historicalEntry struct {
	id descpb.ID
	// modTime is the modificationtime when the descritpor was given this name
	modTime hlc.Timestamp
	// expiration is the modification time of the version that gave the describtor
	// a different name
	expiration hlc.Timestamp
}

// nameCache is a cache of descriptor's name -> leased descriptor state mapping
// the Manager updates the cache everytime a lease is acquired or released.
// The cache maintains the latest version for each name, plus a historical log
// of old name mapping for descriptors that have been renamed,
// allowing timestamp-aware lookup without hitting the KV store.
// All methods are thread-safe.
type nameCache struct {
	mu          syncutil.RWMutex
	descriptors nstree.NameMap

	historical map[historicalCacheKey][]historicalEntry
}

// Resolves a (qualified) name to the descriptor's ID.
// Returns a valid descriptorVersionState and expiration (hlc.Timestamp)
// for descriptor with that name, if the name had been previously cached
// and the cache has a descriptor version that has not expired.
// Returns nil (and empty timestamp) otherwise.
// This method handles normalizing the descriptor name.
// The descriptor's refcount is incremented before returning, so the caller
// is responsible for releasing it to the leaseManager.
func (c *nameCache) get(
	ctx context.Context,
	parentID descpb.ID,
	parentSchemaID descpb.ID,
	name string,
	timestamp hlc.Timestamp,
) (desc *descriptorVersionState, expiration hlc.Timestamp) {
	c.mu.RLock()
	var ok bool
	desc, ok = c.descriptors.GetByName(
		parentID, parentSchemaID, name,
	).(*descriptorVersionState)
	c.mu.RUnlock()
	if !ok {
		return nil, expiration
	}
	expensiveLogEnabled := log.ExpensiveLogEnabled(ctx, 2)
	desc.mu.Lock()
	if desc.mu.lease == nil {
		desc.mu.Unlock()
		// This get() raced with a release operation. Remove this cache
		// entry if needed.
		c.remove(desc)
		return nil, hlc.Timestamp{}
	}

	defer desc.mu.Unlock()

	if !NameMatchesDescriptor(desc, parentID, parentSchemaID, name) {
		panic(errors.AssertionFailedf("out of sync entry in the name cache. "+
			"Cache entry: (%d, %d, %q) -> %d. Lease: (%d, %d, %q).",
			parentID, parentSchemaID, name,
			desc.GetID(),
			desc.GetParentID(), desc.GetParentSchemaID(), desc.GetName()),
		)
	}

	// Expired descriptor. Don't hand it out.
	if desc.hasExpired(ctx, timestamp) {
		return nil, hlc.Timestamp{}
	}

	desc.incRefCount(ctx, expensiveLogEnabled)
	if exp := desc.expiration.Load(); exp != nil {
		return desc, *exp
	}
	return desc, hlc.Timestamp{}
}

func (c *nameCache) insert(ctx context.Context, desc *descriptorVersionState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	got, ok := c.descriptors.GetByName(
		desc.GetParentID(), desc.GetParentSchemaID(), desc.GetName(),
	).(*descriptorVersionState)
	if ok && desc.getExpiration(ctx).Less(got.getExpiration(ctx)) {
		return
	}

	// If the same descriptor ID was perviously cached under different name,
	// record the historical mapping so that timestamp-aware lookup can find it
	// without a KV round trip.
	if !desc.SkipNamespace() {
		if prev, prevOk := c.descriptors.GetByID(desc.GetID()).(*descriptorVersionState); prevOk &&
			!prev.SkipNamespace() &&
			(prev.GetName() != desc.GetName() ||
				prev.GetParentID() != desc.GetParentID() ||
				prev.GetParentSchemaID() != desc.GetParentSchemaID()) {
			// The descriptor was renamed. Record the old name -> ID mapping.
			key := historicalCacheKey{
				parentID:       prev.GetParentID(),
				parentSchemaID: prev.GetParentSchemaID(),
				name:           prev.GetName(),
			}

			c.historical[key] = append(c.historical[key], historicalEntry{
				id:         prev.GetID(),
				modTime:    prev.GetModificationTime(),
				expiration: desc.GetModificationTime(),
			})

		}
	}

	c.descriptors.Upsert(desc, desc.SkipNamespace())
}

func (c *nameCache) remove(desc *descriptorVersionState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// If this was the lease that the cache had for the descriptor name, remove
	// it. If the cache had some other descriptor, this remove is a no-op.
	if got := c.descriptors.GetByID(desc.GetID()); got == desc {
		c.descriptors.Remove(desc.GetID())
	}
}

// getHistoricalID look up historical name -> ID mapping for the given name
// at the given timestamp. It return descpb.InvalidID if no valid mapping is found.
func (c *nameCache) getHistoricalID(
	parentID descpb.ID, parentSchemaID descpb.ID, name string, timestamp hlc.Timestamp,
) descpb.ID {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := historicalCacheKey{
		parentID:       parentID,
		parentSchemaID: parentSchemaID,
		name:           name,
	}

	for _, entry := range c.historical[key] {
		if entry.modTime.LessEq(timestamp) && timestamp.Less(entry.expiration) {
			return entry.id
		}
	}

	return descpb.InvalidID
}
