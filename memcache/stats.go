// Copyright 2018 Google Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package memcache

import (
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
)

const unitDimensionless = "1"

var (
	mAdd         = stats.Int64("add", "Measure add operations", unitDimensionless)
	mAddMulti    = stats.Int64("multi_add", "Measure add-multi operations", unitDimensionless)
	mGet         = stats.Int64("get", "Measure get operations", unitDimensionless)
	mGetMulti    = stats.Int64("multi_get", "Measure get-multi operations", unitDimensionless)
	mSet         = stats.Int64("sets", "Measure set operations", unitDimensionless)
	mSetMulti    = stats.Int64("multi_set", "Measure set-multi operations", unitDimensionless)
	mCas         = stats.Int64("cas", "Measure CAS operations", unitDimensionless)
	mCasMulti    = stats.Int64("multi_cas", "Measure CAS-multi operations", unitDimensionless)
	mDelete      = stats.Int64("delete", "Measure delete operations", unitDimensionless)
	mIncrement   = stats.Int64("incr", "Measure increment operations", unitDimensionless)
	mKeyLength   = stats.Int64("key_len", "The key-length measure", unitDimensionless)
	mValueLength = stats.Int64("value_len", "The value-length measure", unitDimensionless)
	mStats       = stats.Int64("stats", "The stats invocation count measure", unitDimensionless)

	mCasConflictError = stats.Int64("cas_conflict", "Measure CAS conflict errors", unitDimensionless)
	mNotStoredError   = stats.Int64("not_stored", "Measure NotStored errors", unitDimensionless)
	mServerError      = stats.Int64("server_error", "Measure Server errors", unitDimensionless)

	mCacheHit        = stats.Int64("hit", "Measure cache hits", unitDimensionless)
	mCacheMiss       = stats.Int64("miss", "Measure cache misses", unitDimensionless)
	mFlush           = stats.Int64("flush", "Measure flush operations", unitDimensionless)
	mStored          = stats.Int64("stored", "Measure stored items count", unitDimensionless)
	mZeroLengthItems = stats.Int64("zero_len_items", "Measure zero length items", unitDimensionless)
	mZeroLengthKey   = stats.Int64("zero_len_key", "Measure zero length keys", unitDimensionless)
)

var AllViews = []*view.View{
	{
		Name:        "memcache/add",
		Description: "Tracks the number of invocations of Add",
		Measure:     mAdd,
		Aggregation: view.Count(),
	},
	{
		Name:        "memcache/multi_add",
		Description: "Tracks the number of invocations of AddMulti",
		Measure:     mAdd,
		Aggregation: view.Count(),
	},
	{
		Name:        "memcache/get",
		Description: "Tracks the number of invocations of Get",
		Measure:     mGet,
		Aggregation: view.Count(),
	},
	{
		Name:        "memcache/multi_get",
		Description: "Tracks the number of invocations of GetMulti",
		Measure:     mGetMulti,
		Aggregation: view.Count(),
	},
	{
		Name:        "memcache/sets",
		Description: "Tracks the number of invocations of Set",
		Measure:     mSet,
		Aggregation: view.Count(),
	},
	{
		Name:        "memcache/set_multi",
		Description: "Tracks the number of invocations of SetMulti",
		Measure:     mSetMulti,
		Aggregation: view.Count(),
	},
	{
		Name:        "memcache/cas",
		Description: "Tracks the number of invocations of Cas",
		Measure:     mCas,
		Aggregation: view.Count(),
	},
	{
		Name:        "memcache/multi_cas",
		Description: "Tracks the number of invocations of CasMulti",
		Measure:     mCasMulti,
		Aggregation: view.Count(),
	},
	{
		Name:        "memcache/delete",
		Description: "Tracks the number of invocations of Delete",
		Measure:     mDelete,
		Aggregation: view.Count(),
	},
	{
		Name:        "memcache/increment",
		Description: "Tracks the number of invocations of Increment",
		Measure:     mIncrement,
		Aggregation: view.Count(),
	},
	{
		Name:        "memcache/stats",
		Description: "Tracks the number of Stats invocations",
		Measure:     mStats,
		Aggregation: view.Count(),
	},

	{
		Name:        "memcache/key_length",
		Description: "Key-Length view",
		Measure:     mKeyLength,
		Aggregation: view.Distribution(
			// Valid memcache key lengths are in the range 0-255
			0, 10, 25, 40, 50, 75, 85, 100, 125, 150, 175, 200, 225, 250, 300,
		),
	},
	{
		Name:        "memcache/value_length",
		Description: "Tracks the distribution of value lengths",
		Measure:     mValueLength,
		Aggregation: view.Distribution(
			0, 10, 25, 50, 100, 250, 500, 625, 800,
			1<<10, 10<<10, 100<<10, 200<<10, 500<<10,
			1<<20, 10<<20, 100<<20, 200<<20, 500<<20,
			1<<30, 10<<30, 100<<30, 200<<30, 500<<30,
		),
	},
	{
		Name:        "memcache/cache_hits",
		Description: "Cache-hits view",
		Measure:     mCacheHit,
		Aggregation: view.Count(),
	},
	{
		Name:        "memcache/cache_misses",
		Description: "Cache-misses view",
		Measure:     mCacheMiss,
		Aggregation: view.Count(),
	},
	{
		Name:        "memcache/zero_len_keys",
		Description: "Zero-length key count view",
		Measure:     mZeroLengthKey,
		Aggregation: view.Count(),
	},
	{
		Name:        "memcache/zero_len_items",
		Description: "Zero-length item count view",
		Measure:     mZeroLengthItems,
		Aggregation: view.Count(),
	},
	{
		Name:        "memcache/stored",
		Description: "Stored items count view",
		Measure:     mStored,
		Aggregation: view.Count(),
	},
	{
		Name:        "memcache/not_stored",
		Description: "Not-Stored error count view",
		Measure:     mNotStoredError,
		Aggregation: view.Count(),
	},
{
		Name:        "memcache/server_errors",
		Description: "Tracks the number of errors encountered on the Memcached AppEngine server",
		Measure:     mServerError,
		Aggregation: view.Count(),
	},
	{
		Name:        "memcache/cas_conflict_error",
		Description: "Cas conflict error count view",
		Measure:     mCasConflictError,
		Aggregation: view.Count(),
	},

	{
		Name:        "memcache/flushed",
		Description: "Flush invocation count view",
		Measure:     mFlush,
		Aggregation: view.Count(),
	},
}
