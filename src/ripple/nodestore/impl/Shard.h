//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2012, 2013 Ripple Labs Inc.

    Permission to use, copy, modify, and/or distribute this software for any
    purpose  with  or without fee is hereby granted, provided that the above
    copyright notice and this permission notice appear in all copies.

    THE  SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
    WITH  REGARD  TO  THIS  SOFTWARE  INCLUDING  ALL  IMPLIED  WARRANTIES  OF
    MERCHANTABILITY  AND  FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
    ANY  SPECIAL ,  DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
    WHATSOEVER  RESULTING  FROM  LOSS  OF USE, DATA OR PROFITS, WHETHER IN AN
    ACTION  OF  CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
    OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/
//==============================================================================

#ifndef RIPPLE_NODESTORE_SHARD_H_INCLUDED
#define RIPPLE_NODESTORE_SHARD_H_INCLUDED

#include <ripple/app/ledger/Ledger.h>
#include <ripple/basics/BasicConfig.h>
#include <ripple/basics/RangeSet.h>
#include <ripple/nodestore/DatabaseShard.h>
#include <ripple/nodestore/NodeObject.h>
#include <ripple/nodestore/Scheduler.h>

#include <boost/filesystem.hpp>
#include <boost/serialization/map.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>

namespace ripple {
namespace NodeStore {
namespace detail {

// Return the first ledger sequence of the shard index
constexpr
std::uint32_t
firstSeq(std::uint32_t const shardIndex)
{
    return 1 + (shardIndex * ledgersPerShard);
}

// Return the last ledger sequence of the shard index
constexpr
std::uint32_t
lastSeq(std::uint32_t const shardIndex)
{
    return (shardIndex + 1) * ledgersPerShard;
}

static constexpr auto genesisShardIndex = seqToShardIndex(genesisSeq);
static constexpr auto genesisNumLedgers = ledgersPerShard -
    (genesisSeq - firstSeq(genesisShardIndex));

// Average disk space a shard requires (in bytes)
constexpr std::uint64_t avgShardSize_ = ledgersPerShard * (1024ull * 256);

} // detail

/* A range of historical ledgers backed by a nodestore.
   Shards are indexed and store `ledgersPerShard`.
   Shard `i` stores ledgers starting with sequence: `1 + (i * ledgersPerShard)`
   and ending with sequence: `(i + 1) * ledgersPerShard`.
   Once a shard has all its ledgers, it is marked as read only
   and is never written to again.
*/
class Shard
{
public:
    explicit
    Shard(std::uint32_t shardIndex);

    bool
    open(Section config, Scheduler& scheduler,
        boost::filesystem::path dir, beast::Journal& j);

    bool
    setStored(std::shared_ptr<Ledger const> const& l,
        beast::Journal& j);

    boost::optional<std::uint32_t>
    prepare();

    bool
    hasLedger(std::uint32_t seq) const;

    void
    validate(Application& app, beast::Journal& j);

    std::uint32_t
    index() const { return index_; }

    bool
    complete() const { return complete_; }

    std::uint64_t
    fileSize() const { return fileSize_; }

    Backend&
    getBackend() const
    {
        assert(backend_);
        return *backend_;
    }

    std::uint32_t
    fdlimit() const
    {
        assert(backend_);
        return backend_->fdlimit();
    }

    std::shared_ptr<Ledger const>
    lastStored() { return lastStored_; }

    std::uint32_t
    numComplete() const { return boost::icl::length(storedSeqs_); }

private:
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive & ar, const unsigned int version)
    {
        ar & storedSeqs_;
    }

    static constexpr auto controlFileName = "control.txt";

    // Shard Index
    std::uint32_t const index_;

    // First ledger sequence in this shard
    std::uint32_t const firstSeq_;

    // Last ledger sequence in this shard
    std::uint32_t const lastSeq_;

    std::uint64_t fileSize_ {0};
    std::unique_ptr<Backend> backend_;

    // Path to database files
    boost::filesystem::path dir_;

    // True if shard has its entire ledger range stored
    bool complete_{false};

    // Sequences of ledgers stored with an incomplete shard
    RangeSet<std::uint32_t> storedSeqs_;

    // Path to control file
    boost::filesystem::path control_;

    // Used as an optimization for visitDifferences
    std::shared_ptr<Ledger const> lastStored_;

    void
    updateFileSize();

    bool
    saveControl();

    bool
    valLedger(std::shared_ptr<Ledger const> const& l,
        std::shared_ptr<Ledger const> const& next, beast::Journal& j);
};

} // NodeStore
} // ripple

#endif
