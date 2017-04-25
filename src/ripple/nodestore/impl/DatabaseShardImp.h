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

#ifndef RIPPLE_NODESTORE_DATABASESHARDIMP_H_INCLUDED
#define RIPPLE_NODESTORE_DATABASESHARDIMP_H_INCLUDED

#include <ripple/nodestore/DatabaseShard.h>
#include <ripple/nodestore/impl/Shard.h>

namespace ripple {
namespace NodeStore {

class DatabaseShardImp : public DatabaseShard
{
public:
    DatabaseShardImp() = delete;
    DatabaseShardImp(DatabaseShardImp const&) = delete;
    DatabaseShardImp& operator=(DatabaseShardImp const&) = delete;

    DatabaseShardImp(std::string const& name, Stoppable& parent,
        Scheduler& scheduler, int readThreads, Section const& config,
            beast::Journal journal);

    DatabaseShardImp(NodeStore::Database&, Section const& config,
        Scheduler& scheduler, beast::Journal journal);

    ~DatabaseShardImp() override;

    bool
    init() override;

    boost::optional<std::uint32_t>
    prepare(std::uint32_t validLedgerSeq) override;

    std::shared_ptr<Ledger>
    fetchLedger(Application& app, uint256 const& hash,
        std::uint32_t seq) override;

    void
    setStored(std::shared_ptr<Ledger const> const& ledger) override;

    bool
    hasLedger(std::uint32_t seq) override;

    std::string
    getCompleteShards() override;

    void
    validate(Application& app) override;

    std::string
    getName() const override
    {
        return "shardstore";
    }

    void
    for_each(std::function <void(std::shared_ptr<NodeObject>)> f) override
    {
        Throw<std::runtime_error>("Shard store import not supported");
    }

    void
    import(Database& source) override
    {
        Throw<std::runtime_error>("Shard store import not supported");
    }

    std::int32_t
    getWriteLoad() const override;

    void
    store(NodeObjectType type, Blob&& data,
        uint256 const& hash, std::uint32_t seq) override;

    bool
    copyLedger(std::shared_ptr<Ledger const> const& ledger) override;

private:
    mutable std::mutex m_;
    std::map<std::uint32_t, std::unique_ptr<Shard>> complete_;
    std::unique_ptr<Shard> incomplete_;
    Section const config_;
    boost::filesystem::path dir_;
    bool canAdd_ {true};
    std::string status_;

    // Tells if backend type uses permanent storage
    bool backed_;

    // Maximum disk space the DB can use (in bytes)
    std::uint64_t const maxDiskSpace_;

    // Disk space used to store the shards (in bytes)
    std::uint64_t usedDiskSpace_ {0};

    // Average disk space a shard requires (in bytes)
    std::uint64_t avgShardSz_ {ledgersPerShard * (192 * 1024)};

    std::shared_ptr<NodeObject>
    fetchFrom(uint256 const& hash, std::uint32_t seq) override;

    // Finds a random shard index that is not stored
    boost::optional<std::uint32_t>
    findShardIndexToAdd(std::uint32_t validLedgerSeq);

    // Updates complete shards, disk usage, stats
    void
    update(std::shared_ptr<Ledger const> const& ledger);

    // Updates stats
    void
    updateStats();
};

} // NodeStore
} // ripple

#endif
