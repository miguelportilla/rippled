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

#ifndef RIPPLE_NODESTORE_DATABASESHARD_H_INCLUDED
#define RIPPLE_NODESTORE_DATABASESHARD_H_INCLUDED

#include <ripple/nodestore/Database.h>
#include <ripple/app/ledger/Ledger.h>
#include <ripple/nodestore/Types.h>

#include <boost/optional.hpp>

#include <memory>

namespace ripple {
namespace NodeStore {

/** Finds a shard index containing the ledger sequence

    @param seq The ledger sequence
    @return The shard index
*/
constexpr
std::uint32_t
seqToShardIndex(std::uint32_t const seq)
{
    return (seq - 1) / ledgersPerShard;
}

/** A collection of historical shards
*/
class DatabaseShard : public Database
{
public:
    /** Construct a shard store

        @param name The Stoppable name for this Database
        @param parent The parent Stoppable
        @param scheduler The scheduler to use for performing asynchronous tasks
        @param readThreads The number of async read threads to create
        @param journal Destination for logging output
    */
    DatabaseShard(std::string const& name, Stoppable& parent,
        Scheduler& scheduler, int readThreads, beast::Journal journal)
        : Database(name, parent, scheduler, readThreads, journal)
    {}

    /** Initialize the database

        @return `true` if the database initialized without error
    */
    virtual
    bool
    init() = 0;

    /** Prepare to store a new ledger in the shard

        @param validLedgerSeq the index of the maximum valid ledgers
        @return if a ledger should be fetched and stored, then returns the ledger
                index of the ledger to request. Otherwise returns boost::none.
                Some reasons this may return boost::none are: this database does
                not store shards, all shards are are stored and full, max allowed
                disk space would be exceeded, or a ledger was recently requested
                and not enough time has passed between requests.
        @implNote adds a new writable shard if necessary
    */
    virtual
    boost::optional<std::uint32_t>
    prepare(std::uint32_t validLedgerSeq) = 0;

    /** Fetch a ledger from the shard store

        @param hash The key of the ledger to retrieve
        @param seq The sequence of the ledger
        @return The ledger if found, nullptr otherwise
    */
    virtual
    std::shared_ptr<Ledger>
    fetchLedger(uint256 const& hash, std::uint32_t seq) = 0;

    /** Notifies the database that the given ledger has been
        fully acquired and stored.

        @param ledger The stored ledger to be marked as complete
    */
    virtual
    void
    setStored(std::shared_ptr<Ledger const> const& ledger) = 0;

    /** Query if a ledger with the given sequence is stored

        @param seq The ledger sequence to check if stored
        @return `true` if the ledger is stored
    */
    virtual
    bool
    hasLedger(std::uint32_t seq) = 0;

    /** Query which complete shards are stored

        @return the indexes of complete shards
    */
    virtual
    std::string
    getCompleteShards() = 0;

    /** Verifies shard store data is valid.

        @param app The application object
    */
    virtual
    void
    validate() = 0;
};

}
}

#endif
