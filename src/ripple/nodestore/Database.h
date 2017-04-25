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

#ifndef RIPPLE_NODESTORE_DATABASE_H_INCLUDED
#define RIPPLE_NODESTORE_DATABASE_H_INCLUDED

#include <ripple/basics/TaggedCache.h>
#include <ripple/basics/KeyCache.h>
#include <ripple/core/Stoppable.h>
#include <ripple/nodestore/Backend.h>
#include <ripple/nodestore/Scheduler.h>
#include <ripple/nodestore/NodeObject.h>

#include <thread>

namespace ripple {

class Ledger;

namespace NodeStore {

/** Persistency layer for NodeObject

    A Node is a ledger object which is uniquely identified by a key, which is
    the 256-bit hash of the body of the node. The payload is a variable length
    block of serialized data.

    All ledger data is stored as node objects and as such, needs to be persisted
    between launches. Furthermore, since the set of node objects will in
    general be larger than the amount of available memory, purged node objects
    which are later accessed must be retrieved from the node store.

    @see NodeObject
*/
class Database : public Stoppable
{
public:
    Database() = delete;

    /** Construct the node store.

        @param name The Stoppable name for this Database.
        @param parent The parent Stoppable.
        @param scheduler The scheduler to use for performing asynchronous tasks.
        @param readThreads The number of async read threads to create.
        @param journal Destination for logging output.
    */
    Database(std::string name, Stoppable& parent, Scheduler& scheduler,
        int readThreads, beast::Journal journal);

    /** Destroy the node store.
        All pending operations are completed, pending writes flushed,
        and files closed before this returns.
    */
    virtual
    ~Database();

    /** Retrieve the name associated with this backend.
        This is used for diagnostics and may not reflect the actual path
        or paths used by the underlying backend.
    */
    virtual
    std::string
    getName() const = 0;

    /** Visit every object in the database
        This is usually called during import.

        @note This routine will not be called concurrently with itself
                or other methods.
        @see import
    */
    virtual
    void
    for_each(std::function <void(std::shared_ptr<NodeObject>)> f) = 0;

    /** Import objects from another database. */
    virtual
    void
    import(Database& source) = 0;

    /** Retrieve the estimated number of pending write operations.
        This is used for diagnostics.
    */
    virtual
    std::int32_t
    getWriteLoad() const = 0;

    /** Store the object.

        The caller's Blob parameter is overwritten.

        @param type The type of object.
        @param ledgerIndex The ledger in which the object appears.
        @param data The payload of the object. The caller's
                    variable is overwritten.
        @param hash The 256-bit hash of the payload data.

        @return `true` if the object was stored?
    */
    virtual
    void
    store(NodeObjectType type, Blob&& data,
        uint256 const& hash, std::uint32_t seq) = 0;

    /** Fetch an object.
        If the object is known to be not in the database, isn't found in the
        database during the fetch, or failed to load correctly during the fetch,
        `nullptr` is returned.

        @note This can be called concurrently.
        @param hash The key of the object to retrieve.
        @param seq The sequence of the ledger where the object is stored.
        @return The object, or nullptr if it couldn't be retrieved.
    */
    std::shared_ptr<NodeObject>
    fetch(uint256 const& hash, std::uint32_t seq)
    {
        return doFetch(hash, seq, false);
    }

    /** Fetch an object without waiting.
        If I/O is required to determine whether or not the object is present,
        `false` is returned. Otherwise, `true` is returned and `object` is set
        to refer to the object, or `nullptr` if the object is not present.
        If I/O is required, the I/O is scheduled.

        @note This can be called concurrently.
        @param hash The key of the object to retrieve
        @param object The object retrieved
        @return Whether the operation completed
    */
    bool
    asyncFetch(uint256 const& hash, std::uint32_t seq,
        std::shared_ptr<NodeObject>& object);

    /** Copies a ledger stored in a different database to this one.

        @param ledger The ledger to copy.
        @return true if the operation was successful
    */
    virtual
    bool
    copyLedger(std::shared_ptr<Ledger const> const& ledger) = 0;

    /** Wait for all currently pending async reads to complete.
    */
    void
    waitReads();

    /** Get the maximum number of async reads the node store prefers.
        @return The number of async reads preferred.
    */
    int
    getDesiredAsyncReadCount();

    /** Get the positive cache hits to total attempts ratio. */
    float
    getCacheHitRate()
    {
        return pCache_.getHitRate();
    }

    /** Set the maximum number of entries and maximum cache age for both caches.

        @param size Number of cache entries (0 = ignore)
        @param age Maximum cache age in seconds
    */
    void
    tune(int size, int age);

    /** Remove expired entries from the positive and negative caches. */
    void
    sweep()
    {
        pCache_.sweep();
        nCache_.sweep();
    }

    /** Gather statistics pertaining to read and write activities.
        Return the reads and writes, and total read and written bytes.
     */
    std::uint32_t
    getStoreCount() const
    {
        return storeCount_;
    }

    std::uint32_t
    getFetchTotalCount() const
    {
        return fetchTotalCount_;
    }

    std::uint32_t
    getFetchHitCount() const
    {
        return fetchHitCount_;
    }

    std::uint32_t
    getStoreSize() const
    {
        return storeSz_;
    }

    std::uint32_t
    getFetchSize() const
    {
        return fetchSz_;
    }

    /** Return the number of files needed by our backend(s) */
    int
    fdlimit() const
    {
        return fdLimit_;
    }

    void
    onStop();

protected:
    beast::Journal j_;
    Scheduler& scheduler_;
    int fdLimit_{ 0 };
    TaggedCache<uint256, NodeObject> pCache_; // Positive cache
    KeyCache<uint256> nCache_; // Negative cache

    void
    stopThreads();

    void
    storeInternal(NodeObjectType type, Blob&& data,
        uint256 const& hash, Backend& backend);

    void
    storeBatchInternal(Batch& batch, Backend& backend);

    std::shared_ptr<NodeObject>
    fetchInternal(uint256 const& hash, Backend& backend);

    void
    importInternal(Database& source, Backend& dest);

private:
    std::atomic<std::uint32_t> storeCount_ {0};
    std::atomic<std::uint32_t> fetchTotalCount_ {0};
    std::atomic<std::uint32_t> fetchHitCount_ {0};
    std::atomic<std::uint32_t> storeSz_ {0};
    std::atomic<std::uint32_t> fetchSz_ {0};

    std::mutex readLock_;
    std::condition_variable readCondVar_;
    std::condition_variable readGenCondVar_;
    std::map<uint256, std::uint32_t> read_; // reads to do
    std::pair<uint256, std::uint32_t> readLast_; // last read
    std::vector<std::thread> readThreads_;
    bool readShut_ {false};
    uint64_t readGen_ {0}; // current read generation

    virtual
    std::shared_ptr<NodeObject>
    fetchFrom(uint256 const& hash, std::uint32_t seq) = 0;

    std::shared_ptr<NodeObject>
    doFetch(uint256 const& hash, std::uint32_t seq, bool isAsync);

    void
    threadEntry();
};

}
}

#endif
