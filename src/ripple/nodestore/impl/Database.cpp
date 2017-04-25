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

#include <ripple/nodestore/Database.h>
#include <ripple/nodestore/impl/Tuning.h>
#include <ripple/basics/chrono.h>
#include <ripple/beast/core/CurrentThreadName.h>
#include <ripple/protocol/HashPrefix.h>

namespace ripple {
namespace NodeStore {

Database::Database(std::string name, Stoppable& parent,
    Scheduler& scheduler, int readThreads, beast::Journal journal)
    : Stoppable(name, parent)
    , j_(journal)
    , scheduler_(scheduler)
    , pCache_(name, cacheTargetSize, cacheTargetSeconds, stopwatch(), journal)
    , nCache_(name, stopwatch(), cacheTargetSize, cacheTargetSeconds)
{
    while (readThreads-- > 0)
        readThreads_.emplace_back(&Database::threadEntry, this);
}

Database::~Database()
{
    // NOTE!
    // Any derived class should call the stopThreads() method in its
    // destructor.  Otherwise, occasionally, the derived class may
    // crash during shutdown when its members are accessed by one of
    // these threads after the derived class is destroyed but before
    // this base class is destroyed.
    stopThreads();
}

bool
Database::asyncFetch(uint256 const& hash, std::uint32_t seq,
    std::shared_ptr<NodeObject>& object)
{
    // See if the object is in cache
    object = pCache_.fetch(hash);
    if (object || nCache_.touch_if_exists(hash))
        return true;
    {
        // Otherwise post a read
        std::lock_guard <std::mutex> l(readLock_);
        if (read_.emplace(hash, seq).second)
            readCondVar_.notify_one();
    }
    return false;
}

void
Database::waitReads()
{
    std::unique_lock<std::mutex> l(readLock_);

    // Wake in two generations
    std::uint64_t const wakeGen = readGen_ + 2;
    while (! readShut_ && ! read_.empty() && (readGen_ < wakeGen))
        readGenCondVar_.wait(l);
}

int
Database::getDesiredAsyncReadCount()
{
    // We prefer a client not fill our cache
    // We don't want to push data out of the cache
    // before it's retrieved
    return pCache_.getTargetSize() / asyncDivider;
}

void
Database::tune(int size, int age)
{
    pCache_.setTargetSize(size);
    pCache_.setTargetAge(age);
    nCache_.setTargetSize(size);
    nCache_.setTargetAge(age);
}

void
Database::onStop()
{
    // After stop time we can no longer use the JobQueue for background
    // reads.  Join the background read threads.
    Database::stopThreads();
    stopped();
}

void
Database::stopThreads()
{
    {
        std::lock_guard <std::mutex> l(readLock_);
        if (readShut_) // Only stop threads once.
            return;

        readShut_ = true;
        readCondVar_.notify_all();
        readGenCondVar_.notify_all();
    }

    for (auto& e : readThreads_)
        e.join();
}

void
Database::storeInternal(NodeObjectType type, Blob&& data,
    uint256 const& hash, Backend& backend)
{
    #if RIPPLE_VERIFY_NODEOBJECT_KEYS
    assert(hash == sha512Hash(makeSlice(data)));
    #endif

    auto nObj = NodeObject::createObject(type, std::move(data), hash);
    pCache_.canonicalize(hash, nObj, true);

    backend.store(nObj);
    ++storeCount_;
    storeSz_ += nObj->getData().size();
    nCache_.erase(hash);
}

void
Database::storeBatchInternal(Batch& batch, Backend& backend)
{
    for (auto& nObj : batch)
    {
        #if RIPPLE_VERIFY_NODEOBJECT_KEYS
        assert(nObj->getHash() == sha512Hash(makeSlice(nObj->getData())));
        #endif

        pCache_.canonicalize(nObj->getHash(), nObj, true);
        ++storeCount_;
        storeSz_ += nObj->getData().size();
        nCache_.erase(nObj->getHash());
    }
    backend.storeBatch(batch);
}

std::shared_ptr<NodeObject>
Database::fetchInternal(uint256 const& hash, Backend& backend)
{
    std::shared_ptr<NodeObject> nObj;
    Status status;
    try
    {
        status = backend.fetch(hash.begin(), &nObj);
    }
    catch (std::exception const& e)
    {
        JLOG(j_.error()) << "Exception, " << e.what();
        Rethrow();
    }

    switch(status)
    {
    case ok:
        ++fetchHitCount_;
        if (nObj)
            fetchSz_ += nObj->getData().size();
    case notFound:
        break;

    case dataCorrupt:
        // VFALCO TODO Deal with encountering corrupt data!
        JLOG(j_.fatal()) <<
            "Corrupt NodeObject #" << hash;
        break;

    default:
        JLOG(j_.warn()) <<
            "Unknown status=" << status;
        break;
    }
    return nObj;
}

void
Database::importInternal(Database& source, Backend& dest)
{
    Batch b;
    b.reserve(batchWritePreallocationSize);
    source.for_each(
        [&](std::shared_ptr<NodeObject> nObj)
        {
            assert(nObj);
            if (! nObj) // This should never happen
                return;

            ++storeCount_;
            storeSz_ += nObj->getData().size();

            b.push_back(nObj);
            if (b.size() >= batchWritePreallocationSize)
            {
                dest.storeBatch(b);
                b.clear();
                b.reserve(batchWritePreallocationSize);
            }
        });
    if (! b.empty())
        dest.storeBatch(b);
}

// Perform a fetch and report the time it took
std::shared_ptr<NodeObject>
Database::doFetch(uint256 const& hash, std::uint32_t seq, bool isAsync)
{
    FetchReport report;
    report.isAsync = isAsync;
    report.wentToDisk = false;

    using namespace std::chrono;
    auto const before = steady_clock::now();

    // See if the object already exists in the cache
    auto nObj = pCache_.fetch(hash);
    if (! nObj && ! nCache_.touch_if_exists(hash))
    {
        // Try the database(s)
        report.wentToDisk = true;
        nObj = fetchFrom(hash, seq);
        ++fetchTotalCount_;
        if (! nObj)
        {
            // Just in case a write occurred
            nObj = pCache_.fetch(hash);
            if (! nObj)
                // We give up
                nCache_.insert(hash);
        }
        else
        {
            // Ensure all threads get the same object
            pCache_.canonicalize(hash, nObj);

            // Since this was a 'hard' fetch, we will log it.
            JLOG(j_.trace()) <<
                "HOS: " << hash << " fetch: in db";
        }
    }
    report.wasFound = static_cast<bool>(nObj);
    report.elapsed = duration_cast<milliseconds>(
        steady_clock::now() - before);
    scheduler_.onFetch(report);
    return nObj;
}

// Entry point for async read threads
void
Database::threadEntry()
{
    beast::setCurrentThreadName("prefetch");
    while (true)
    {
        std::pair<uint256, std::uint32_t> last;
        {
            std::unique_lock<std::mutex> l(readLock_);
            while (! readShut_ && read_.empty())
            {
                // All work is done
                readGenCondVar_.notify_all();
                readCondVar_.wait(l);
            }
            if (readShut_)
                break;

            // Read in key order to make the back end more efficient
            auto it = read_.lower_bound(readLast_.first);
            if (it == read_.end())
            {
                it = read_.begin();

                // A generation has completed
                ++readGen_;
                readGenCondVar_.notify_all();
            }

            last = *it;
            read_.erase(it);
            readLast_ = last;
        }

        // Perform the read
        doFetch(last.first, last.second, true);
    }
}

} // NodeStore
} // ripple
