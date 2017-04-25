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

#include <BeastConfig.h>
#include <ripple/nodestore/impl/DatabaseRotatingImp.h>
#include <ripple/app/ledger/Ledger.h>
#include <ripple/protocol/HashPrefix.h>

namespace ripple {
namespace NodeStore {

DatabaseRotatingImp::DatabaseRotatingImp(
    std::string const& name, Scheduler& scheduler, int readThreads,
        Stoppable& parent, std::shared_ptr <Backend> writableBackend,
            std::shared_ptr <Backend> archiveBackend, beast::Journal journal)
    : DatabaseRotating(name, parent, scheduler, readThreads, journal)
    , writableBackend_(writableBackend)
    , archiveBackend_(archiveBackend)
{
    if (writableBackend)
        fdLimit_ += writableBackend_->fdlimit();
    if (archiveBackend_)
        fdLimit_ += archiveBackend_->fdlimit();
}

// Make sure to call it already locked!
std::shared_ptr<Backend>
DatabaseRotatingImp::rotateBackends(
    std::shared_ptr <Backend> const& newBackend)
{
    std::shared_ptr <Backend> oldBackend = archiveBackend_;
    archiveBackend_ = writableBackend_;
    writableBackend_ = newBackend;

    return oldBackend;
}

bool
DatabaseRotatingImp::copyLedger(
    std::shared_ptr<Ledger const> const& ledger)
{
    if (ledger->info().accountHash.isZero())
    {
        assert(false);
        JLOG(j_.error()) <<
            "ledger has a zero account hash";
        return false;
    }
    auto& srcDB = const_cast<Database&>(
        ledger->stateMap().family().db());
    if (&srcDB == this)
    {
        assert(false);
        JLOG(j_.error()) <<
            "Source and destination are the same";
        return false;
    }
    Batch batch;
    bool error = false;
    auto f = [&](SHAMapAbstractNode& node) {
        if (auto nObj = srcDB.fetch(
            node.getNodeHash().as_uint256(), node.getSeq()))
                batch.emplace_back(std::move(nObj));
        else
            error = true;
        return !error;
    };
    // Batch the ledger header
    {
        Serializer s(128);
        s.add32(HashPrefix::ledgerMaster);
        addRaw(ledger->info(), s);
        batch.emplace_back(NodeObject::createObject(hotLEDGER,
            std::move(s.modData()), ledger->info().hash));
    }
    // Batch the state map
    if (ledger->stateMap().getHash().isNonZero())
    {
        if (! ledger->stateMap().isValid())
        {
            JLOG(j_.error()) <<
                "invalid state map";
            return false;
        }
        ledger->stateMap().snapShot(false)->visitNodes(f);
        if (error)
            return false;
    }
    // Batch the transaction map
    if (ledger->info().txHash.isNonZero())
    {
        if (! ledger->txMap().isValid())
        {
            JLOG(j_.error()) <<
                "invalid transaction map";
            return false;
        }
        ledger->txMap().snapShot(false)->visitNodes(f);
        if (error)
            return false;
    }
    // Store batch
    storeBatchInternal(batch, *getWritableBackend());
    return true;
}

std::shared_ptr<NodeObject>
DatabaseRotatingImp::fetchFrom(uint256 const& hash, std::uint32_t seq)
{
    Backends b = getBackends();
    auto nObj = fetchInternal(hash, *b.writableBackend);
    if (! nObj)
    {
        nObj = fetchInternal(hash, *b.archiveBackend);
        if (nObj)
        {
            getWritableBackend()->store(nObj);
            nCache_.erase(hash);
        }
    }
    return nObj;
}

} // NodeStore
} // ripple
