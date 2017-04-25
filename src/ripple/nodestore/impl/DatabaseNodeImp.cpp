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
#include <ripple/nodestore/impl/DatabaseNodeImp.h>
#include <ripple/app/ledger/Ledger.h>
#include <ripple/protocol/HashPrefix.h>

namespace ripple {
namespace NodeStore {

bool
DatabaseNodeImp::copyLedger(
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
    storeBatchInternal(batch, *backend_);
    return true;
}

} // NodeStore
} // ripple
