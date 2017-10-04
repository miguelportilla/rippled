//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2017 Ripple Labs Inc.

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
#include <test/nodestore/TestBase.h>
#include <ripple/core/ConfigSections.h>
#include <ripple/protocol/HashPrefix.h>
#include <algorithm>

namespace ripple {

using namespace NodeStore;

class DatabaseShard_test : public TestBase
{

    // Create SLE with key and payload
    static
    std::shared_ptr<SLE>
    sle (std::uint64_t id,
         std::uint32_t seq = 1)
    {
        auto const le =
            std::make_shared<SLE>(Keylet{ltACCOUNT_ROOT, uint256(id)});
        le->setFieldU32(sfSequence, seq);
        return le;
    }

    std::shared_ptr<Ledger>
    createAndStoreLedger(
        std::uint32_t seq,
        Config const& config,
        Family& family,
        NetClock::time_point close = NetClock::time_point{})
    {
        auto l = std::make_shared<Ledger>(seq, close, config, family);
        l->stateMap().setLedgerSeq(seq);
        l->txMap().setLedgerSeq(seq);

        // Add data to the state map
        for (std::uint64_t id = 1; id <= 16; ++id)
        {
            auto sle = std::make_shared<SLE>(ltACCOUNT_ROOT, uint256(id));
            sle->setFieldU32(sfSequence, 1);
            l->rawInsert(sle);
        }

        // Add data to the tx map
        for (std::uint64_t id = 1; id <= 16; ++id)
        {
            auto s = std::make_shared<Serializer>();
            auto tx = STTx { ttPAYMENT, [](auto& t) {
                    t.setFieldAmount(sfAmount, 42);
                    t.setAccountID(sfAccount, AccountID(2));
                    t.setAccountID(sfDestination, AccountID(3));
                }};
            tx.add(*s);
            l->rawTxInsert(
                uint256(id),
                std::move(s),
                std::make_shared<Serializer>());
        }

        // Store header
        {
            Serializer s(128);
            s.add32(HashPrefix::ledgerMaster);
            addRaw(l->info(), s);
            family.db().store(hotLEDGER, std::move(s.modData()),
                l->info().hash, l->info().seq);
        }

        // Store SHAMaps
        l->stateMap().flushDirty(hotACCOUNT_NODE, seq);
        l->txMap().flushDirty(hotTRANSACTION_NODE, seq);

        // Update ledger, state map and tx map hashes
        l->setImmutable(config);

        return l;
    }

    void shardStoreAndFetch()
    {
        using namespace test::jtx;
        beast::temp_dir dbpath;
        Env env {*this, envconfig ([&dbpath](std::unique_ptr<Config> cfg)
            {
                cfg->overwrite
                    (ConfigSection::shardDatabase (), "type", "nudb");
                cfg->overwrite
                    (ConfigSection::shardDatabase (), "path", dbpath.path());
                cfg->overwrite
                    (ConfigSection::shardDatabase (), "max_size_gb", "4");
                return cfg;
            })};

        auto dbs = env.app().getShardStore();
        if (! BEAST_EXPECT(dbs))
            return;
        auto stat = dbs->prepare(ledgersPerShard * 256);
        if (! BEAST_EXPECT(stat))
            return;

        auto l = createAndStoreLedger(
            *stat,
            env.app().config(),
            env.app().family(),
            env.app().timeKeeper().closeTime());

        if (! BEAST_EXPECT(dbs->copyLedger(l)))
            return;

        auto fetched = dbs->fetchLedger(env.app(), l->info().hash, *stat);
        if(! BEAST_EXPECT(fetched))
            return;

        auto sleCount {0};
        for (auto const& s : fetched->sles)
        {
            BEAST_EXPECT(s->getType() == ltACCOUNT_ROOT);
            BEAST_EXPECT(s->getFieldU32(sfSequence) == 1);
            ++sleCount;
        }
        BEAST_EXPECT(sleCount == 16);

        auto txCount {0};
        for (auto const& t :fetched->txs)
        {
            BEAST_EXPECT(t.first->getTxnType() == ttPAYMENT);
            BEAST_EXPECT(t.first->getFieldAmount(sfAmount) == 42);
            ++txCount;
        }
        BEAST_EXPECT(txCount == 16);
    }

public:
    void run ()
    {
        shardStoreAndFetch();
        return;  // TODO remove

        testNodeStore<DatabaseShard> ("nudb");
        testNodeStore<DatabaseShard> ("nudb", ledgersPerShard);
        testNodeStore<DatabaseShard> ("nudb", ledgersPerShard + 1);
        testNodeStore<DatabaseShard> ("nudb", ledgersPerShard * 2 - 1);
        testNodeStore<DatabaseShard> ("nudb", ledgersPerShard * 2);
    #if RIPPLE_ROCKSDB_AVAILABLE
        testNodeStore<DatabaseShard> ("rocksdb");
        testNodeStore<DatabaseShard> ("rocksdb", ledgersPerShard);
        testNodeStore<DatabaseShard> ("rocksdb", ledgersPerShard + 1);
        testNodeStore<DatabaseShard> ("rocksdb", ledgersPerShard * 2 - 1);
        testNodeStore<DatabaseShard> ("rocksdb", ledgersPerShard * 2);
    #endif
    }
};

BEAST_DEFINE_TESTSUITE(DatabaseShard,shard,ripple);

}
