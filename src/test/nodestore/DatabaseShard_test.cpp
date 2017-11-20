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
#include <ripple/app/ledger/LedgerToJson.h>
#include <ripple/core/ConfigSections.h>
#include <ripple/protocol/HashPrefix.h>
#include <ripple/nodestore/impl/Shard.h>
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
        for (std::uint32_t id = 1; id <= 16; ++id)
        {
            auto s = std::make_shared<Serializer>();
            auto tx = STTx { ttPAYMENT, [id](auto& t) {
                    t.setFieldAmount(sfAmount, 42);
                    t.setAccountID(sfAccount, AccountID(2));
                    t.setAccountID(sfDestination, AccountID(3));
                    t.setFieldU32(sfSequence, id);
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

    void testShardStoreAndFetch(unsigned int lps, bool useCopy)
    {
        using namespace test::jtx;
        testcase("Store and Fetch, sharddb");
        beast::temp_dir dbpath;
        Env env {*this, envconfig ([&](std::unique_ptr<Config> cfg)
            {
                cfg->overwrite
                    (ConfigSection::shardDatabase (), "type", "nudb");
                cfg->overwrite
                    (ConfigSection::shardDatabase (), "path", dbpath.path());
                cfg->overwrite
                    (ConfigSection::shardDatabase (), "max_size_gb", "4");
                cfg->overwrite
                    (ConfigSection::shardDatabase (), "ledgers_per_shard",
                        std::to_string(lps));
                return cfg;
            })};

        auto dbs = env.app().getShardStore();
        if (! BEAST_EXPECT(dbs))
            return;
        auto stat = dbs->prepare(lps * 256);
        if (! BEAST_EXPECT(stat))
            return;

        auto l = createAndStoreLedger(
            *stat,
            env.app().config(),
            env.app().family(),
            env.app().timeKeeper().closeTime());

        if (useCopy)
        {
            if (! BEAST_EXPECT(dbs->copyLedger(l)))
                return;
        }
        else
        {
            {
                // Store the ledger header
                Serializer s(128);
                s.add32(HashPrefix::ledgerMaster);
                addRaw(l->info(), s);
                dbs->store(
                    hotLEDGER,
                    std::move(s.modData()),
                    l->info().hash,
                    l->info().seq);
            }

            auto fstore = [&](SHAMapAbstractNode& node)->bool {
                auto nSrc = env.app().getNodeStore().fetch(
                    node.getNodeHash().as_uint256(),
                    node.getSeq());
                if (! BEAST_EXPECT(nSrc))
                    return false;

                dbs->store(
                    nSrc->getType(),
                    Blob {nSrc->getData()},
                    nSrc->getHash(),
                    l->info().seq);

                return true;
            };

            l->stateMap().snapShot(false)->visitNodes(fstore);
            l->txMap().snapShot(false)->visitNodes(fstore);
            dbs->setStored(l);
        }

        auto fetched = dbs->fetchLedger(l->info().hash, *stat);
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

        // verify the metadata/header info by serializing to json
        BEAST_EXPECT(
            getJson (
                LedgerFill {*l, LedgerFill::full | LedgerFill::expand}) ==
            getJson (
                LedgerFill {*fetched, LedgerFill::full | LedgerFill::expand}));

        BEAST_EXPECT(
            getJson (
                LedgerFill {*l, LedgerFill::full | LedgerFill::binary}) ==
            getJson (
                LedgerFill {*fetched, LedgerFill::full | LedgerFill::binary}));

        // walk shamap and validate each node
        auto fcomp = [&](SHAMapAbstractNode& node)->bool {
            auto nSrc = env.app().getNodeStore().fetch(
                node.getNodeHash().as_uint256(), node.getSeq());
            if (! BEAST_EXPECT(nSrc))
                return false;

            auto nDst = env.app().getShardStore()->fetch(
                node.getNodeHash().as_uint256(), l->info().seq);
            if (! BEAST_EXPECT(nDst))
                return false;

            BEAST_EXPECT(isSame(nSrc, nDst));

            return true;
        };

        l->stateMap().snapShot(false)->visitNodes(fcomp);
        l->txMap().snapShot(false)->visitNodes(fcomp);
    }

public:
    void run ()
    {
        auto lps = 512u;

        testShardStoreAndFetch(lps, true);
        testShardStoreAndFetch(lps, false);

        testNodeStore<DatabaseShard> ("nudb"); //default config used
        testNodeStore<DatabaseShard> ("nudb", lps, lps);
        testNodeStore<DatabaseShard> ("nudb", lps + 1, lps);
        testNodeStore<DatabaseShard> ("nudb", lps * 2 - 1, lps);
        testNodeStore<DatabaseShard> ("nudb", lps * 2, lps);
        testNodeStore<DatabaseShard> ("nudb", lps * 2 + 1, lps);
        testNodeStore<DatabaseShard> ("nudb", lps * 5, lps);
    #if RIPPLE_ROCKSDB_AVAILABLE
        testNodeStore<DatabaseShard> ("rocksdb"); //default config used
        testNodeStore<DatabaseShard> ("rocksdb", lps, lps);
        testNodeStore<DatabaseShard> ("rocksdb", lps + 1, lps);
        testNodeStore<DatabaseShard> ("rocksdb", lps * 2 - 1, lps);
        testNodeStore<DatabaseShard> ("rocksdb", lps * 2, lps);
        testNodeStore<DatabaseShard> ("rocksdb", lps * 2 + 1, lps);
        testNodeStore<DatabaseShard> ("rocksdb", lps * 5, lps);
    #endif
    }
};

BEAST_DEFINE_TESTSUITE(DatabaseShard,shard,ripple);

}
