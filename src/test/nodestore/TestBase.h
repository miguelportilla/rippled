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

#ifndef RIPPLE_NODESTORE_BASE_H_INCLUDED
#define RIPPLE_NODESTORE_BASE_H_INCLUDED

#include <ripple/nodestore/Database.h>
#include <ripple/basics/random.h>
#include <ripple/basics/StringUtilities.h>
#include <ripple/beast/type_name.h>
#include <ripple/beast/unit_test.h>
#include <ripple/beast/utility/rngfill.h>
#include <ripple/beast/utility/temp_dir.h>
#include <ripple/beast/xor_shift_engine.h>
#include <ripple/core/ConfigSections.h>
#include <ripple/nodestore/Backend.h>
#include <ripple/nodestore/DummyScheduler.h>
#include <ripple/nodestore/Manager.h>
#include <ripple/nodestore/Types.h>
#include <ripple/nodestore/DatabaseShard.h>
#include <ripple/nodestore/impl/DatabaseShardImp.h>
#include <ripple/nodestore/impl/Shard.h>
#include <boost/algorithm/string.hpp>
#include <iomanip>
#include <algorithm>
#include <typeinfo>
#include <test/jtx.h>

namespace ripple {
namespace NodeStore {

using SeqItem = std::pair <std::uint32_t, std::shared_ptr<NodeObject>>;
using SeqBatch = std::vector <SeqItem>;

/** Binary function that satisfies the strict-weak-ordering requirement.

    This compares the hashes of both objects and returns true if
    the first hash is considered to go before the second.

    @see std::sort
*/
struct LessThan
{
    bool
    operator()(SeqItem const& lhs, SeqItem const& rhs) const noexcept
    {
        return lhs.second->getHash () < rhs.second->getHash ();
    }
};

/** Returns `true` if objects are identical. */
inline
bool isSame (
    std::shared_ptr<NodeObject> const& lhs,
    std::shared_ptr<NodeObject> const& rhs)
{
    return
        (lhs->getType() == rhs->getType()) &&
        (lhs->getHash() == rhs->getHash()) &&
        (lhs->getData() == rhs->getData());
}

/** Returns `true` if SeqItems are identical. */
inline
bool isSame (
    SeqItem const& lhs,
    SeqItem const& rhs)
{
    return
        lhs.first == rhs.first &&
        isSame(lhs.second, rhs.second);
}

// Some common code for the unit tests
//
class TestBase : public beast::unit_test::suite
{
public:
    // Tunable parameters
    //
    static std::size_t const minPayloadBytes = 1;
    static std::size_t const maxPayloadBytes = 2000;
    static int const numObjectsToTest = 2000;

public:
    // Create a predictable batch of objects
    static
    SeqBatch createPredictableBatch(
        int numObjects, std::uint64_t seed)
    {
        SeqBatch batch;
        batch.reserve (numObjects);

        beast::xor_shift_engine rng {seed};

        for (int i = 0; i < numObjects; ++i)
        {
            NodeObjectType type;

            switch (rand_int(rng, 3))
            {
            case 0: type = hotLEDGER; break;
            case 1: type = hotACCOUNT_NODE; break;
            case 2: type = hotTRANSACTION_NODE; break;
            case 3: type = hotUNKNOWN; break;
            }

            uint256 hash;
            beast::rngfill (hash.begin(), hash.size(), rng);

            Blob blob (
                rand_int(rng,
                    minPayloadBytes, maxPayloadBytes));
            beast::rngfill (blob.data(), blob.size(), rng);

            batch.push_back (
                std::make_pair(
                    0u,
                    NodeObject::createObject(
                        type, std::move(blob), hash)));
        }

        return batch;
    }

    // Compare two batches for equality
    static bool areBatchesEqual (SeqBatch const& lhs, SeqBatch const& rhs)
    {
        bool result = true;

        if (lhs.size () == rhs.size ())
        {
            for (int i = 0; i < lhs.size (); ++i)
            {
                if (! isSame(lhs[i], rhs[i]))
                {
                    result = false;
                    break;
                }
            }
        }
        else
        {
            result = false;
        }

        return result;
    }

    // Get a copy of a batch in a backend
    void fetchCopyOfBatch (
        Backend& backend, SeqBatch* pCopy, SeqBatch const& batch)
    {
        pCopy->clear ();
        pCopy->reserve (batch.size ());

        for (auto const& b : batch)
        {
            std::shared_ptr<NodeObject> object;

            Status const status = backend.fetch (
                b.second->getHash ().cbegin (), &object);

            BEAST_EXPECT(status == ok);

            if (status == ok)
            {
                BEAST_EXPECT(object != nullptr);

                pCopy->push_back (std::make_pair (b.first, object));
            }
        }
    }

    void fetchMissing(Backend& backend, SeqBatch const& batch)
    {
        for (auto const& b : batch)
        {
            std::shared_ptr<NodeObject> object;

            Status const status = backend.fetch (
                b.second->getHash ().cbegin (), &object);

            BEAST_EXPECT(status == notFound);
        }
    }

    void storeBatch(Backend& backend, SeqBatch& batch)
    {
        for (auto& b : batch)
        {
            backend.store (b.second);
        }
    }

    // Store all objects in a batch
    void storeBatch (Database& db, SeqBatch& batch)
    {
        for (auto& b : batch)
        {
            std::shared_ptr<NodeObject> const object (b.second);

            Blob data (object->getData ());

            db.store (object->getType (),
                      std::move (data),
                      object->getHash (),
                      b.first);
        }
    }

    void storeBatch (DatabaseShard&, SeqBatch&)
    {
        assert(false);
    }

    void storeBatch (
        Backend&,
        SeqBatch&,
        beast::unit_test::suite&,
        ripple::test::jtx::Env*)
    {
        assert(false);
    }

    void storeBatch (
        Database&,
        SeqBatch&,
        beast::unit_test::suite&,
        ripple::test::jtx::Env*)
    {
        assert(false);
    }

    void storeBatch (
        DatabaseShard& db,
        SeqBatch& batch,
        beast::unit_test::suite& tc,
        ripple::test::jtx::Env* penv)
    {
        std::uint32_t lastShardIndex {0u};
        std::uint32_t id {0u};
        int transitions {0};
        bool gotGenesisShard {false};

        for (auto& b : batch)
        {
            auto stat = db.prepare(db.ledgersPerShard() * 256);
            if (! stat)
                throw std::runtime_error("prepare failed");
            auto seq = *stat;

            if ((! gotGenesisShard) &&
                    (db.seqToShardIndex(seq) == db.seqToShardIndex(genesisSeq)))
            {
                tc.log << "Got assigned genesis shard." << std::endl;
                gotGenesisShard = true;
            }

            if (lastShardIndex != db.seqToShardIndex(seq))
            {
                tc.log << "Got assigned shard " << db.seqToShardIndex(seq)
                    << " (" << seq << ")" << std::endl;
                if (lastShardIndex != 0u)
                    transitions++;
                lastShardIndex = db.seqToShardIndex(seq);
            }

            std::shared_ptr<NodeObject> const object (b.second);

            Blob data (object->getData ());

            db.store (object->getType (),
                      std::move (data),
                      object->getHash (),
                      seq);

            Config config;
            std::shared_ptr<Ledger> lger =
                std::make_shared<Ledger> (
                    seq,
                    penv->app().timeKeeper().closeTime(),
                    config,
                    penv->app().family());
            lger->stateMap().setLedgerSeq(seq);
            lger->txMap().setLedgerSeq(seq);

            id++;
            auto sle = std::make_shared<SLE>(ltACCOUNT_ROOT, uint256(id));
            sle->setFieldU32(sfSequence, 1);
            lger->rawInsert(sle);
            lger->stateMap().flushDirty(hotACCOUNT_NODE, seq);

            lger->setImmutable(config);
            db.setStored(lger);

            b.first = seq;
        }
        auto expectedTransitions = (batch.size() - 1) / db.ledgersPerShard();
        // the genesis shard is short (doesn't contain a full LPS) so there
        // will be one extra shard seen if/when we get assigned the genesis
        // shard.
        if (gotGenesisShard)
            expectedTransitions++;
        std::stringstream msg;
        msg << "saw " << transitions << " transitions, expected " <<
            expectedTransitions;
        BEAST_EXPECTS(transitions == expectedTransitions, msg.str());
    }

    // Fetch all the hashes in one batch, into another batch.
    static void fetchCopyOfBatch (Database& db,
                                  SeqBatch* pCopy,
                                  SeqBatch const& batch)
    {
        pCopy->clear ();
        pCopy->reserve (batch.size ());

        for (auto& b : batch)
        {
            std::shared_ptr<NodeObject> object = db.fetch (
                b.second->getHash (), b.first);

            if (object != nullptr)
                pCopy->push_back ( std::make_pair(b.first, object));
        }
    }


    template <class T>
    std::pair<
        std::unique_ptr<T, std::function<void(T*)>>,
        std::unique_ptr<ripple::test::jtx::Env>>
    factory(
        Scheduler&,
        Stoppable&,
        Section&,
        beast::Journal);

    template <class T>
    void testNodeStore (std::string const& type,
                        unsigned int numObjectsToTest = 2000,
                        unsigned int ledgersPerShard = 16384u,
                        std::uint64_t seedValue = 50u)
    {
        DummyScheduler scheduler;
        RootStoppable parent ("TestRootStoppable");
        beast::Journal j;

        std::string klass = beast::type_name<T>();
        auto pos = klass.find_last_of(':');
        if (pos != std::string::npos)
            klass = klass.substr(pos+1);
        testcase << type << " Backend with <" << klass
            << "> driver and " << numObjectsToTest << " objects";

        beast::temp_dir node_db;
        Section params;
        params.set ("type", type);
        params.set ("path", node_db.path());
        if (std::is_same<T, DatabaseShard>::value)
            params.set ("ledgers_per_shard", std::to_string(ledgersPerShard));

        beast::xor_shift_engine rng {seedValue};

        // Create a batch
        auto batch = createPredictableBatch (numObjectsToTest, rng());

        {
            // Open the database
            //std:pair<std::unique_ptr <T>, std::unique_ptr <ripple::test::jtx::Env>> fret =
            auto fret =
                factory<T>(scheduler, parent, params, j);

            if (std::is_same<T, DatabaseShard>::value)
                storeBatch (*fret.first, batch, *this, fret.second.get());
            else
                storeBatch (*fret.first, batch);

            {
                // Read it back in
                SeqBatch copy;
                fetchCopyOfBatch (*fret.first, &copy, batch);
                BEAST_EXPECT(areBatchesEqual (batch, copy));
            }

            {
                // Reorder and read the copy again
                std::shuffle (
                    batch.begin(),
                    batch.end(),
                    rng);
                SeqBatch copy;
                fetchCopyOfBatch (*fret.first, &copy, batch);
                BEAST_EXPECT(areBatchesEqual (batch, copy));
            }
        }

        if (type != "memory")
        {
            // Re-open the database without the ephemeral DB
            //std:pair<std::unique_ptr <T>, std::unique_ptr <ripple::test::jtx::Env>> fret =
            auto fret =
                factory<T>(scheduler, parent, params, j);

            // Read it back in
            SeqBatch copy;
            fetchCopyOfBatch (*fret.first, &copy, batch);

            // Canonicalize the source and destination batches
            std::sort (batch.begin (), batch.end (), LessThan{});
            std::sort (copy.begin (), copy.end (), LessThan{});
            BEAST_EXPECT(areBatchesEqual (batch, copy));
        }
    }

};

template <>
inline
std::pair<
    std::unique_ptr<Backend, std::function<void(Backend*)>>,
    std::unique_ptr<ripple::test::jtx::Env>>
TestBase::factory(
    Scheduler& scheduler,
    Stoppable&,
    Section& params,
    beast::Journal j)
{
    auto p = Manager::instance().make_Backend (params, scheduler, j);
    p->open();
    return std::make_pair(
       std::unique_ptr<Backend, std::function<void(Backend*)>>(
            p.release(), [](Backend* p){delete p;}),
        nullptr);
}

template <>
inline
std::pair<
    std::unique_ptr<Database, std::function<void(Database*)>>,
    std::unique_ptr<ripple::test::jtx::Env>>
TestBase::factory(
    Scheduler& scheduler,
    Stoppable& parent,
    Section& params,
    beast::Journal j)
{
    auto p = Manager::instance().make_Database (
        "test", scheduler, 2, parent, params, j);
    return std::make_pair(
        std::unique_ptr<Database, std::function<void(Database*)>>(
            p.release(), [](Database* p){delete p;}),
        nullptr);
}

template <>
inline
std::pair<
    std::unique_ptr<DatabaseShard, std::function<void(DatabaseShard*)>>,
    std::unique_ptr<ripple::test::jtx::Env>>
TestBase::factory(
    Scheduler& scheduler,
    Stoppable& parent,
    Section& params,
    beast::Journal j)
{
    auto penv = std::make_unique<ripple::test::jtx::Env>(
        *this, ripple::test::jtx::envconfig ([&params](std::unique_ptr<Config> cfg)
        {
            cfg->overwrite
                (ConfigSection::shardDatabase (), "type",
                 *params.get<std::string>("type"));
            cfg->overwrite
                (ConfigSection::shardDatabase (), "path",
                 *params.get<std::string>("path"));
            cfg->overwrite
                (ConfigSection::shardDatabase (), "ledgers_per_shard",
                 *params.get<std::string>("ledgers_per_shard"));
            cfg->overwrite
                (ConfigSection::shardDatabase (), "max_size_gb", "4");
            return cfg;
        }));

    // yes, this is a no-op deleter. This is so that we can still return a
    // unique_ptr type but we don't actually own this pointer, the Env does
    // and will free it when it deletes via its own unique_ptr.
    std::unique_ptr<DatabaseShard, std::function<void(DatabaseShard*)>> dp {
        penv->app().getShardStore(), [](DatabaseShard*){} };

    return std::make_pair (std::move(dp), std::move(penv));
}

}
}

#endif
