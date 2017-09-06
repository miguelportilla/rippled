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
#include <ripple/nodestore/Backend.h>
#include <ripple/nodestore/DummyScheduler.h>
#include <ripple/nodestore/Manager.h>
#include <ripple/nodestore/Types.h>
#include <ripple/nodestore/DatabaseShard.h>
#include <ripple/nodestore/impl/DatabaseShardImp.h>
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

    // Store a batch in a backend
    static void storeBatch (beast::unit_test::suite&, Backend& backend, SeqBatch& batch)
    {
        for (auto& b : batch)
        {
            backend.store (b.second);
        }
    }

    // Get a copy of a batch in a backend
    void fetchCopyOfBatch (Backend& backend, SeqBatch* pCopy, SeqBatch const& batch)
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

    // Store all objects in a batch
    static void storeBatch (beast::unit_test::suite&, Database& db, SeqBatch& batch)
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

    static void storeBatch (beast::unit_test::suite& tc, DatabaseShard& db, SeqBatch& batch)
    {
        ripple::test::jtx::Env env{tc};
        std::uint32_t lastSeq {0u};
        std::uint32_t seq {0u};
        for (auto& b : batch)
        {
            auto stat = db.prepare(ledgersPerShard * 256);
            if (! stat)
                throw std::runtime_error("prepare failed");
            seq = *stat;
            if (lastSeq != 0 && seq != (lastSeq-1))
            {
                tc.log << "switched shard from " <<
                    seqToShardIndex(lastSeq) << " (" << lastSeq << ")" <<
                    " to " << seqToShardIndex(seq) << " (" << seq << ")" <<
                    std::endl;
            }

            std::shared_ptr<NodeObject> const object (b.second);

            Blob data (object->getData ());

            db.store (object->getType (),
                      std::move (data),
                      object->getHash (),
                      seq);

            Config config;
            std::shared_ptr<Ledger const> const lger =
                std::make_shared<Ledger> (
                    seq,
                    env.app().timeKeeper().closeTime(),
                    config,
                    env.app().family());
            db.setStored(lger);

            lastSeq = b.first = seq;
        }
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
    static
    std::unique_ptr<T> factory(
        Scheduler&, Stoppable&, Section&, beast::Journal);

    template <class T>
    void testNodeStore (std::string const& type,
                        int numObjectsToTest = 2000,
                        std::uint64_t const seedValue = 50u)
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

        beast::xor_shift_engine rng {seedValue};

        // Create a batch
        auto batch = createPredictableBatch (numObjectsToTest, rng());

        {
            // Open the database
            std::unique_ptr <T> db = factory<T>(
                scheduler, parent, params, j);

            // Write the batch
            storeBatch (*this, *db, batch);

            {
                // Read it back in
                SeqBatch copy;
                fetchCopyOfBatch (*db, &copy, batch);
                BEAST_EXPECT(areBatchesEqual (batch, copy));
            }

            {
                // Reorder and read the copy again
                std::shuffle (
                    batch.begin(),
                    batch.end(),
                    rng);
                SeqBatch copy;
                fetchCopyOfBatch (*db, &copy, batch);
                BEAST_EXPECT(areBatchesEqual (batch, copy));
            }
        }

        if (type != "memory")
        {
            // Re-open the database without the ephemeral DB
            std::unique_ptr <T> db = factory<T>(
                scheduler, parent, params, j);

            // Read it back in
            SeqBatch copy;
            fetchCopyOfBatch (*db, &copy, batch);

            // Canonicalize the source and destination batches
            std::sort (batch.begin (), batch.end (), LessThan{});
            std::sort (copy.begin (), copy.end (), LessThan{});
            BEAST_EXPECT(areBatchesEqual (batch, copy));
        }
    }

};

template <>
inline std::unique_ptr<Backend> TestBase::factory(
    Scheduler& scheduler,
    Stoppable&,
    Section& params,
    beast::Journal j)
{
    return Manager::instance().make_Backend (
        params, scheduler, j);
}

template <>
inline std::unique_ptr<Database> TestBase::factory(
    Scheduler& scheduler,
    Stoppable& parent,
    Section& params,
    beast::Journal j)
{
    return Manager::instance().make_Database (
        "test", scheduler, 2, parent, params, j);
}

template <>
inline std::unique_ptr<DatabaseShard> TestBase::factory(
    Scheduler& scheduler,
    Stoppable& parent,
    Section& params,
    beast::Journal j)
{
    params.set ("max_size_gb", "4");
    std::unique_ptr<DatabaseShard> ptr =
        std::make_unique<NodeStore::DatabaseShardImp>(
            "test", parent, scheduler, 2, params, j);
    if (! ptr->init())
        throw std::runtime_error("init failed");
    return ptr;
}

}
}

#endif
