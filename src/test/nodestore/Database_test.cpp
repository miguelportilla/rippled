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
#include <test/nodestore/TestBase.h>
#include <ripple/nodestore/DummyScheduler.h>
#include <ripple/nodestore/Manager.h>
#include <ripple/beast/utility/temp_dir.h>

namespace ripple {
namespace NodeStore {

class Database_test : public TestBase
{
public:
    void testImport (std::string const& destBackendType,
        std::string const& srcBackendType, std::int64_t seedValue = 50)
    {
        DummyScheduler scheduler;
        RootStoppable parent ("TestRootStoppable");

        beast::temp_dir node_db;
        Section srcParams;
        srcParams.set ("type", srcBackendType);
        srcParams.set ("path", node_db.path());

        // Create a batch
        auto batch = createPredictableBatch (
            numObjectsToTest, seedValue);

        beast::Journal j;

        // Write to source db
        {
            std::unique_ptr <Database> src = Manager::instance().make_Database (
                "test", scheduler, 2, parent, srcParams, j);
            storeBatch (*src, batch);
        }

        SeqBatch copy;

        {
            // Re-open the db
            std::unique_ptr <Database> src = Manager::instance().make_Database (
                "test", scheduler, 2, parent, srcParams, j);

            // Set up the destination database
            beast::temp_dir dest_db;
            Section destParams;
            destParams.set ("type", destBackendType);
            destParams.set ("path", dest_db.path());

            std::unique_ptr <Database> dest = Manager::instance().make_Database (
                "test", scheduler, 2, parent, destParams, j);

            testcase ("import into '" + destBackendType +
                "' from '" + srcBackendType + "'");

            // Do the import
            dest->import (*src);

            // Get the results of the import
            fetchCopyOfBatch (*dest, &copy, batch);
        }

        // Canonicalize the source and destinaion batches
        std::sort (batch.begin (), batch.end (), LessThan{});
        std::sort (copy.begin (), copy.end (), LessThan{});
        BEAST_EXPECT(areBatchesEqual (batch, copy));
    }


    //--------------------------------------------------------------------------

    void run ()
    {
        testNodeStore<Database> ("memory");
        testNodeStore<Database> ("nudb");
    #if RIPPLE_ROCKSDB_AVAILABLE
        testNodeStore<Database> ("rocksdb");
    #endif

        testImport ("nudb",    "nudb");
    #if RIPPLE_ROCKSDB_AVAILABLE
        testImport ("rocksdb", "rocksdb");
    #endif
    }
};

BEAST_DEFINE_TESTSUITE(Database,NodeStore,ripple);

}
}
