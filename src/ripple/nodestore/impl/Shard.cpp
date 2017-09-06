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

#include <ripple/nodestore/impl/Shard.h>
#include <ripple/nodestore/Manager.h>

#include <ripple/app/ledger/InboundLedgers.h>

namespace ripple {
namespace NodeStore {

Shard::Shard(std::uint32_t shardIndex)
    : index_{shardIndex}
    , firstSeq_{std::max(genesisSeq, detail::firstSeq(shardIndex))}
    , lastSeq_{detail::lastSeq(shardIndex)}
{
    assert(index_ >= detail::genesisShardIndex);
}

bool
Shard::open(Section config, Scheduler& scheduler,
    boost::filesystem::path dir, beast::Journal& j)
{
    assert(!backend_);
    using namespace boost::filesystem;
    dir_ = dir / std::to_string(index_);
    config.set("path", dir_.string());
    auto newShard{!is_directory(dir_) || is_empty(dir_)};
    try
    {
        backend_ = Manager::instance().make_Backend(config, scheduler, j);
    }
    catch (std::exception const& e)
    {
        JLOG(j.error()) <<
            "Shard: Exception, " << e.what();
        return false;
    }

    if (backend_->fdlimit() == 0)
        return true;

    control_ = dir_ / controlFileName;
    if (newShard)
    {
        if (!saveControl())
        {
            JLOG(j.fatal()) <<
                "Unable to save control file";
            return false;
        }
    }
    else if (is_regular_file(control_))
    {
        std::ifstream ifs(control_.string());
        if (!ifs.is_open())
        {
            JLOG(j.error()) <<
                "Unable to open control file";
            return false;
        }
        boost::archive::text_iarchive ar(ifs);
        ar & storedSeqs_;
        if (!storedSeqs_.empty())
        {
            if (boost::icl::first(storedSeqs_) < firstSeq_ ||
                boost::icl::last(storedSeqs_) > lastSeq_)
            {
                JLOG(j.error()) <<
                    "Invalid control file";
                return false;
            }
            if (boost::icl::length(storedSeqs_) ==
                (index_ == detail::genesisShardIndex ?
                    detail::genesisNumLedgers : ledgersPerShard))
            {
                JLOG(j.debug()) <<
                    "Found control file for complete shard";
                storedSeqs_.clear();
                remove(control_);
                complete_ = true;
            }
        }
    }
    else
        complete_ = true;
    updateFileSize();
    return true;
}

bool
Shard::setStored(std::shared_ptr<Ledger const> const& l,
    beast::Journal& j)
{
    assert(backend_);
    if (l->info().seq < firstSeq_ || l->info().seq > lastSeq_)
    {
        assert(false);
        JLOG(j.error()) <<
            "Invalid ledger seq " <<
            std::to_string(l->info().seq) <<
            " for shard " << std::to_string(index_);
        return false;
    }
    if (complete_ || boost::icl::contains(storedSeqs_, l->info().seq))
    {
        assert(false);
        JLOG(j.error()) <<
            "ledger seq " <<
            std::to_string(l->info().seq) <<
            " already stored in shard " <<
            std::to_string(index_);
        return false;
    }
    storedSeqs_.insert(l->info().seq);
    if (boost::icl::length(storedSeqs_) ==
        (index_ == detail::genesisShardIndex ?
            detail::genesisNumLedgers : ledgersPerShard))
    {
        complete_ = true;
        storedSeqs_.clear();
        if (backend_->fdlimit() == 0)
            return true;
        remove(control_);
    }
    else
    {
        lastStored_ = l;
        if (backend_->fdlimit() == 0)
            return true;
        if (!saveControl())
        {
            JLOG(j.fatal()) <<
                "Unable to save control file";
            return false;
        }
    }
    updateFileSize();
    return true;
}

boost::optional<std::uint32_t>
Shard::prepare()
{
    if (storedSeqs_.empty())
         return lastSeq_;
    return prevMissing(storedSeqs_, 1 + lastSeq_, firstSeq_);
}

bool
Shard::hasLedger(std::uint32_t seq) const
{
    if (seq < firstSeq_ || seq > lastSeq_)
        return false;
    if (complete_)
        return true;
    return boost::icl::contains(storedSeqs_, seq);
}

void
Shard::validate(Application& app, beast::Journal& j)
{
    std::uint32_t lastSeq{detail::lastSeq(index_)};
    std::shared_ptr<Ledger> l;
    uint256 hash;
    {
        std::uint32_t seq;
        std::tie(l, seq, hash) = loadLedgerHelper(
            "WHERE LedgerSeq >= " + std::to_string(lastSeq) +
            " order by LedgerSeq asc limit 1", app);
        if (!l)
        {
            JLOG(j.fatal()) <<
                "Unable to validate shard " <<
                std::to_string(index_) <<
                ", can't find lookup data";
            return;
        }
        if (seq != lastSeq)
        {
            if (auto h = hashOfSeq(*l, lastSeq, j))
                hash = *h;
            else
            {
                JLOG(j.fatal()) <<
                    "Unable to validate shard " <<
                    std::to_string(index_) <<
                    ", can't find hash for ledger seq " <<
                    std::to_string(lastSeq);
                return;
            }
        }
    }

    std::uint32_t firstSeq{detail::firstSeq(index_)};
    std::shared_ptr<Ledger> next;
    while (lastSeq >= firstSeq)
    {
        std::shared_ptr<NodeObject> nObj;
        try
        {
            if (backend_->fetch(hash.begin(), &nObj) == dataCorrupt)
            {
                JLOG(j.fatal()) <<
                    "Corrupt NodeObject hash" << hash;
                break;
            }
        }
        catch (std::exception const& e)
        {
            JLOG(j.fatal()) <<
                "Exception, " << e.what();
            break;
        }
        if (!nObj)
            break;
        l = std::make_shared<Ledger>(
            InboundLedger::deserializeHeader(makeSlice(nObj->getData()),
                true), app.config(), *app.shardFamily());
        if (l->info().hash != hash || l->info().seq != lastSeq)
        {
            JLOG(j.fatal()) <<
                "hash " << hash <<
                " seq " << std::to_string(lastSeq) <<
                " cannot be a ledger";
            break;
        }
        l->stateMap().setLedgerSeq(lastSeq);
        l->txMap().setLedgerSeq(lastSeq);
        if (!l->stateMap().fetchRoot(
            SHAMapHash{l->info().accountHash}, nullptr))
        {
            JLOG(j.fatal()) <<
                "Don't have Account State root for ledger";
            break;
        }
        if (l->info().txHash.isNonZero())
        {
            if (!l->txMap().fetchRoot(
                SHAMapHash{l->info().txHash}, nullptr))
            {
                JLOG(j.fatal()) <<
                    "Don't have TX root for ledger";
                break;
            }
        }
        if (!valLedger(l, next, j))
            break;
        hash = l->info().parentHash;
        --lastSeq;
        next = l;
    }
    std::string s("shard " + std::to_string(index_) + " (" +
        std::to_string(detail::firstSeq(index_)) + "-" +
            std::to_string(detail::lastSeq(index_)) + "). ");
    if (lastSeq < firstSeq)
        s += "Valid and complete.";
    else if (complete_)
        s += "Invalid, failed on seq " +
        std::to_string(lastSeq) + " hash " + to_string(hash);
    else
        s += "Incomplete, stopped at seq " +
            std::to_string(lastSeq) + " hash " + to_string(hash);
    JLOG(j.fatal()) << s;
}

void
Shard::updateFileSize()
{
    fileSize_ = 0;
    using namespace boost::filesystem;
    boost::system::error_code ec;
    std::uint64_t fs;
    // using error code handling here...sometimes files in these directories can
    // be moving targets
    for (auto const& de : directory_iterator(dir_))
    {
        if (is_regular_file(de, ec))
        {
            if (ec) // TODO - warn log this ?
                continue;
            fs = file_size(de, ec);
            if (ec) // TODO - warn log this ?
                continue;
            fileSize_ += fs;
        }
    }
}

bool
Shard::saveControl()
{
    std::ofstream ofs{ control_.string(), std::ios::trunc };
    if (!ofs.is_open())
        return false;
    boost::archive::text_oarchive ar(ofs);
    ar & storedSeqs_;
    return true;
}

bool
Shard::valLedger(std::shared_ptr<Ledger const> const& l,
    std::shared_ptr<Ledger const> const& next, beast::Journal& j)
{
    if (l->info().accountHash.isZero())
        return false;
    bool error {false};
    auto f = [&, this](SHAMapAbstractNode& node) {
        auto hash {node.getNodeHash().as_uint256()};
        std::shared_ptr<NodeObject> nObj;
        try
        {
            if (backend_->fetch(hash.begin(), &nObj) == dataCorrupt)
            {
                JLOG(j.fatal()) << "Corrupt NodeObject hash" << hash;
                error = true;
            }
        }
        catch (std::exception const& e)
        {
            JLOG(j.fatal()) << "Exception, " << e.what();
            error = true;
        }
        return !error;
    };

    if (l->stateMap().getHash().isNonZero())
    {
        if (!l->stateMap().isValid())
            return false;
        try
        {
            if (next)
                l->stateMap().visitDifferences(&next->stateMap(), f);
            else
                l->stateMap().visitNodes(f);
        }
        catch (std::exception const& e)
        {
            JLOG(j.fatal()) << "Exception, " << e.what();
            return false;
        }
        if (error)
            return false;
    }
    if (l->info().txHash.isNonZero())
    {
        if (!l->txMap().isValid())
            return false;
        try
        {
            if (next)
                l->txMap().visitDifferences(&next->txMap(), f);
            else
                l->txMap().visitNodes(f);
        }
        catch (std::exception const& e)
        {
            JLOG(j.fatal()) << "Exception, " << e.what();
            return false;
        }
        if (error)
            return false;
    }
    return true;
};


} // NodeStore
} // ripple
