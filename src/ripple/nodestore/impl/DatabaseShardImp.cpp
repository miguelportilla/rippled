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

#include <ripple/nodestore/impl/DatabaseShardImp.h>
#include <ripple/app/ledger/InboundLedgers.h>
#include <ripple/app/ledger/Ledger.h>
#include <ripple/basics/chrono.h>
#include <ripple/basics/random.h>
#include <ripple/nodestore/Manager.h>
#include <ripple/protocol/HashPrefix.h>

namespace ripple {
namespace NodeStore {

DatabaseShardImp::DatabaseShardImp(std::string const& name,
    Stoppable& parent, Scheduler& scheduler, int readThreads,
        Section const& config, beast::Journal journal)
    : DatabaseShard(name, parent, scheduler, readThreads, journal)
    , config_(config)
    , dir_(get<std::string>(config, "path"))
    , maxDiskSpace_(get<std::uint64_t>(config, "max_size_gb") << 30)
{
}

DatabaseShardImp::~DatabaseShardImp()
{
    stopThreads();
}

bool
DatabaseShardImp::init()
{
    using namespace boost::filesystem;
    {
        // Validate backend
        Factory* f;
        {
            std::string const type(get<std::string>(config_, "type"));
            if (type.empty() || !(f = Manager::instance().find(type)))
            {
                JLOG(j_.error()) <<
                    "Invalid shard store type specified";
                return false;
            }
        }

        std::string tmpDir;
        auto i = 0;
        do
            tmpDir = "TMP" + std::to_string(i++);
        while (is_directory(dir_ / tmpDir));

        auto d = dir_ / tmpDir;
        auto config{config_};
        config.set("path", d.string());
        {
            fdLimit_ = f->createInstance(NodeObject::keyBytes,
                config, scheduler_, j_)->fdlimit();
            backed_ = static_cast<bool>(fdLimit_);
        }
        remove_all(d);
    }
    if (!backed_)
        return true;

    // Find shards
    for (auto const& de : directory_iterator(dir_))
    {
        if (!is_directory(de))
            continue;
        auto dirName = de.path().stem().string();
        if (!std::all_of(dirName.begin(), dirName.end(), ::isdigit))
            continue;
        auto shardIndex = std::stoul(dirName);
        if (shardIndex < detail::genesisShardIndex)
            continue;
        auto shard = std::make_unique<Shard>(shardIndex);
        if (!shard->open(config_, scheduler_, dir_, j_))
            return false;
        usedDiskSpace_ += shard->fileSize();
        if (shard->complete())
            complete_.emplace(shard->index(), std::move(shard));
        else
        {
            if (incomplete_)
            {
                JLOG(j_.error()) <<
                    "More than one control file";
                return false;
            }
            incomplete_ = std::move(shard);
        }
    }
    if (!incomplete_ && complete_.empty())
    {
        // New Shard Store, calculate file descriptor requirements
        if (maxDiskSpace_ > space(dir_).free)
        {
            JLOG(j_.warn()) << "Insufficient disk space";
        }
        fdLimit_ = 1 + (fdLimit_ *
            std::max<std::uint64_t>(1, maxDiskSpace_ / avgShardSz_));
    }
    else
        updateStats();
    return true;
}

boost::optional<std::uint32_t>
DatabaseShardImp::prepare(std::uint32_t validLedgerSeq)
{
    std::lock_guard<std::mutex> l(m_);
    if (incomplete_)
        return incomplete_->prepare();
    if (!canAdd_)
        return boost::none;
    if (backed_)
    {
        // Create a new shard to acquire
        if (usedDiskSpace_ + avgShardSz_ > maxDiskSpace_)
        {
            JLOG(j_.debug()) <<
                "Maximum size reached";
            canAdd_ = false;
            return boost::none;
        }
        if (avgShardSz_ > boost::filesystem::space(dir_).free)
        {
            JLOG(j_.warn()) <<
                "Insufficient disk space";
            canAdd_ = false;
            return boost::none;
        }
    }

    auto const shardIndexToAdd = findShardIndexToAdd(validLedgerSeq);
    if (!shardIndexToAdd)
    {
        JLOG(j_.debug()) <<
            "no new shards to add";
        canAdd_ = false;
        return boost::none;
    }

    incomplete_ = std::make_unique<Shard>(*shardIndexToAdd);
    if (!incomplete_->open(config_, scheduler_, dir_, j_))
    {
        incomplete_.reset();
        remove_all(dir_ / std::to_string(*shardIndexToAdd));
        return boost::none;
    }
    return incomplete_->prepare();
}

std::shared_ptr<Ledger>
DatabaseShardImp::fetchLedger(Application& app,
    uint256 const& hash, std::uint32_t seq)
{
    if (!hasLedger(seq))
        return {};
    auto nObj = fetch(hash, seq);
    if (!nObj)
        return {};

    auto ledger = std::make_shared<Ledger>(
        InboundLedger::deserializeHeader(makeSlice(nObj->getData()), true),
            app.config(), *app.shardFamily());
    if (ledger->info().hash != hash || ledger->info().seq != seq)
    {
        JLOG(j_.error()) <<
            "hash " << hash <<
            " seq " << std::to_string(seq) <<
            " cannot be a ledger";
        return {};
    }
    ledger->stateMap().setLedgerSeq(seq);
    ledger->txMap().setLedgerSeq(seq);

    if (!ledger->stateMap().fetchRoot(
        SHAMapHash{ledger->info().accountHash}, nullptr))
    {
        JLOG(j_.error()) <<
            "Don't have Account State root for ledger";
        return {};
    }
    if (ledger->info().txHash.isNonZero())
    {
        if (!ledger->txMap().fetchRoot(
            SHAMapHash{ledger->info().txHash}, nullptr))
        {
            JLOG(j_.error()) <<
                "Don't have TX root for ledger";
            return {};
        }
    }
    return ledger;
}

void
DatabaseShardImp::setStored(std::shared_ptr<Ledger const> const& ledger)
{
    auto const shardIndex = seqToShardIndex(ledger->info().seq);
    std::unique_lock<std::mutex> l(m_);
    if (!incomplete_ || shardIndex != incomplete_->index())
    {
        JLOG(j_.warn()) <<
            "ledger seq " << std::to_string(ledger->info().seq) <<
            " is not being acquired";
        return;
    }
    update(ledger);
}

bool
DatabaseShardImp::hasLedger(std::uint32_t seq)
{
    auto const shardIndex = seqToShardIndex(seq);
    std::lock_guard<std::mutex> l(m_);
    if (complete_.find(shardIndex) != complete_.end())
        return true;
    if (incomplete_ && incomplete_->index() == shardIndex)
        return incomplete_->hasLedger(seq);
    return false;
}

std::string
DatabaseShardImp::getCompleteShards()
{
    std::lock_guard<std::mutex> l(m_);
    return status_;
}

void
DatabaseShardImp::validate(Application& app)
{
    if (complete_.empty() && !incomplete_)
    {
        JLOG(j_.fatal()) <<
            "No shards to validate";
        return;
    }

    std::string s{"Validating shards "};
    for (auto& e : complete_)
        s += std::to_string(e.second->index()) + ",";
    if (incomplete_)
        s += std::to_string(incomplete_->index());
    else
        s.pop_back();
    JLOG(j_.fatal()) << s;

    for (auto& e : complete_)
        e.second->validate(app, j_);
    if (incomplete_)
        incomplete_->validate(app, j_);
}

std::int32_t
DatabaseShardImp::getWriteLoad() const
{
    std::int32_t wl{0};
    {
        std::lock_guard<std::mutex> l(m_);
        for (auto const& s : complete_)
            wl += s.second->getBackend().getWriteLoad();
        if (incomplete_)
            wl += incomplete_->getBackend().getWriteLoad();
    }
    return wl;
}

void
DatabaseShardImp::store(NodeObjectType type,
    Blob&& data, uint256 const& hash, std::uint32_t seq)
{
    auto const shardIndex = seqToShardIndex(seq);
    std::unique_lock<std::mutex> l(m_);
    if (!incomplete_ || shardIndex != incomplete_->index())
    {
        l.unlock();
        JLOG(j_.warn()) <<
            "ledger seq " << std::to_string(seq) <<
            " is not being acquired";
        return;
    }
    auto& backend = incomplete_->getBackend();
    l.unlock();
    storeInternal(type, std::move(data), hash, backend);
    JLOG(j_.trace()) <<
        "shard " << std::to_string(shardIndex) <<
        " ledger seq " << std::to_string(seq) << " stored";
}

bool
DatabaseShardImp::copyLedger(std::shared_ptr<Ledger const> const& ledger)
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
    auto const shardIndex = seqToShardIndex(ledger->info().seq);
    std::unique_lock<std::mutex> l(m_);
    auto it = complete_.find(shardIndex);
    if (it != complete_.end())
    {
        l.unlock();
        JLOG(j_.warn()) <<
            "ledger seq " <<
            std::to_string(ledger->info().seq) <<
            " already stored";
        return false;
    }
    if (!incomplete_ || shardIndex != incomplete_->index())
    {
        l.unlock();
        JLOG(j_.warn()) <<
            "ledger seq " << std::to_string(ledger->info().seq) <<
            " is not being acquired";
        return false;
    }

    auto& backend = incomplete_->getBackend();
    auto next = incomplete_->lastStored();
    l.unlock();
    if (next && next->info().parentHash != ledger->info().hash)
        next = nullptr;
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
        if (!ledger->stateMap().isValid())
        {
            JLOG(j_.error()) <<
                "invalid state map";
            return false;
        }
        if (next)
        {
            auto have = next->stateMap().snapShot(false);
            ledger->stateMap().snapShot(false)->visitDifferences(&(*have), f);
        }
        else
            ledger->stateMap().snapShot(false)->visitNodes(f);
        if (error)
            return false;
    }
    // Batch the transaction map
    if (ledger->info().txHash.isNonZero())
    {
        if (!ledger->txMap().isValid())
        {
            JLOG(j_.error()) <<
                "invalid transaction map";
            return false;
        }
        if (next)
        {
            auto have = next->txMap().snapShot(false);
            ledger->txMap().snapShot(false)->visitDifferences(&(*have), f);
        }
        else
            ledger->txMap().snapShot(false)->visitNodes(f);
        if (error)
            return false;
    }
    // Store batch in shard
    storeBatchInternal(batch, backend);
    l.lock();
    update(ledger);
    return true;
}

std::shared_ptr<NodeObject>
DatabaseShardImp::fetchFrom(uint256 const& hash, std::uint32_t seq)
{
    auto const shardIndex = seqToShardIndex(seq);
    std::unique_lock<std::mutex> l(m_);
    auto it = complete_.find(shardIndex);
    if (it != complete_.end())
    {
        auto& backend = it->second->getBackend();
        l.unlock();
        return fetchInternal(hash, backend);
    }
    if (incomplete_ && incomplete_->index() == shardIndex)
    {
        auto& backend = incomplete_->getBackend();
        l.unlock();
        return fetchInternal(hash, backend);
    }
    return {};
}

// Lock must be held
boost::optional<std::uint32_t>
DatabaseShardImp::findShardIndexToAdd(std::uint32_t validLedgerSeq)
{
    auto maxShardIndex = seqToShardIndex(validLedgerSeq);
    if (validLedgerSeq != detail::lastSeq(maxShardIndex))
        --maxShardIndex;

    auto numShards = complete_.size() + (incomplete_ ? 1 : 0);
    assert(numShards <= maxShardIndex + 1);

    // If equal, have all the shards
    if (numShards >= maxShardIndex + 1)
        return boost::none;

    if (maxShardIndex < 1024 || float(numShards) / maxShardIndex > 0.5f)
    {
        // Small or mostly full index space to sample
        // Find the available indexes and select one at random
        std::vector<std::uint32_t> available;
        available.reserve(maxShardIndex - numShards + 1);
        for (std::uint32_t i = detail::genesisShardIndex;
            i <= maxShardIndex; ++i)
        {
            if (complete_.find(i) == complete_.end() &&
                (!incomplete_ || incomplete_->index() != i))
                    available.push_back(i);
        }
        if (!available.empty())
            return available[rand_int(0u,
                static_cast<std::uint32_t>(available.size() - 1))];
    }

    // Large, sparse index space to sample
    // Keep choosing indexes at random until an available one is found
    // chances of running more than 30 times is less than 1 in a billion
    for (int i = 0; i < 40; ++i)
    {
        auto const r = rand_int(detail::genesisShardIndex, maxShardIndex);
        if (complete_.find(r) == complete_.end() &&
            (!incomplete_ || incomplete_->index() != r))
                return r;
    }
    assert(0);
    return boost::none;
}

// Lock must be held
void
DatabaseShardImp::update(std::shared_ptr<Ledger const> const& ledger)
{
    auto sz {incomplete_->fileSize()};
    if (!incomplete_->setStored(ledger, j_))
        return;
    usedDiskSpace_ += (incomplete_->fileSize() - sz);

    JLOG(j_.debug()) <<
        "ledger seq " <<
        std::to_string(ledger->info().seq) <<
        " stored in shard " <<
        std::to_string(incomplete_->index()) << ".";

    if (!incomplete_->complete())
        return;

    JLOG(j_.trace()) <<
        "shard " << std::to_string(incomplete_->index()) <<
        " complete.";
    complete_.emplace(incomplete_->index(), std::move(incomplete_));
    incomplete_.reset();
    updateStats();
}

// Lock must be held
void
DatabaseShardImp::updateStats()
{
    // Calculate shard file sizes
    std::uint32_t filesPerShard{0};
    if (!complete_.empty())
    {
        status_.clear();
        filesPerShard = complete_.begin()->second->fdlimit();
        auto avgShardSz = 0;
        for (auto it = complete_.begin(); it != complete_.end(); ++it)
        {
            if (it == complete_.begin())
                status_ = std::to_string(it->first);
            else
            {
                if (it->first - std::prev(it)->first > 1)
                {
                    if (status_.back() == '-')
                        status_ += std::to_string(std::prev(it)->first);
                    status_ += "," + std::to_string(it->first);
                }
                else
                {
                    if (status_.back() != '-')
                        status_ += "-";
                    if (std::next(it) == complete_.end())
                        status_ += std::to_string(it->first);
                }
            }
            avgShardSz += it->second->fileSize();
        }
        if (backed_)
            avgShardSz_ = avgShardSz / complete_.size();
    }
    else if(incomplete_)
        filesPerShard = incomplete_->fdlimit();
    if (!backed_)
        return;

    fdLimit_ = 1 + (filesPerShard *
        (complete_.size() + (incomplete_ ? 1 : 0)));

    if (usedDiskSpace_ >= maxDiskSpace_)
    {
        JLOG(j_.warn()) << "Maximum size reached";
        canAdd_ = false;
    }
    else
    {
        auto const sz = maxDiskSpace_ - usedDiskSpace_;
        if (sz > space(dir_).free)
        {
            JLOG(j_.warn()) <<
                "Max Shard Store size exceeds remaining free disk space";
        }
        fdLimit_ += (filesPerShard * (sz / avgShardSz_));
    }
}

} // NodeStore
} // ripple
