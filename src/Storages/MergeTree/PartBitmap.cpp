#include <Storages/MergeTree/PartBitmap.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
}

PartBitmap::MutablePtr PartBitmap::clone() const
{
    RoaringBitmapPtr new_bitmap = std::make_shared<RoaringBitmap>();
    new_bitmap->rb_or(*bitmap);
    return create(new_bitmap, seq_id, update_seq_id);
}

void PartBitmap::serialize(MutableDataPartStoragePtr data_part_storage)
{
    String path = data_part_storage->getFullPath();
    try
    {
        {
            auto out = data_part_storage->writeFile("bitmap.bin.tmp", DBMS_DEFAULT_BUFFER_SIZE, {});
            bitmap->write(*out);
            out->write(reinterpret_cast<char *>(&seq_id), sizeof(seq_id));
            out->write(reinterpret_cast<char *>(&update_seq_id), sizeof(update_seq_id));
        }
        fs::rename(fs::path(path) / "bitmap.bin.tmp", fs::path(path) / "bitmap.bin");
        LOG_INFO(&Poco::Logger::root(), "Serialize Part bitmap {}bitmap.bin (seq={} update_seq={}) successfully.",
                 path, seq_id, update_seq_id);
    }
    catch (...)
    {
        tryLogCurrentException(&Poco::Logger::root(), "Failed to serialize bitmap");
        throw Exception(
            ErrorCodes::UNKNOWN_EXCEPTION,
            "Failed to serialize bitmap {}bitmap.bin, seq_id: {}, update_seq_id: {}",
            path,
            seq_id,
            update_seq_id);
    }
}

void PartBitmap::deserialize(MutableDataPartStoragePtr data_part_storage)
{
    String path = data_part_storage->getFullPath();
    try
    {
        auto in = data_part_storage->readFile("bitmap.bin", {}, std::nullopt, std::nullopt);
        bitmap->read(*in);
        in->readStrict(reinterpret_cast<char *>(&seq_id), sizeof(seq_id));
        in->readStrict(reinterpret_cast<char *>(&update_seq_id), sizeof(update_seq_id));
        LOG_INFO(&Poco::Logger::root(), "Deserialize Part bitmap {}bitmap.bin (seq={} update_seq={}) successfully.",
                 path, seq_id, update_seq_id);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw Exception(
            ErrorCodes::UNKNOWN_EXCEPTION,
            "Failed to deserialize bitmap {}bitmap.bin, seq_id: {}, update_seq_id: {}",
            path,
            seq_id,
            update_seq_id);
    }
}

String PartBitmap::toString() const
{
    PaddedPODArray<UInt32> res_data;
    bitmap->rb_to_array(res_data);

    WriteBufferFromOwnString buf;
    buf << "list: [";
    for (UInt32 v : res_data)
    {
        buf << v << ", ";
    }
    buf << "]";
    return buf.str();
}

PartBitmap::Ptr PartBitmapEntry::getPartBitmap()
{
    if (part_bitmap != nullptr)
        return part_bitmap;

    auto lock = lockPartBitmap();

    if (part_bitmap != nullptr)
        return part_bitmap;

    auto data_part = getDataPart();
    PartBitmap::MutablePtr new_part_bitmap = PartBitmap::create(0, 0);
    if (!data_part->isEmpty())
    {
        MutableDataPartStoragePtr data_part_storage = const_cast<IMergeTreeDataPart *>(data_part.get())->getDataPartStoragePtr();
        new_part_bitmap->deserialize(data_part_storage);
    }

    part_bitmap = std::move(new_part_bitmap);
    return part_bitmap;
}

void PartBitmapEntry::setPartBitmap(PartBitmap::MutablePtr new_part_bitmap)
{
    auto lock = lockPartBitmap();

    auto data_part = getDataPart();
    MutableDataPartStoragePtr data_part_storage = const_cast<IMergeTreeDataPart *>(data_part.get())->getDataPartStoragePtr();
    new_part_bitmap->serialize(data_part_storage);
    part_bitmap = std::move(new_part_bitmap);
}

MergeTreeDataPartPtr PartBitmapEntry::getDataPart()
{
    auto data_part = part.lock();
    if (data_part)
        return data_part;

    LOG_WARNING(&Poco::Logger::get("PartBitmapCache"), "It is a bug, data part {} has been removed.", part_name);
    return nullptr;
}

PartBitmapEntryPtr PartBitmapCache::getPartBitmapEntry(const MergeTreeDataPartPtr & data_part)
{
    std::lock_guard lock(mutex);
    auto it = find(data_part->name);
    if (it == end())
    {
        it = emplace(data_part->name, std::make_shared<PartBitmapEntry>(data_part)).first;
    }

    return it->second;
}

PartBitmap::Ptr PartBitmapCache::getPartBitmap(const MergeTreeDataPartPtr & data_part)
{
    auto entry = getPartBitmapEntry(data_part);
    return entry->getPartBitmap();
}

void PartBitmapCache::setPartBitmap(const MergeTreeDataPartPtr & data_part, PartBitmap::MutablePtr new_part_bitmap)
{
    auto entry = getPartBitmapEntry(data_part);
    entry->setPartBitmap(std::move(new_part_bitmap));
}

void PartBitmapCache::removePartBitmap(const MergeTreeDataPartPtr & data_part)
{
    std::lock_guard lock(mutex);
    if (auto it = find(data_part->name); it != end())
        erase(it);
}

}
