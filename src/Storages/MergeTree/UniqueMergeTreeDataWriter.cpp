#include <Storages/MergeTree/UniqueMergeTreeDataWriter.h>

#include <Columns/ColumnConst.h>
#include <Columns/IColumn.h>
#include <Common/HashTable/HashMap.h>
#include <Common/Exception.h>
#include <Disks/createVolume.h>
#include <Interpreters/AggregationCommon.h>
#include <Storages/MergeTree/UniqueMergeTreeBlockOutputStream.h>
#include <Interpreters/Context.h>
#include <IO/HashingWriteBuffer.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <IO/WriteHelpers.h>
#include <Poco/File.h>
#include <Common/typeid_cast.h>


#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <vector>


namespace ProfileEvents
{
    extern const Event MergeTreeDataWriterBlocks;
    extern const Event MergeTreeDataWriterBlocksAlreadySorted;
    extern const Event MergeTreeDataWriterRows;
    extern const Event MergeTreeDataWriterUncompressedBytes;
    extern const Event MergeTreeDataWriterCompressedBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_PARTS;
}

MergeTreeData::MutableDataPartPtr UniqueMergeTreeDataWriter::writeTempPart(BlockWithPartition & block_with_partition, const StorageMetadataPtr & metadata_snapshot)
{
    Block & block = block_with_partition.block;

    static const String TMP_PREFIX = "tmp_insert_";

    /// This will generate unique name in scope of current server process.
    Int64 temp_index = data.insert_increment.get();

    IMergeTreeDataPart::MinMaxIndex minmax_idx;
    minmax_idx.update(block, data.minmax_idx_columns);

    MergeTreePartition partition(std::move(block_with_partition.partition));

    String partition_id = partition.getID(metadata_snapshot->getPartitionKey().sample_block);
    MergeTreePartInfo new_part_info(partition_id, temp_index, temp_index, 0);
    String part_name;
    if (data.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        DayNum min_date(minmax_idx.hyperrectangle[data.minmax_idx_date_column_pos].left.get<UInt64>());
        DayNum max_date(minmax_idx.hyperrectangle[data.minmax_idx_date_column_pos].right.get<UInt64>());

        const auto & date_lut = DateLUT::instance();

        auto min_month = date_lut.toNumYYYYMM(min_date);
        auto max_month = date_lut.toNumYYYYMM(max_date);

        if (min_month != max_month)
            throw Exception("Logical error: part spans more than one month.", ErrorCodes::LOGICAL_ERROR);

        part_name = new_part_info.getPartNameV0(min_date, max_date);
    }
    else
        part_name = new_part_info.getPartName();

    /// Size of part would not be greater than block.bytes() + epsilon
    size_t expected_size = block.bytes();

    DB::IMergeTreeDataPart::TTLInfos move_ttl_infos;
    const auto & move_ttl_entries = metadata_snapshot->getMoveTTLs();
    for (const auto & ttl_entry : move_ttl_entries)
        updateTTL(ttl_entry, move_ttl_infos, move_ttl_infos.moves_ttl[ttl_entry.result_column], block, false);

    NamesAndTypesList columns = metadata_snapshot->getColumns().getAllPhysical().filter(block.getNames());
    columns.insert(columns.end(), metadata_snapshot->internal_columns.begin(), metadata_snapshot->internal_columns.end());
    ReservationPtr reservation = data.reserveSpacePreferringTTLRules(expected_size, move_ttl_infos, time(nullptr));
    VolumePtr volume = data.getStoragePolicy()->getVolume(0);

    auto new_data_part = data.createPart(
            part_name,
            data.choosePartType(expected_size, block.rows()),
            new_part_info,
            createVolumeFromReservation(reservation, volume),
            TMP_PREFIX + part_name);

    new_data_part->setColumns(columns);
    new_data_part->partition = std::move(partition);
    new_data_part->minmax_idx = std::move(minmax_idx);
    new_data_part->is_temp = true;

    if (new_data_part->isStoredOnDisk())
    {
        /// The name could be non-unique in case of stale files from previous runs.
        String full_path = new_data_part->getFullRelativePath();

        if (new_data_part->volume->getDisk()->exists(full_path))
        {
            LOG_WARNING(log, "Removing old temporary directory {}", fullPath(new_data_part->volume->getDisk(), full_path));
            new_data_part->volume->getDisk()->removeRecursive(full_path);
        }

        new_data_part->volume->getDisk()->createDirectories(full_path);
    }


    // add column unique_key_id
    {
        int unique_columns_count = data.merging_params.unique_columns.size();
        std::vector<rocksdb::Slice> slices;
        int slices_width = unique_columns_count + 1;// reserve for partition_id
        slices.resize(block.rows() * slices_width);

        for (int i = 0; i<unique_columns_count; i++) {
            String unqiue_column_name = data.merging_params.unique_columns[i];
            ColumnWithTypeAndName column_type_name = block.getByName(unqiue_column_name);

            const IColumn* column = column_type_name.column.get();
            for (size_t j=0; j < block.rows(); j++) {
                slices[j*slices_width + i + 1] = {column->getDataAt(j).data, column->getDataAt(j).size};
            }
        }

        for (size_t i = 0; i < block.rows(); i++) {
            slices[i*slices_width] = {partition_id.data(), partition_id.size()};
        }

        std::vector<rocksdb::Slice> all_keys;
        all_keys.reserve(block.rows());
        std::vector<std::string> all_keys_string;
        all_keys_string.resize(block.rows());
        for (size_t i=0; i < block.rows(); i++) {
            all_keys.emplace_back(rocksdb::SliceParts(slices.data() + i * slices_width, slices_width),
                &all_keys_string[i]);
        }

        ColumnWithTypeAndName column;
        column.name = "_unique_key_id";
        column.type = std::make_shared<DataTypeUInt32>();
        auto col = column.type->createColumn();
        ColumnUInt32::Container & flag_data = typeid_cast<ColumnUInt32 &>(*col).getData();
        flag_data.resize(block.rows());
        column.column = std::move(col);
        block.insert(column);

        // tuple: 0: unique_key_id, 1: ver, 2: dirty 3: last_valid_index
        std::unordered_map<std::string, std::tuple<int, int, int, int>> key_map;
        time_t now = time(nullptr);
        ColumnPtr ver_cloumn;
        if (!data.merging_params.version_column.empty()) {
            ver_cloumn = block.getByName(data.merging_params.version_column).column;
        } else {
            auto tmp_ver_cloumn = ColumnUInt32::create();
            ColumnUInt32::Container & ver_data = typeid_cast<ColumnUInt32 &>(*tmp_ver_cloumn).getData();
            ver_data.resize_fill(block.rows(), now) ;
            ver_cloumn = std::move(tmp_ver_cloumn);
        }

        BitmapPtr bitmap = std::make_shared<Bitmap>();
        rocksdb::WriteBatch batch;
        int64_t part_bitmap_seq_id = -1;
        int64_t last_unique_key_id = -1;
        {
            std::vector<std::string> values;
            std::vector<rocksdb::Status> status;
            std::lock_guard write_lock(storage_data.getWritePartMutex());
            storage_data.readRocksDb(status, all_keys, values);
            part_bitmap_seq_id = storage_data.getAndIncreamentPartBitmapSeqId();

            for (size_t i=0; i<block.rows(); i++) {
                unsigned int ver_data = (*ver_cloumn).getUInt(i);
                if (status[i].ok()) {
                    auto itr = key_map.find(all_keys_string[i]);
                    int unique_key_id = -1;
                    unsigned int ver = 0;
                    if (itr == key_map.end()) {
                        char * v = values[i].data();
                        unique_key_id = *reinterpret_cast<int *>(v);
                        ver = *reinterpret_cast<int *>(v + sizeof(int));
                        itr = key_map.insert(std::make_pair(all_keys_string[i], std::tuple(unique_key_id, ver, 0, i))).first;
                    } else {
                        unique_key_id = std::get<0>(itr->second);
                        ver = std::get<1>(itr->second);
                    }

                    if (ver_data >= ver) {
                        uint32_t last_valid_index = std::get<3>(itr->second);
                        if (i != last_valid_index)
                            flag_data[last_valid_index] = -1;
                        flag_data[i] = unique_key_id;
                        itr->second = std::tuple(unique_key_id, ver_data, 1, i);
                        bitmap->add(unique_key_id);
                    } else {
                        flag_data[i] = -1;
                    }
                }
                else if (status[i].IsNotFound()) {
                    auto itr = key_map.find(all_keys_string[i]);
                    int unique_key_id = -1;
                    unsigned int ver = 0;
                    if (itr == key_map.end()) {
                        unique_key_id = storage_data.getAndIncreamentUniqueKeyId();
                        flag_data[i] = unique_key_id;
                        ver = ver_data;
                        key_map.insert(std::make_pair(all_keys_string[i], std::tuple(unique_key_id, ver, 1, i)));
                        bitmap->add(unique_key_id);
                    } else {
                        unique_key_id = std::get<0>(itr->second);
                        flag_data[i] = unique_key_id;
                        ver = std::get<1>(itr->second);

                        if (ver_data >= ver) {
                            int last_valid_index = std::get<3>(itr->second);
                            flag_data[last_valid_index] = -1;
                            itr->second = std::tuple(unique_key_id, ver_data, 1, i);
                            bitmap->add(unique_key_id);
                        } else
                            flag_data[i] = -1;
                    }
                }
            }

            for (auto itr : key_map) {
                std::tuple<int, int, int, int> & tuple = itr.second;
                if (std::get<2>(tuple) == 1) {
                    UInt32 vv[2];
                    vv[0] = std::get<0>(tuple);
                    vv[1] = std::get<1>(tuple);
                    rocksdb::Slice key(itr.first.data(), itr.first.length());
                    rocksdb::Slice value(reinterpret_cast<char *>(vv), sizeof(vv));
                    batch.Put(key, value);
                }
            }
            {
                rocksdb::Slice key("_last_update_seq_id");
                rocksdb::Slice value(reinterpret_cast<char *>(&(part_bitmap_seq_id)), sizeof(part_bitmap_seq_id));
                batch.Put(key, value);
            }
            {
                rocksdb::Slice key("_last_unique_key_id");
                last_unique_key_id = storage_data.getUniqueKeyId();
                rocksdb::Slice value(reinterpret_cast<char *>(&(last_unique_key_id)), sizeof(last_unique_key_id));
                batch.Put(key, value);
            }
            rocksdb::Status s = storage_data.writeRocksDb(batch);
            assert(s.ok());
        }

        PartBitmap::MutablePtr part_bitmap = PartBitmap::create(bitmap, new_data_part, part_bitmap_seq_id);
        part_bitmap->write(part_bitmap_seq_id);

        String db_update_file_path = new_data_part->getFullPath() + "key_id_log.bin";
        WriteBufferFromFile db_update_file(db_update_file_path);
        writeBinary(batch.Count(), db_update_file);
        for (auto itr : key_map) {
            std::tuple<int, int, int, int> & tuple = itr.second;
            if (std::get<2>(tuple) == 1) {
                UInt32 vv[2];
                vv[0] = std::get<0>(tuple);
                vv[1] = std::get<1>(tuple);

                int32_t len = itr.first.length();
                db_update_file.write(reinterpret_cast<char *>(&len), sizeof(int32_t));
                db_update_file.write(itr.first.data(), len);
                db_update_file.write(reinterpret_cast<char *>(vv), sizeof(vv));
            }
        }
        {
            String key = "_last_update_seq_id";
            int len = key.size();
            db_update_file.write(reinterpret_cast<char *>(&len), sizeof(int32_t));
            db_update_file.write(key.data(), len);
            db_update_file.write(reinterpret_cast<char *>(&part_bitmap_seq_id), sizeof(int64_t));
        }
        {
            String key = "_last_unique_key_id";
            int len = key.size();
            db_update_file.write(reinterpret_cast<char *>(&len), sizeof(int32_t));
            db_update_file.write(key.data(), len);
            db_update_file.write(reinterpret_cast<char *>(&last_unique_key_id), sizeof(int64_t));
        }

        db_update_file.close();
    }

    /// If we need to calculate some columns to sort.
    if (metadata_snapshot->hasSortingKey() || metadata_snapshot->hasSecondaryIndices())
        data.getSortingKeyAndSkipIndicesExpression(metadata_snapshot)->execute(block);

    Names sort_columns = metadata_snapshot->getSortingKeyColumns();
    SortDescription sort_description;
    size_t sort_columns_size = sort_columns.size();
    sort_description.reserve(sort_columns_size);

    for (size_t i = 0; i < sort_columns_size; ++i)
        sort_description.emplace_back(block.getPositionByName(sort_columns[i]), 1, 1);

    ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterBlocks);

    /// Sort
    IColumn::Permutation * perm_ptr = nullptr;
    IColumn::Permutation perm;
    if (!sort_description.empty())
    {
        if (!isAlreadySorted(block, sort_description))
        {
            stableGetPermutation(block, sort_description, perm);
            perm_ptr = &perm;
        }
        else
            ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterBlocksAlreadySorted);
    }

    if (metadata_snapshot->hasRowsTTL())
        updateTTL(metadata_snapshot->getRowsTTL(), new_data_part->ttl_infos, new_data_part->ttl_infos.table_ttl, block, true);

    for (const auto & [name, ttl_entry] : metadata_snapshot->getColumnTTLs())
        updateTTL(ttl_entry, new_data_part->ttl_infos, new_data_part->ttl_infos.columns_ttl[name], block, true);

    new_data_part->ttl_infos.update(move_ttl_infos);

    /// This effectively chooses minimal compression method:
    ///  either default lz4 or compression method with zero thresholds on absolute and relative part size.
    auto compression_codec = data.global_context.chooseCompressionCodec(0, 0);

    const auto & index_factory = MergeTreeIndexFactory::instance();
    MergedBlockOutputStream out(new_data_part, metadata_snapshot, columns, index_factory.getMany(metadata_snapshot->getSecondaryIndices()), compression_codec);

    out.writePrefix();
    out.writeWithPermutation(block, perm_ptr);
    out.writeSuffixAndFinalizePart(new_data_part);

    ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterRows, block.rows());
    ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterUncompressedBytes, block.bytes());
    ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterCompressedBytes, new_data_part->getBytesOnDisk());

    return new_data_part;
}

}
