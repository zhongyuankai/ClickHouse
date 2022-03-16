#include "UniqueMergeTreeDataMergerMutator.h"

#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>
#include <Disks/StoragePolicy.h>
#include <Storages/MergeTree/SimpleMergeSelector.h>
#include <Storages/MergeTree/AllMergeSelector.h>
#include <Storages/MergeTree/TTLMergeSelector.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/StorageFromMergeTreeDataPart.h>
#include <DataStreams/TTLBlockInputStream.h>
#include <DataStreams/DistinctSortedBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/ColumnGathererStream.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Merges/CollapsingSortedTransform.h>
#include <Processors/Merges/SummingSortedTransform.h>
#include <Processors/Merges/ReplacingSortedTransform.h>
#include <Processors/Merges/GraphiteRollupSortedTransform.h>
#include <Processors/Merges/AggregatingSortedTransform.h>
#include <Processors/Merges/VersionedCollapsingTransform.h>
#include <Processors/Merges/UniqueSortedTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Executors/PipelineExecutingBlockInputStream.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/Context.h>
#include <Common/interpolate.h>
#include <Common/typeid_cast.h>
#include <Common/escapeForFileName.h>

#include <cmath>
#include <ctime>
#include <numeric>

#include <boost/algorithm/string/replace.hpp>


namespace ProfileEvents
{
    extern const Event MergedRows;
    extern const Event MergedUncompressedBytes;
    extern const Event MergesTimeMilliseconds;
    extern const Event Merge;
}

namespace CurrentMetrics
{
    extern const Metric BackgroundPoolTask;
    extern const Metric PartMutation;
}

namespace DB {

namespace ErrorCodes {
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int LOGICAL_ERROR;
    extern const int ABORTED;
}


using MergeAlgorithm = MergeTreeDataMergerMutator::MergeAlgorithm;

UniqueMergeTreeDataMergerMutator::UniqueMergeTreeDataMergerMutator(
    StorageReplicatedUniqueMergeTree & data_, size_t background_pool_size_) :
    MergeTreeDataMergerMutator(data_, background_pool_size_), storage_data(data_)
{

}

MergeTreeData::MutableDataPartPtr UniqueMergeTreeDataMergerMutator::mergePartsToTemporaryPart(
    const FutureMergedMutatedPart & future_part,
    const StorageMetadataPtr & metadata_snapshot,
    MergeList::Entry & merge_entry,
    TableLockHolder &,
    time_t time_of_merge,
    const ReservationPtr & space_reservation,
    bool deduplicate)
{
    static const String TMP_PREFIX = "tmp_merge_";

    if (merges_blocker.isCancelled())
        throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);

    const MergeTreeData::DataPartsVector & parts = future_part.parts;
    std::vector<PartBitmap::Ptr> parts_bitmaps;
    MergeTreeData::DataPartsVector partition_parts = storage_data.getDataPartsVectorInPartition(
        MergeTreeData::DataPartState::Committed, parts.front()->info.partition_id);
    MergeTreeData::DataPartsVector select_parts = parts;
    storage_data.mergePartsBitmaps(partition_parts, select_parts, parts_bitmaps);

    LOG_DEBUG(log, "Merging {} parts: from {} to {} into {}", future_part.parts.size(), future_part.parts.front()->name, future_part.parts.back()->name, future_part.type.toString());

    auto disk = space_reservation->getDisk();
    String part_path = data.relative_data_path;
    String new_part_tmp_path = part_path + TMP_PREFIX + future_part.name + "/";
    if (disk->exists(new_part_tmp_path))
        throw Exception("Directory " + fullPath(disk, new_part_tmp_path) + " already exists", ErrorCodes::DIRECTORY_ALREADY_EXISTS);


    Names all_column_names = metadata_snapshot->getColumns().getNamesOfPhysical();
    NamesAndTypesList storage_columns = metadata_snapshot->getColumns().getAllPhysical();
    for (const NameAndTypePair &name_type : metadata_snapshot->internal_columns) {
        all_column_names.push_back(name_type.name);
        storage_columns.push_back(name_type);
    }
    const auto data_settings = data.getSettings();

    NamesAndTypesList gathering_columns;
    NamesAndTypesList merging_columns;
    Names gathering_column_names, merging_column_names;
    extractMergingAndGatheringColumns(
        storage_columns,
        metadata_snapshot->getSortingKey().expression,
        metadata_snapshot->getSecondaryIndices(),
        data.merging_params,
        gathering_columns,
        gathering_column_names,
        merging_columns,
        merging_column_names);

    auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + future_part.name, disk);
    MergeTreeData::MutableDataPartPtr new_data_part = data.createPart(
        future_part.name,
        future_part.type,
        future_part.part_info,
        single_disk_volume,
        TMP_PREFIX + future_part.name);

    new_data_part->setColumns(storage_columns);
    new_data_part->partition.assign(future_part.getPartition());
    new_data_part->is_temp = true;

    bool need_remove_expired_values = false;
    for (const auto & part : parts)
        new_data_part->ttl_infos.update(part->ttl_infos);

    const auto & part_min_ttl = new_data_part->ttl_infos.part_min_ttl;
    if (part_min_ttl && part_min_ttl <= time_of_merge)
        need_remove_expired_values = true;

    if (need_remove_expired_values && ttl_merges_blocker.isCancelled())
    {
        LOG_INFO(log, "Part {} has values with expired TTL, but merges with TTL are cancelled.", new_data_part->name);
        need_remove_expired_values = false;
    }

    size_t sum_input_rows_upper_bound = merge_entry->total_rows_count;
    MergeAlgorithm merge_alg = chooseMergeAlgorithm(parts, sum_input_rows_upper_bound, gathering_columns, deduplicate, need_remove_expired_values);

    LOG_DEBUG(log, "Selected MergeAlgorithm: {}", ((merge_alg == MergeAlgorithm::Vertical) ? "Vertical" : "Horizontal"));

    /// Note: this is done before creating input streams, because otherwise data.data_parts_mutex
    /// (which is locked in data.getTotalActiveSizeInBytes())
    /// (which is locked in shared mode when input streams are created) and when inserting new data
    /// the order is reverse. This annoys TSan even though one lock is locked in shared mode and thus
    /// deadlock is impossible.
    auto compression_codec = data.global_context.chooseCompressionCodec(
        merge_entry->total_size_bytes_compressed,
        static_cast<double> (merge_entry->total_size_bytes_compressed) / data.getTotalActiveSizeInBytes());

    /// TODO: Should it go through IDisk interface?
    String rows_sources_file_path;
    std::unique_ptr<WriteBufferFromFileBase> rows_sources_uncompressed_write_buf;
    std::unique_ptr<WriteBuffer> rows_sources_write_buf;
    std::optional<ColumnSizeEstimator> column_sizes;

    if (merge_alg == MergeAlgorithm::Vertical)
    {
        disk->createDirectories(new_part_tmp_path);
        rows_sources_file_path = new_part_tmp_path + "rows_sources";
        rows_sources_uncompressed_write_buf = disk->writeFile(rows_sources_file_path);
        rows_sources_write_buf = std::make_unique<CompressedWriteBuffer>(*rows_sources_uncompressed_write_buf);

        MergeTreeData::DataPart::ColumnToSize merged_column_to_size;
        for (const MergeTreeData::DataPartPtr & part : parts)
            part->accumulateColumnSizes(merged_column_to_size);

        column_sizes = ColumnSizeEstimator(merged_column_to_size, merging_column_names, gathering_column_names);
    }
    else
    {
        merging_columns = storage_columns;
        merging_column_names = all_column_names;
        gathering_columns.clear();
        gathering_column_names.clear();
    }

    /** Read from all parts, merge and write into a new one.
      * In passing, we calculate expression for sorting.
      */
    Pipes pipes;
    UInt64 watch_prev_elapsed = 0;

    /// We count total amount of bytes in parts
    /// and use direct_io + aio if there is more than min_merge_bytes_to_use_direct_io
    bool read_with_direct_io = false;
    if (data_settings->min_merge_bytes_to_use_direct_io != 0)
    {
        size_t total_size = 0;
        for (const auto & part : parts)
        {
            total_size += part->getBytesOnDisk();
            if (total_size >= data_settings->min_merge_bytes_to_use_direct_io)
            {
                LOG_DEBUG(log, "Will merge parts reading files in O_DIRECT");
                read_with_direct_io = true;

                break;
            }
        }
    }

    MergeStageProgress horizontal_stage_progress(
        column_sizes ? column_sizes->keyColumnsWeight() : 1.0);

    for (const auto & part : parts)
    {
        auto input = std::make_unique<MergeTreeSequentialSource>(
            data, metadata_snapshot, part, merging_column_names, read_with_direct_io, true);

        input->setProgressCallback(
            MergeProgressCallback(merge_entry, watch_prev_elapsed, horizontal_stage_progress));

        Pipe pipe(std::move(input));

        if (metadata_snapshot->hasSortingKey())
        {
            pipe.addSimpleTransform([&metadata_snapshot](const Block & header)
            {
                return std::make_shared<ExpressionTransform>(header, metadata_snapshot->getSortingKey().expression);
            });
        }

        pipes.emplace_back(std::move(pipe));
    }

    Names sort_columns = metadata_snapshot->getSortingKeyColumns();
    SortDescription sort_description;
    size_t sort_columns_size = sort_columns.size();
    sort_description.reserve(sort_columns_size);

    Names partition_key_columns = metadata_snapshot->getPartitionKey().column_names;

    Block header = pipes.at(0).getHeader();
    for (size_t i = 0; i < sort_columns_size; ++i)
        sort_description.emplace_back(header.getPositionByName(sort_columns[i]), 1, 1);

    /// The order of the streams is important: when the key is matched, the elements go in the order of the source stream number.
    /// In the merged part, the lines with the same key must be in the ascending order of the identifier of original part,
    ///  that is going in insertion order.
    ProcessorPtr merged_transform;

    /// If merge is vertical we cannot calculate it
    bool blocks_are_granules_size = (merge_alg == MergeAlgorithm::Vertical);

    UInt64 merge_block_size = data_settings->merge_max_block_size;
    switch (data.merging_params.mode)
    {
        case MergeTreeData::MergingParams::Ordinary:
            merged_transform = std::make_unique<MergingSortedTransform>(
                header, pipes.size(), sort_description, merge_block_size, 0, rows_sources_write_buf.get(), true, blocks_are_granules_size);
            break;

        case MergeTreeData::MergingParams::Collapsing:
            merged_transform = std::make_unique<CollapsingSortedTransform>(
                header, pipes.size(), sort_description, data.merging_params.sign_column, false,
                merge_block_size, rows_sources_write_buf.get(), blocks_are_granules_size);
            break;

        case MergeTreeData::MergingParams::Summing:
            merged_transform = std::make_unique<SummingSortedTransform>(
                header, pipes.size(), sort_description, data.merging_params.columns_to_sum, partition_key_columns, merge_block_size);
            break;

        case MergeTreeData::MergingParams::Aggregating:
            merged_transform = std::make_unique<AggregatingSortedTransform>(
                header, pipes.size(), sort_description, merge_block_size);
            break;

        case MergeTreeData::MergingParams::Replacing:
            merged_transform = std::make_unique<ReplacingSortedTransform>(
                header, pipes.size(), sort_description, data.merging_params.version_column,
                merge_block_size, rows_sources_write_buf.get(), blocks_are_granules_size);
            break;

        case MergeTreeData::MergingParams::Graphite:
            merged_transform = std::make_unique<GraphiteRollupSortedTransform>(
                header, pipes.size(), sort_description, merge_block_size,
                data.merging_params.graphite_params, time_of_merge);
            break;

        case MergeTreeData::MergingParams::VersionedCollapsing:
            merged_transform = std::make_unique<VersionedCollapsingTransform>(
                header, pipes.size(), sort_description, data.merging_params.sign_column,
                merge_block_size, rows_sources_write_buf.get(), blocks_are_granules_size);
            break;

        case MergeTreeData::MergingParams::Unique:
            merged_transform = std::make_unique<UniqueSortedTransform>(
                header, pipes.size(), sort_description, merge_block_size, 0, rows_sources_write_buf.get(), true,
                blocks_are_granules_size, true, parts_bitmaps, header.getPositionByName("_unique_key_id"));
            break;
    }

    QueryPipeline pipeline;
    pipeline.init(Pipe::unitePipes(std::move(pipes)));
    pipeline.addTransform(std::move(merged_transform));
    pipeline.setMaxThreads(1);
    BlockInputStreamPtr merged_stream = std::make_shared<PipelineExecutingBlockInputStream>(std::move(pipeline));

    if (deduplicate)
        merged_stream = std::make_shared<DistinctSortedBlockInputStream>(merged_stream, sort_description, SizeLimits(), 0 /*limit_hint*/, Names());

    if (need_remove_expired_values)
        merged_stream = std::make_shared<TTLBlockInputStream>(merged_stream, data, metadata_snapshot, new_data_part, time_of_merge, false);


    if (metadata_snapshot->hasSecondaryIndices())
    {
        const auto & indices = metadata_snapshot->getSecondaryIndices();
        merged_stream = std::make_shared<ExpressionBlockInputStream>(merged_stream, indices.getSingleExpressionForIndices(metadata_snapshot->getColumns(), data.global_context));
        merged_stream = std::make_shared<MaterializingBlockInputStream>(merged_stream);
    }

    const auto & index_factory = MergeTreeIndexFactory::instance();
    MergedBlockOutputStream to{
        new_data_part,
        metadata_snapshot,
        merging_columns,
        index_factory.getMany(metadata_snapshot->getSecondaryIndices()),
        compression_codec,
        data_settings->min_merge_bytes_to_use_direct_io,
        blocks_are_granules_size};

    merged_stream->readPrefix();
    to.writePrefix();

    size_t rows_written = 0;
    const size_t initial_reservation = space_reservation ? space_reservation->getSize() : 0;

    auto is_cancelled = [&]() { return merges_blocker.isCancelled()
        || (need_remove_expired_values && ttl_merges_blocker.isCancelled()); };

    Block block;
    while (!is_cancelled() && (block = merged_stream->read()))
    {
        rows_written += block.rows();

        to.write(block);

        merge_entry->rows_written = merged_stream->getProfileInfo().rows;
        merge_entry->bytes_written_uncompressed = merged_stream->getProfileInfo().bytes;

        /// Reservation updates is not performed yet, during the merge it may lead to higher free space requirements
        if (space_reservation && sum_input_rows_upper_bound)
        {
            /// The same progress from merge_entry could be used for both algorithms (it should be more accurate)
            /// But now we are using inaccurate row-based estimation in Horizontal case for backward compatibility
            Float64 progress = (merge_alg == MergeAlgorithm::Horizontal)
                ? std::min(1., 1. * rows_written / sum_input_rows_upper_bound)
                : std::min(1., merge_entry->progress.load(std::memory_order_relaxed));

            space_reservation->update(static_cast<size_t>((1. - progress) * initial_reservation));
        }
    }

    merged_stream->readSuffix();
    merged_stream.reset();

    if (merges_blocker.isCancelled())
        throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);

    if (need_remove_expired_values && ttl_merges_blocker.isCancelled())
        throw Exception("Cancelled merging parts with expired TTL", ErrorCodes::ABORTED);

    MergeTreeData::DataPart::Checksums checksums_gathered_columns;

    /// Gather ordinary columns
    if (merge_alg == MergeAlgorithm::Vertical)
    {
        size_t sum_input_rows_exact = merge_entry->rows_read;
        merge_entry->columns_written = merging_column_names.size();
        merge_entry->progress.store(column_sizes->keyColumnsWeight(), std::memory_order_relaxed);

        BlockInputStreams column_part_streams(parts.size());

        auto it_name_and_type = gathering_columns.cbegin();

        rows_sources_write_buf->next();
        rows_sources_uncompressed_write_buf->next();
        /// Ensure data has written to disk.
        rows_sources_uncompressed_write_buf->finalize();

        size_t rows_sources_count = rows_sources_write_buf->count();
        /// In special case, when there is only one source part, and no rows were skipped, we may have
        /// skipped writing rows_sources file. Otherwise rows_sources_count must be equal to the total
        /// number of input rows.
        if ((rows_sources_count > 0 || parts.size() > 1) && sum_input_rows_exact != rows_sources_count)
            throw Exception("Number of rows in source parts (" + toString(sum_input_rows_exact)
                + ") differs from number of bytes written to rows_sources file (" + toString(rows_sources_count)
                + "). It is a bug.", ErrorCodes::LOGICAL_ERROR);

        CompressedReadBufferFromFile rows_sources_read_buf(disk->readFile(rows_sources_file_path));
        IMergedBlockOutputStream::WrittenOffsetColumns written_offset_columns;

        for (size_t column_num = 0, gathering_column_names_size = gathering_column_names.size();
            column_num < gathering_column_names_size;
            ++column_num, ++it_name_and_type)
        {
            const String & column_name = it_name_and_type->name;
            Names column_names{column_name};
            Float64 progress_before = merge_entry->progress.load(std::memory_order_relaxed);

            MergeStageProgress column_progress(progress_before, column_sizes->columnWeight(column_name));
            for (size_t part_num = 0; part_num < parts.size(); ++part_num)
            {
                auto column_part_source = std::make_shared<MergeTreeSequentialSource>(
                    data, metadata_snapshot, parts[part_num], column_names, read_with_direct_io, true);

                column_part_source->setProgressCallback(
                    MergeProgressCallback(merge_entry, watch_prev_elapsed, column_progress));

                QueryPipeline column_part_pipeline;
                column_part_pipeline.init(Pipe(std::move(column_part_source)));
                column_part_pipeline.setMaxThreads(1);

                column_part_streams[part_num] =
                        std::make_shared<PipelineExecutingBlockInputStream>(std::move(column_part_pipeline));
            }

            rows_sources_read_buf.seek(0, 0);
            ColumnGathererStream column_gathered_stream(column_name, column_part_streams, rows_sources_read_buf);

            MergedColumnOnlyOutputStream column_to(
                new_data_part,
                metadata_snapshot,
                column_gathered_stream.getHeader(),
                compression_codec,
                /// we don't need to recalc indices here
                /// because all of them were already recalculated and written
                /// as key part of vertical merge
                std::vector<MergeTreeIndexPtr>{},
                &written_offset_columns,
                to.getIndexGranularity());

            size_t column_elems_written = 0;

            column_to.writePrefix();
            while (!merges_blocker.isCancelled() && (block = column_gathered_stream.read()))
            {
                column_elems_written += block.rows();
                column_to.write(block);
            }

            if (merges_blocker.isCancelled())
                throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);

            column_gathered_stream.readSuffix();
            auto changed_checksums = column_to.writeSuffixAndGetChecksums(new_data_part, checksums_gathered_columns);
            checksums_gathered_columns.add(std::move(changed_checksums));

            if (rows_written != column_elems_written)
            {
                throw Exception("Written " + toString(column_elems_written) + " elements of column " + column_name +
                                ", but " + toString(rows_written) + " rows of PK columns", ErrorCodes::LOGICAL_ERROR);
            }

            /// NOTE: 'progress' is modified by single thread, but it may be concurrently read from MergeListElement::getInfo() (StorageSystemMerges).

            merge_entry->columns_written += 1;
            merge_entry->bytes_written_uncompressed += column_gathered_stream.getProfileInfo().bytes;
            merge_entry->progress.store(progress_before + column_sizes->columnWeight(column_name), std::memory_order_relaxed);
        }

        disk->remove(rows_sources_file_path);
    }

    for (const auto & part : parts)
        new_data_part->minmax_idx.merge(part->minmax_idx);


    /// Print overall profiling info. NOTE: it may duplicates previous messages
    {
        double elapsed_seconds = merge_entry->watch.elapsedSeconds();
        LOG_DEBUG(log,
            "Merge sorted {} rows, containing {} columns ({} merged, {} gathered) in {} sec., {} rows/sec., {}/sec.",
            merge_entry->rows_read,
            all_column_names.size(),
            merging_column_names.size(),
            gathering_column_names.size(),
            elapsed_seconds,
            merge_entry->rows_read / elapsed_seconds,
            ReadableSize(merge_entry->bytes_read_uncompressed / elapsed_seconds));
    }

    if (merge_alg != MergeAlgorithm::Vertical)
        to.writeSuffixAndFinalizePart(new_data_part);
    else
        to.writeSuffixAndFinalizePart(new_data_part, &storage_columns, &checksums_gathered_columns);

    BitmapPtr new_bitmap = std::make_shared<Bitmap>();
    for (size_t i = 0; i<parts.size(); i++) {
        PartBitmap::Ptr part_bitmap = storage_data.getPartBitmap(parts[i]);
        new_bitmap->rb_or(*part_bitmap->bitmap);
    }
    PartBitmap::MutablePtr part_bitmap = PartBitmap::create(new_bitmap, new_data_part, storage_data.getPartBitmap(parts[0])->seq_id);
    part_bitmap->write(part_bitmap->seq_id);

    return new_data_part;
}

MergeTreeData::MutableDataPartPtr UniqueMergeTreeDataMergerMutator::mutatePartToTemporaryPart(
    const FutureMergedMutatedPart & future_part,
    const StorageMetadataPtr & metadata_snapshot,
    const MutationCommands & commands,
    MergeListEntry & merge_entry,
    time_t time_of_mutation,
    const Context & context,
    const ReservationPtr & space_reservation,
    TableLockHolder &)
{
    checkOperationIsNotCanceled(merge_entry);

    if (future_part.parts.size() != 1)
        throw Exception("Trying to mutate " + toString(future_part.parts.size()) + " parts, not one. "
            "This is a bug.", ErrorCodes::LOGICAL_ERROR);

    CurrentMetrics::Increment num_mutations{CurrentMetrics::PartMutation};
    const auto & source_part = future_part.parts[0];
    auto storage_from_source_part = StorageFromMergeTreeDataPart::create(source_part);

    auto context_for_reading = context;
    context_for_reading.setSetting("max_streams_to_max_threads_ratio", 1);
    context_for_reading.setSetting("max_threads", 1);

    MutationCommands commands_for_part;
    for (const auto & command : commands)
    {
        if (command.partition == nullptr || future_part.parts[0]->info.partition_id == data.getPartitionIDFromQuery(
            command.partition, context_for_reading))
            commands_for_part.emplace_back(command);
    }

    if (source_part->isStoredOnDisk() && !isStorageTouchedByMutations(
        storage_from_source_part, metadata_snapshot, commands_for_part, context_for_reading))
    {
        LOG_TRACE(log, "Part {} doesn't change up to mutation version {}", source_part->name, future_part.part_info.mutation);
        return data.cloneAndLoadDataPartOnSameDisk(source_part, "tmp_clone_", future_part.part_info, metadata_snapshot);
    }
    else
    {
        LOG_TRACE(log, "Mutating part {} to mutation version {}", source_part->name, future_part.part_info.mutation);
    }

    BlockInputStreamPtr in = nullptr;
    Block updated_header;
    std::unique_ptr<MutationsInterpreter> interpreter;

    const auto data_settings = data.getSettings();
    MutationCommands for_interpreter;
    MutationCommands for_file_renames;

    splitMutationCommands(source_part, commands_for_part, for_interpreter, for_file_renames);

    UInt64 watch_prev_elapsed = 0;
    MergeStageProgress stage_progress(1.0);

    NamesAndTypesList storage_columns = metadata_snapshot->getColumns().getAllPhysical();
    storage_columns.insert(storage_columns.begin(), metadata_snapshot->internal_columns.begin(),
                           metadata_snapshot->internal_columns.end());

    if (!for_interpreter.empty())
    {
        interpreter = std::make_unique<MutationsInterpreter>(
            storage_from_source_part, metadata_snapshot, for_interpreter, context_for_reading, true);
        in = interpreter->execute();
        updated_header = interpreter->getUpdatedHeader();
        in->setProgressCallback(MergeProgressCallback(merge_entry, watch_prev_elapsed, stage_progress));
    }

    auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + future_part.name, space_reservation->getDisk());
    auto new_data_part = data.createPart(
        future_part.name, future_part.type, future_part.part_info, single_disk_volume, "tmp_mut_" + future_part.name);

    new_data_part->is_temp = true;
    new_data_part->ttl_infos = source_part->ttl_infos;

    /// It shouldn't be changed by mutation.
    new_data_part->index_granularity_info = source_part->index_granularity_info;
    new_data_part->setColumns(getColumnsForNewDataPart(source_part, updated_header, storage_columns, for_file_renames));
    new_data_part->partition.assign(source_part->partition);

    auto disk = new_data_part->volume->getDisk();
    String new_part_tmp_path = new_data_part->getFullRelativePath();

    /// Note: this is done before creating input streams, because otherwise data.data_parts_mutex
    /// (which is locked in data.getTotalActiveSizeInBytes())
    /// (which is locked in shared mode when input streams are created) and when inserting new data
    /// the order is reverse. This annoys TSan even though one lock is locked in shared mode and thus
    /// deadlock is impossible.
    auto compression_codec = context.chooseCompressionCodec(
        source_part->getBytesOnDisk(),
        static_cast<double>(source_part->getBytesOnDisk()) / data.getTotalActiveSizeInBytes());

    disk->createDirectories(new_part_tmp_path);

    /// Don't change granularity type while mutating subset of columns
    auto mrk_extension = source_part->index_granularity_info.is_adaptive ? getAdaptiveMrkExtension(new_data_part->getType())
                                                                         : getNonAdaptiveMrkExtension();

    bool need_remove_expired_values = false;

    if (in && shouldExecuteTTL(metadata_snapshot, in->getHeader().getNamesAndTypesList().getNames(), commands_for_part))
        need_remove_expired_values = true;

    if (ttl_merges_blocker.isCancelled()) {
        need_remove_expired_values = false;
    }

    if (commands.size() == 1 && commands[0].type == MutationCommand::FAST_MATERIALIZE_TTL)
    {
        ///get the files that will be changed in FAST_MATERIALIZE_TTL
        NameSet files_to_skip;
        files_to_skip.insert("ttl.txt");
        files_to_skip.insert("checksums.txt");

        /// Create hardlinks for unchanged files
        for (auto it = disk->iterateDirectory(source_part->getFullRelativePath()); it->isValid(); it->next())
        {
            if (files_to_skip.count(it->name()))
                continue;

            String destination = new_part_tmp_path + "/";
            destination += it->name();
            disk->createHardLink(it->path(), destination);
        }

        /// get the checksums of source_part
        new_data_part->checksums = source_part->checksums;

        ///get delta  and   update the ttl_info
        int delta = commands[0].ttl_delta;
        new_data_part->ttl_infos.table_ttl.min += delta;
        new_data_part->ttl_infos.table_ttl.max += delta;
        finalizeMutatedPart(source_part, new_data_part, true);

        return new_data_part;
    }
    /// All columns from part are changed and may be some more that were missing before in part
    if (!isWidePart(source_part) || (interpreter && interpreter->isAffectingAllColumns()))
    {
        auto part_indices = getIndicesForNewDataPart(metadata_snapshot->getSecondaryIndices(), for_file_renames);
        mutateAllPartColumns(
            new_data_part,
            metadata_snapshot,
            part_indices,
            in,
            time_of_mutation,
            compression_codec,
            merge_entry,
            need_remove_expired_values);

        /// no finalization required, because mutateAllPartColumns use
        /// MergedBlockOutputStream which finilaze all part fields itself

        for (auto it = disk->iterateDirectory(source_part->getFullRelativePath()); it->isValid(); it->next()) {
            if (it->name() == "bitmap.bin") {
                String destination = new_part_tmp_path + "/";
                destination += it->name();
                disk->createHardLink(it->path(), destination);
            }
        }
    }
    else /// TODO: check that we modify only non-key columns in this case.
    {
        /// We will modify only some of the columns. Other columns and key values can be copied as-is.
        auto indices_to_recalc = getIndicesToRecalculate(in, updated_header.getNamesAndTypesList(), metadata_snapshot, context);

        NameSet files_to_skip = collectFilesToSkip(updated_header, indices_to_recalc, mrk_extension);
        NameToNameVector files_to_rename = collectFilesForRenames(source_part, for_file_renames, mrk_extension);

        if (need_remove_expired_values)
            files_to_skip.insert("ttl.txt");

        /// Create hardlinks for unchanged files
        for (auto it = disk->iterateDirectory(source_part->getFullRelativePath()); it->isValid(); it->next())
        {
            if (files_to_skip.count(it->name()))
                continue;

            String destination = new_part_tmp_path + "/";
            String file_name = it->name();
            auto rename_it = std::find_if(files_to_rename.begin(), files_to_rename.end(), [&file_name](const auto & rename_pair) { return rename_pair.first == file_name; });
            if (rename_it != files_to_rename.end())
            {
                if (rename_it->second.empty())
                    continue;
                destination += rename_it->second;
            }
            else
            {
                destination += it->name();
            }

            disk->createHardLink(it->path(), destination);
        }

        merge_entry->columns_written = storage_columns.size() - updated_header.columns();

        new_data_part->checksums = source_part->checksums;

        if (in) {
            mutateSomePartColumns(
                source_part,
                metadata_snapshot,
                indices_to_recalc,
                updated_header,
                new_data_part,
                in,
                time_of_mutation,
                compression_codec,
                merge_entry,
                need_remove_expired_values);
        }

        for (const auto & [rename_from, rename_to] : files_to_rename) {
            if (rename_to.empty() && new_data_part->checksums.files.count(rename_from))
            {
                new_data_part->checksums.files.erase(rename_from);
            }
            else if (new_data_part->checksums.files.count(rename_from))
            {
                new_data_part->checksums.files[rename_to] = new_data_part->checksums.files[rename_from];

                new_data_part->checksums.files.erase(rename_from);
            }
        }

        finalizeMutatedPart(source_part, new_data_part, need_remove_expired_values);
    }

    return new_data_part;
}

}
