#include <Storages/MergeTree/PartBitmap.h>

namespace DB
{


void PartBitmap::write()
{
    String path = data_part->getDataPartStoragePtr()->getFullPath();
    String tmp_path = path + "bitmap.bin.tmp";
    {
        WriteBufferFromFile out(tmp_path);
        bitmap->write(out);
        writeVarUInt(seq_id, out);
        writeVarUInt(update_seq_id, out);
    }
    fs::rename(tmp_path, path + "bitmap.bin");
}

void PartBitmap::read()
{
    String path = fs::path(data_part->getDataPartStoragePtr()->getFullPath()) / "bitmap.bin";
    ReadBufferFromFile in(path);
    bitmap->read(in);
    readVarUInt(seq_id, in);
    readVarUInt(update_seq_id, in);
}


}
