#pragma once

#include <unordered_set>
#include <unordered_map>

#include <base/types.h>
#include <Core/QualifiedTableName.h>

namespace DB
{

namespace ErrorCodes
{
}

class ReadBuffer;
class WriteBuffer;


/// The following are request-response messages for TablesStatus request of the client-server protocol.
/// Client can ask for about a set of tables and the server will respond with the following information for each table:
/// - Is the table Replicated?
/// - If yes, replication delay for that table.
///
/// For nonexistent tables there will be no TableStatus entry in the response.

struct TableStatus
{
    /// 0 bit is_replicated
    /// 1 bit is_leader
    /// 2-7 bit reserved
    uint8_t replica_status = 0;
    UInt32 absolute_delay = 0;
    void write(WriteBuffer & out) const;
    void read(ReadBuffer & in);

    bool isReplicated() const { return replica_status > 0; }
    bool isLeader() const { return replica_status == 0b00000011; }
};

struct TablesStatusRequest
{
    std::unordered_set<QualifiedTableName> tables;

    void write(WriteBuffer & out, UInt64 server_protocol_revision) const;
    void read(ReadBuffer & in, UInt64 client_protocol_revision);
};

struct TablesStatusResponse
{
    std::unordered_map<QualifiedTableName, TableStatus> table_states_by_id;

    void write(WriteBuffer & out, UInt64 client_protocol_revision) const;
    void read(ReadBuffer & in, UInt64 server_protocol_revision);
};

}
