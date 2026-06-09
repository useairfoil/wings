use std::{io::Cursor, net::Ipv4Addr};

use binrw::{BinRead, BinWrite, NullString};
use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, Utc};
use prost_types::Any;
use uuid::Uuid;

pub const MANIFEST_VERSION: u8 = 1;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, BinRead, BinWrite)]
#[brw(repr = u8)]
pub enum CompressionType {
    #[default]
    None = 0,
    Zstd = 1,
}

/// The queue manifest.
///
/// The manifest is split in two parts: the header and the data.
/// The header is always stored uncompressed at the beginning of the manifest file.
/// The data is possibly compressed.
///
/// ```text
/// +------------------------------------------------+
/// | header (16 bytes):                             |
/// |   version: u8 (= 1)                            |
/// |   broker_address: u32 LE                       |
/// |   broker_port: u16 LE                          |
/// |   broker_epoch: u64 LE                         |
/// |   compression: u8 (0 = none, 1 = zstd)         |
/// +------------------------------------------------+
/// | task data (variable len, aligned):             |
/// |   magic: [u8; 4] = "TASK"                      |
/// |   entries_count: u32 LE                        |
/// |   entry 0: [data]                              |
/// |   entry 1: [data]                              |
/// |   ...                                          |
/// +------------------------------------------------+
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub header: Header,
    pub entries: Vec<Task>,
}

/// Manifest header.
///
/// The header contains information about the manifest itself and the most recent broker.
#[derive(Debug, Clone, PartialEq, Eq, BinRead, BinWrite)]
#[brw(little, magic = 1u8)]
pub struct Header {
    /// The IP address of the most recent broker.
    #[br(parse_with = helpers::ipv4_addr_parser)]
    #[bw(write_with = helpers::ipv4_addr_writer)]
    pub address: Ipv4Addr,

    /// The port of the most recent broker.
    pub port: u16,

    /// The epoch of the most recent broker.
    pub epoch: u64,

    /// The compression type used for the entries.
    pub compression: CompressionType,
}

/// Internal struct used to read/write task data.
#[binrw::binrw]
#[brw(little, magic = b"TASK")]
struct TaskData {
    #[bw(try_calc(u32::try_from(entries.len())))]
    entries_count: u32,
    #[br(count = entries_count)]
    #[bw(align_after = 0x8)]
    pub entries: Vec<Task>,
}

impl TaskData {
    pub(crate) fn to_bytes(&self) -> Result<Bytes, binrw::Error> {
        let mut buf = Cursor::new(Vec::new());
        self.write(&mut buf)?;

        Ok(Bytes::from(buf.into_inner()))
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, binrw::Error> {
        let mut cursor = Cursor::new(bytes);
        Self::read(&mut cursor)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, BinRead, BinWrite)]
#[brw(little)]
pub enum TaskStatus {
    /// The task has been created but not yet scheduled for execution.
    #[default]
    #[brw(magic = 0u8)]
    Created,
    /// The task is currently being executed.
    #[brw(magic = 1u8)]
    Running {
        /// The worker that is currently executing the task.
        worker: Worker,

        /// The time at which the lease expires.
        #[br(parse_with = helpers::dt_utc_parser)]
        #[bw(write_with = helpers::dt_utc_writer)]
        lease_expires_at: DateTime<Utc>,
    },
    /// The task has failed.
    #[brw(magic = 2u8)]
    Failed {
        /// Human-readable error message.
        error_message: NullString,

        /// Machine-readable error details.
        #[br(parse_with = helpers::any_parser)]
        #[bw(write_with = helpers::any_writer)]
        details: Any,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, BinRead, BinWrite)]
#[brw(little)]
pub struct Task {
    /// The unique identifier of the task.
    #[br(parse_with = helpers::uuid_parser)]
    #[bw(write_with = helpers::uuid_writer)]
    pub id: Uuid,

    /// The time at which the task was originally scheduled.
    ///
    /// Used to track the task completion latency.
    #[br(parse_with = helpers::dt_utc_parser)]
    #[bw(write_with = helpers::dt_utc_writer)]
    pub originally_scheduled_at: DateTime<Utc>,

    /// The time at which the task is scheduled to run.
    #[br(parse_with = helpers::dt_utc_parser)]
    #[bw(write_with = helpers::dt_utc_writer)]
    pub scheduled_at: DateTime<Utc>,

    /// The maximum number of retries allowed for this task.
    pub max_retries: u16,

    /// The number of times this task has been retried.
    pub retry_count: u16,

    /// The current status of the task.
    pub status: TaskStatus,

    /// The serialized payload of the task.
    #[br(parse_with = helpers::any_parser)]
    #[bw(write_with = helpers::any_writer)]
    pub payload: Any,
}

impl Task {
    #[allow(unused)]
    pub(crate) fn to_bytes(&self) -> Result<Bytes, binrw::Error> {
        let mut buf = Cursor::new(Vec::new());
        self.write(&mut buf)?;

        Ok(Bytes::from(buf.into_inner()))
    }

    #[allow(unused)]
    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, binrw::Error> {
        let mut cursor = Cursor::new(bytes);
        Self::read(&mut cursor)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BinRead, BinWrite)]
#[brw(little)]
pub struct Worker {
    /// The unique identifier of the worker.
    #[br(parse_with = helpers::uuid_parser)]
    #[bw(write_with = helpers::uuid_writer)]
    pub id: Uuid,

    /// The last heartbeat timestamp of the worker.
    #[br(parse_with = helpers::dt_utc_parser)]
    #[bw(write_with = helpers::dt_utc_writer)]
    pub last_heartbeat: DateTime<Utc>,
}

impl Manifest {
    pub fn into_bytes(self) -> Result<Bytes, binrw::Error> {
        let header = self.header.to_bytes()?;

        let data = TaskData {
            entries: self.entries,
        }
        .to_bytes()?;

        let data = match self.header.compression {
            CompressionType::None => data,
            CompressionType::Zstd => {
                Bytes::from(zstd::bulk::compress(&data, 0).map_err(binrw::Error::Io)?)
            }
        };

        let mut bytes = BytesMut::with_capacity(data.len() + Header::SIZE);

        bytes.put(header);
        bytes.put(data);

        Ok(bytes.freeze())
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, binrw::Error> {
        if bytes.len() < Header::SIZE {
            return Err(binrw::Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "manifest header missing",
            )));
        }

        let (header, data) = bytes.split_at(Header::SIZE);
        let header = Header::from_bytes(header)?;

        let data = match header.compression {
            CompressionType::None => data.to_vec(),
            CompressionType::Zstd => {
                zstd::stream::decode_all(data.as_ref()).map_err(binrw::Error::Io)?
            }
        };

        let data = TaskData::from_bytes(&data)?;

        Ok(Self {
            entries: data.entries,
            header,
        })
    }
}

impl Header {
    pub const SIZE: usize = 16;

    pub fn to_bytes(&self) -> Result<Bytes, binrw::Error> {
        let mut buf = Cursor::new(Vec::with_capacity(Self::SIZE));
        self.write(&mut buf)?;

        Ok(Bytes::from(buf.into_inner()))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, binrw::Error> {
        let mut cursor = Cursor::new(bytes);
        Self::read(&mut cursor)
    }
}

mod helpers {
    use std::net::Ipv4Addr;

    use binrw::{BinRead, BinResult, BinWrite, NullString, parser, writer};
    use chrono::{DateTime, Utc};
    use prost_types::Any;
    use uuid::Uuid;

    #[binrw::binrw]
    #[brw(little)]
    struct AnyBin {
        type_url: NullString,
        #[bw(try_calc(u32::try_from(value.len())))]
        value_count: u32,
        #[br(count = value_count)]
        value: Vec<u8>,
    }

    #[parser(reader, endian)]
    pub fn ipv4_addr_parser() -> BinResult<Ipv4Addr> {
        let bits = <u32>::read_options(reader, endian, ())?;
        Ok(Ipv4Addr::from_bits(bits))
    }

    #[writer(writer, endian)]
    pub fn ipv4_addr_writer(addr: &Ipv4Addr) -> BinResult<()> {
        let bits = addr.to_bits();
        bits.write_options(writer, endian, ())
    }

    #[parser(reader, endian)]
    pub fn uuid_parser() -> BinResult<Uuid> {
        let hi = <u64>::read_options(reader, endian, ())?;
        let lo = <u64>::read_options(reader, endian, ())?;
        Ok(Uuid::from_u64_pair(hi, lo))
    }

    #[writer(writer, endian)]
    pub fn uuid_writer(id: &Uuid) -> BinResult<()> {
        let (hi, lo) = id.as_u64_pair();
        hi.write_options(writer, endian, ())?;
        lo.write_options(writer, endian, ())?;
        Ok(())
    }

    #[parser(reader, endian)]
    pub fn dt_utc_parser() -> BinResult<DateTime<Utc>> {
        let ts = <i64>::read_options(reader, endian, ())?;
        Ok(DateTime::from_timestamp_millis(ts).unwrap())
    }

    #[writer(writer, endian)]
    pub fn dt_utc_writer(dt: &DateTime<Utc>) -> BinResult<()> {
        let ts = dt.timestamp_millis();
        ts.write_options(writer, endian, ())?;
        Ok(())
    }

    #[parser(reader, endian)]
    pub fn any_parser() -> BinResult<Any> {
        let inner = <AnyBin>::read_options(reader, endian, ())?;
        Ok(Any {
            type_url: inner.type_url.to_string(),
            value: inner.value,
        })
    }

    #[writer(writer, endian)]
    pub fn any_writer(any: &Any) -> BinResult<()> {
        let inner = AnyBin {
            type_url: NullString::from(any.type_url.as_str()),
            value: any.value.clone(),
        };
        inner.write_options(writer, endian, ())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use pretty_hex::{pretty_hex, simple_hex};

    use super::*;

    fn new_payload() -> Any {
        Any {
            type_url: "my_type".to_string(),
            value: b"my_value".to_vec(),
        }
    }

    fn new_task() -> Task {
        Task {
            id: Uuid::from_u128(0x0011_2233_4455_6677_8899_aabb_ccdd_eeff),
            originally_scheduled_at: DateTime::from_timestamp(1_700_000_000, 0)
                .expect("timestamp should be valid"),
            scheduled_at: DateTime::from_timestamp(1_700_000_060, 0)
                .expect("timestamp should be valid"),
            max_retries: 3,
            retry_count: 0,
            status: TaskStatus::default(),
            payload: new_payload(),
        }
    }

    fn new_running_status() -> TaskStatus {
        TaskStatus::Running {
            worker: Worker {
                id: Uuid::from_u128(0xffee_ddcc_bbaa_9988_7766_5544_3322_1100),
                last_heartbeat: DateTime::from_timestamp(1_700_000_120, 0)
                    .expect("timestamp should be valid"),
            },
            lease_expires_at: DateTime::from_timestamp(1_700_000_180, 0)
                .expect("timestamp should be valid"),
        }
    }

    fn new_failed_status() -> TaskStatus {
        TaskStatus::Failed {
            error_message: NullString::from("task failed"),
            details: new_payload(),
        }
    }

    fn new_header(compression: CompressionType) -> Header {
        Header {
            address: Ipv4Addr::new(127, 0, 0, 1),
            port: 8080,
            epoch: 42,
            compression,
        }
    }

    fn new_manifest(compression: CompressionType) -> Manifest {
        let first = new_task();
        let mut second = new_task();
        second.status = new_running_status();

        Manifest {
            entries: vec![first, second],
            header: new_header(compression),
        }
    }

    #[test]
    fn task_with_created_status() {
        let task = new_task();

        let bytes = task.to_bytes().expect("task should serialize");
        let parsed = Task::from_bytes(&bytes).expect("task should parse");

        assert_eq!(parsed, task);

        insta::assert_snapshot!(pretty_hex(&bytes), @r#"
        Length: 57 (0x39) bytes
        0000:   77 66 55 44  33 22 11 00  ff ee dd cc  bb aa 99 88   wfUD3"..........
        0010:   00 68 e5 cf  8b 01 00 00  60 52 e6 cf  8b 01 00 00   .h......`R......
        0020:   03 00 00 00  00 6d 79 5f  74 79 70 65  00 08 00 00   .....my_type....
        0030:   00 6d 79 5f  76 61 6c 75  65                         .my_value
        "#);
    }

    #[test]
    fn task_with_running_status() {
        let mut task = new_task();
        task.status = new_running_status();

        let bytes = task.to_bytes().expect("task should serialize");
        let parsed = Task::from_bytes(&bytes).expect("task should parse");

        assert_eq!(parsed, task);

        insta::assert_snapshot!(pretty_hex(&bytes), @r#"
        Length: 89 (0x59) bytes
        0000:   77 66 55 44  33 22 11 00  ff ee dd cc  bb aa 99 88   wfUD3"..........
        0010:   00 68 e5 cf  8b 01 00 00  60 52 e6 cf  8b 01 00 00   .h......`R......
        0020:   03 00 00 00  01 88 99 aa  bb cc dd ee  ff 00 11 22   ..............."
        0030:   33 44 55 66  77 c0 3c e7  cf 8b 01 00  00 20 27 e8   3DUfw.<...... '.
        0040:   cf 8b 01 00  00 6d 79 5f  74 79 70 65  00 08 00 00   .....my_type....
        0050:   00 6d 79 5f  76 61 6c 75  65                         .my_value
        "#);
    }

    #[test]
    fn task_with_failed_status() {
        let mut task = new_task();
        task.status = new_failed_status();

        let bytes = task.to_bytes().expect("task should serialize");
        let parsed = Task::from_bytes(&bytes).expect("task should parse");

        assert_eq!(parsed, task);

        insta::assert_snapshot!(pretty_hex(&bytes), @r#"
        Length: 89 (0x59) bytes
        0000:   77 66 55 44  33 22 11 00  ff ee dd cc  bb aa 99 88   wfUD3"..........
        0010:   00 68 e5 cf  8b 01 00 00  60 52 e6 cf  8b 01 00 00   .h......`R......
        0020:   03 00 00 00  02 74 61 73  6b 20 66 61  69 6c 65 64   .....task failed
        0030:   00 6d 79 5f  74 79 70 65  00 08 00 00  00 6d 79 5f   .my_type.....my_
        0040:   76 61 6c 75  65 6d 79 5f  74 79 70 65  00 08 00 00   valuemy_type....
        0050:   00 6d 79 5f  76 61 6c 75  65                         .my_value
        "#);
    }

    #[test]
    fn task_data_with_two_tasks() {
        let first = new_task();
        let mut second = new_task();
        second.status = new_running_status();
        let data = TaskData {
            entries: vec![first, second],
        };

        let bytes = data.to_bytes().expect("task data should serialize");
        let parsed = TaskData::from_bytes(&bytes).expect("task data should parse");

        assert_eq!(parsed.entries.len(), 2);
        assert_eq!(parsed.entries, data.entries);

        insta::assert_snapshot!(pretty_hex(&bytes), @r#"
        Length: 160 (0xa0) bytes
        0000:   54 41 53 4b  02 00 00 00  77 66 55 44  33 22 11 00   TASK....wfUD3"..
        0010:   ff ee dd cc  bb aa 99 88  00 68 e5 cf  8b 01 00 00   .........h......
        0020:   60 52 e6 cf  8b 01 00 00  03 00 00 00  00 6d 79 5f   `R...........my_
        0030:   74 79 70 65  00 08 00 00  00 6d 79 5f  76 61 6c 75   type.....my_valu
        0040:   65 77 66 55  44 33 22 11  00 ff ee dd  cc bb aa 99   ewfUD3".........
        0050:   88 00 68 e5  cf 8b 01 00  00 60 52 e6  cf 8b 01 00   ..h......`R.....
        0060:   00 03 00 00  00 01 88 99  aa bb cc dd  ee ff 00 11   ................
        0070:   22 33 44 55  66 77 c0 3c  e7 cf 8b 01  00 00 20 27   "3DUfw.<...... '
        0080:   e8 cf 8b 01  00 00 6d 79  5f 74 79 70  65 00 08 00   ......my_type...
        0090:   00 00 6d 79  5f 76 61 6c  75 65 00 00  00 00 00 00   ..my_value......
        "#);
    }

    #[test]
    fn manifest_without_compression() {
        let manifest = new_manifest(CompressionType::None);
        let expected = manifest.clone();

        let bytes = manifest.into_bytes().expect("manifest should serialize");
        let parsed = Manifest::from_bytes(&bytes).expect("manifest should parse");

        assert_eq!(parsed, expected);

        insta::assert_snapshot!(pretty_hex(&bytes), @r#"
        Length: 176 (0xb0) bytes
        0000:   01 01 00 00  7f 90 1f 2a  00 00 00 00  00 00 00 00   .......*........
        0010:   54 41 53 4b  02 00 00 00  77 66 55 44  33 22 11 00   TASK....wfUD3"..
        0020:   ff ee dd cc  bb aa 99 88  00 68 e5 cf  8b 01 00 00   .........h......
        0030:   60 52 e6 cf  8b 01 00 00  03 00 00 00  00 6d 79 5f   `R...........my_
        0040:   74 79 70 65  00 08 00 00  00 6d 79 5f  76 61 6c 75   type.....my_valu
        0050:   65 77 66 55  44 33 22 11  00 ff ee dd  cc bb aa 99   ewfUD3".........
        0060:   88 00 68 e5  cf 8b 01 00  00 60 52 e6  cf 8b 01 00   ..h......`R.....
        0070:   00 03 00 00  00 01 88 99  aa bb cc dd  ee ff 00 11   ................
        0080:   22 33 44 55  66 77 c0 3c  e7 cf 8b 01  00 00 20 27   "3DUfw.<...... '
        0090:   e8 cf 8b 01  00 00 6d 79  5f 74 79 70  65 00 08 00   ......my_type...
        00a0:   00 00 6d 79  5f 76 61 6c  75 65 00 00  00 00 00 00   ..my_value......
        "#);
    }

    #[test]
    fn manifest_with_zstd_compression() {
        let manifest = new_manifest(CompressionType::Zstd);
        let expected = manifest.clone();

        let bytes = manifest.into_bytes().expect("manifest should serialize");
        let parsed = Manifest::from_bytes(&bytes).expect("manifest should parse");

        assert_eq!(parsed, expected);

        insta::assert_snapshot!(pretty_hex(&bytes), @r#"
        Length: 128 (0x80) bytes
        0000:   01 01 00 00  7f 90 1f 2a  00 00 00 00  00 00 00 01   .......*........
        0010:   28 b5 2f fd  20 a0 3d 03  00 34 05 54  41 53 4b 02   (./. .=..4.TASK.
        0020:   00 00 00 77  66 55 44 33  22 11 00 ff  ee dd cc bb   ...wfUD3".......
        0030:   aa 99 88 00  68 e5 cf 8b  01 00 00 60  52 e6 03 00   ....h......`R...
        0040:   00 00 00 6d  79 5f 74 79  70 65 00 08  76 61 6c 75   ...my_type..valu
        0050:   65 01 88 99  aa bb cc dd  ee ff 00 11  22 33 44 55   e..........."3DU
        0060:   66 77 c0 3c  e7 20 27 e8  00 00 00 00  00 00 06 00   fw.<. '.........
        0070:   5c a0 60 25  43 16 2e 96  2b a2 bd a0  da da f2 04   \.`%C...+.......
        "#);
    }

    #[test]
    fn header_round_trips_bytes() {
        let header = Header {
            address: Ipv4Addr::new(1, 2, 3, 4),
            port: 0x0506,
            epoch: 0x0708_090a_0b0c_0d0e,
            compression: CompressionType::Zstd,
        };

        let bytes = header.to_bytes().expect("header should serialize");
        let parsed = Header::from_bytes(&bytes).expect("header should parse");

        assert_eq!(parsed, header);

        insta::assert_snapshot!(simple_hex(&bytes), @"01 04 03 02  01 06 05 0e  0d 0c 0b 0a  09 08 07 01");
    }

    #[test]
    fn header_errors_with_wrong_version() {
        let header = Header {
            address: Ipv4Addr::new(1, 2, 3, 4),
            port: 0x0506,
            epoch: 0x0708_090a_0b0c_0d0e,
            compression: CompressionType::Zstd,
        };

        let mut bytes = header.to_bytes().expect("header should serialize").to_vec();
        bytes[0] = 0;

        let err = Header::from_bytes(&bytes).unwrap_err();
        insta::assert_compact_debug_snapshot!(err, @"bad magic at 0x0: 0");
    }
}
