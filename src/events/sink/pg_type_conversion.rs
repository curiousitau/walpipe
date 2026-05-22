//! PostgreSQL type conversion utilities
//!
//! Provides utilities for converting PostgreSQL replication data into structured formats
//! for processing by event sinks like Hook0.

use crate::protocol::messages::{TupleData, ReplicationMessage};
use crate::utils::binary::Oid;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;
use tracing::{debug, warn};
use uuid::Uuid;

#[derive(Debug)]
#[allow(unused)]
pub(crate) enum EventConversionError {
    ColumnCountMismatch,
    UuidParseErr(uuid::Error),
    DateTimeParse(chrono::ParseError),
    ColumnTypeMismatch,
    MissingColumn(&'static str),
}

type RelationColumns = Vec<(String, PgType)>;

const EVENT_STREAM_TABLE: &str = "event_stream";

#[derive(Clone)]
pub(crate) struct ReplicationEventDecoder {
    /// Map of table OIDs to their column definitions
    pub(crate) relations: HashMap<Oid, RelationColumns>,
    /// Map of table OIDs to their relation names, used to filter
    /// out changes from tables that are not of interest (e.g. walpipe_heartbeat)
    relation_names: HashMap<Oid, String>,
}

#[allow(unused)]
#[derive(Debug)]
pub(crate) enum ColumnValue {
    String(String),
    Uuid(uuid::Uuid),
    Int(u32),
    Bool(bool),
    Json(serde_json::Value),
    Timestamp(NaiveDateTime),
    Date(NaiveDate),
    TimestampTZ(DateTime<Utc>),
}

impl TryInto<String> for ColumnValue {
    type Error = EventConversionError;

    fn try_into(self) -> Result<String, Self::Error> {
        Ok(match self {
            ColumnValue::String(value) => value.clone(),
            ColumnValue::Uuid(value) => value.to_string(),
            ColumnValue::Int(value) => value.to_string(),
            ColumnValue::Bool(value) => value.to_string(),
            ColumnValue::Json(value) => value.to_string(),
            ColumnValue::Timestamp(value) => value.to_string(),
            ColumnValue::Date(value) => value.to_string(),
            ColumnValue::TimestampTZ(value) => value.to_string(),
        })
    }
}

impl TryInto<String> for &ColumnValue {
    type Error = EventConversionError;

    fn try_into(self) -> Result<String, Self::Error> {
        Ok(match self {
            ColumnValue::String(value) => value.clone(),
            ColumnValue::Uuid(value) => value.to_string(),
            ColumnValue::Int(value) => value.to_string(),
            ColumnValue::Bool(value) => value.to_string(),
            ColumnValue::Json(value) => value.to_string(),
            ColumnValue::Timestamp(value) => value.to_string(),
            ColumnValue::Date(value) => value.to_string(),
            ColumnValue::TimestampTZ(value) => value.to_string(),
        })
    }
}

pub(crate) struct ReplicationRow {
    pub(crate) column_values: HashMap<String, ColumnValue>,
}

pub(crate) fn parse_timestamptz(str: String) -> Result<DateTime<Utc>, EventConversionError> {
    DateTime::parse_from_str(&str, "%Y-%m-%d %H:%M:%S%.f%#z")
        .map(|x| x.to_utc())
        .map_err(EventConversionError::DateTimeParse)
}

impl ReplicationEventDecoder {
    pub(crate) fn decode(&mut self, message: &ReplicationMessage) -> Option<ReplicationRow> {
        match message {
            ReplicationMessage::Relation { relation } => {
                pub(crate) fn to_tuple(c: &crate::protocol::messages::ColumnInfo) -> Option<(String, PgType)> {
                    let try_from = PgType::try_from(c.column_type);

                    if try_from.is_err() {
                        warn!("Unknown column type");

                        return None;
                    }

                    Some((c.column_name.clone(), try_from.unwrap()))
                }

                debug!(
                    "Storing table OID {} {}",
                    relation.oid, relation.relation_name
                );
                self.relation_names.insert(relation.oid, relation.relation_name.clone());
                self.relations.insert(
                    relation.oid,
                    relation.columns.iter().filter_map(to_tuple).collect(),
                );

                None
            }
            ReplicationMessage::Insert {
                relation_id,
                tuple_data,
                is_stream: _,
                xid: _,
            } => {
                if !self.is_event_stream_table(*relation_id) {
                    return None;
                }
                self.tuple_to_row(*relation_id, tuple_data)
            }
            ReplicationMessage::Update {
                relation_id,
                key_type: _,
                old_tuple_data: _,
                new_tuple_data,
                is_stream: _,
                xid: _,
            } => {
                if !self.is_event_stream_table(*relation_id) {
                    return None;
                }
                self.tuple_to_row(*relation_id, new_tuple_data)
            }
            _ => None,
        }
    }

    fn is_event_stream_table(&self, relation_id: Oid) -> bool {
        match self.relation_names.get(&relation_id) {
            Some(name) => name == EVENT_STREAM_TABLE,
            None => false,
        }
    }

    pub(crate) fn tuple_to_row(
        &self,
        relation_id: Oid,
        tuple_data: &TupleData,
    ) -> Option<ReplicationRow> {
        let table = self
            .relations
            .get(&relation_id)
            .unwrap_or_else(|| panic!("Unknown table OID: {}", relation_id));

        let mut column_values: HashMap<String, ColumnValue> = HashMap::new();

        for (i, x) in tuple_data.columns.clone().iter().enumerate() {
            let col_def = &table[i];

            let data = x.data.clone();

            let col_val: ColumnValue = match col_def.1 {
                PgType::Jsonb | PgType::Json => match Value::from_str(&data) {
                    Ok(value) => ColumnValue::Json(value),

                    Err(e) => {
                        warn!("Jsonb column parsing failed: {}", e);
                        ColumnValue::String(data)
                    }
                },
                PgType::Uuid => match Uuid::try_parse(&data) {
                    Ok(value) => ColumnValue::Uuid(value),

                    Err(e) => {
                        warn!("Uuid column parsing failed: {}", e);
                        ColumnValue::String(data)
                    }
                },
                PgType::Date => match NaiveDate::from_str(&data) {
                    Ok(value) => ColumnValue::Date(value),

                    Err(e) => {
                        warn!("Date column parsing failed: {}", e);
                        ColumnValue::String(data)
                    }
                },
                PgType::Timestamp => match NaiveDateTime::from_str(&data) {
                    Ok(value) => ColumnValue::Timestamp(value),

                    Err(e) => {
                        warn!("Timestamp column parsing failed: {}", e);
                        ColumnValue::String(data)
                    }
                },
                PgType::Timestamptz => match parse_timestamptz(data.clone()) {
                    Ok(value) => ColumnValue::TimestampTZ(value.to_utc()),

                    Err(e) => {
                        warn!("TimestampTZ column parsing failed: {:?}", e);
                        ColumnValue::String(data)
                    }
                },
                _ => ColumnValue::String(data),
            };

            column_values.insert(col_def.0.clone(), col_val);
        }

        Some(ReplicationRow {
            // relation_id,
            column_values,
        })
    }

    pub(crate) fn new() -> Self {
        Self {
            relations: HashMap::new(),
            relation_names: HashMap::new(),
        }
    }
}

#[repr(u32)]
#[derive(Clone)]
pub(crate) enum PgType {
    Bool = 16,
    Bytea = 17,
    Char = 18,
    Name = 19,
    Int8 = 20,
    Int2 = 21,
    Int2vector = 22,
    Int4 = 23,
    Text = 25,
    Oid = 26,
    Tid = 27,
    Xid = 28,
    Cid = 29,
    Oidvector = 30,
    Json = 114,
    Xml = 142,
    Xid8 = 5069,
    Point = 600,
    Lseg = 601,
    Path = 602,
    Box = 603,
    Polygon = 604,
    Line = 628,
    Float4 = 700,
    Float8 = 701,
    Unknown = 705,
    Circle = 718,
    Money = 790,
    Macaddr = 829,
    Inet = 869,
    Cidr = 650,
    Macaddr8 = 774,
    Aclitem = 1033,
    Bpchar = 1042,
    Varchar = 1043,
    Date = 1082,
    Time = 1083,
    Timestamp = 1114,
    Timestamptz = 1184,
    Interval = 1186,
    Timetz = 1266,
    Bit = 1560,
    Varbit = 1562,
    Numeric = 1700,
    Refcursor = 1790,
    Uuid = 2950,
    Tsvector = 3614,
    Gtsvector = 3642,
    Tsquery = 3615,
    Jsonb = 3802,
    Jsonpath = 4072,
    TxidSnapshot = 2970,
    Int4range = 3904,
    Numrange = 3906,
    Tsrange = 3908,
    Tstzrange = 3910,
    Daterange = 3912,
    Int8range = 3926,
    Int4multirange = 4451,
    Nummultirange = 4532,
    Tsmultirange = 4533,
    Tstzmultirange = 4534,
    Datemultirange = 4535,
}

impl std::convert::TryFrom<u32> for PgType {
    type Error = ();

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            16 => Ok(PgType::Bool),
            17 => Ok(PgType::Bytea),
            18 => Ok(PgType::Char),
            19 => Ok(PgType::Name),
            20 => Ok(PgType::Int8),
            21 => Ok(PgType::Int2),
            22 => Ok(PgType::Int2vector),
            23 => Ok(PgType::Int4),
            25 => Ok(PgType::Text),
            26 => Ok(PgType::Oid),
            27 => Ok(PgType::Tid),
            28 => Ok(PgType::Xid),
            29 => Ok(PgType::Cid),
            30 => Ok(PgType::Oidvector),
            114 => Ok(PgType::Json),
            142 => Ok(PgType::Xml),
            5069 => Ok(PgType::Xid8),
            600 => Ok(PgType::Point),
            601 => Ok(PgType::Lseg),
            602 => Ok(PgType::Path),
            603 => Ok(PgType::Box),
            604 => Ok(PgType::Polygon),
            628 => Ok(PgType::Line),
            700 => Ok(PgType::Float4),
            701 => Ok(PgType::Float8),
            705 => Ok(PgType::Unknown),
            718 => Ok(PgType::Circle),
            790 => Ok(PgType::Money),
            829 => Ok(PgType::Macaddr),
            869 => Ok(PgType::Inet),
            650 => Ok(PgType::Cidr),
            774 => Ok(PgType::Macaddr8),
            1033 => Ok(PgType::Aclitem),
            1042 => Ok(PgType::Bpchar),
            1043 => Ok(PgType::Varchar),
            1082 => Ok(PgType::Date),
            1083 => Ok(PgType::Time),
            1114 => Ok(PgType::Timestamp),
            1184 => Ok(PgType::Timestamptz),
            1186 => Ok(PgType::Interval),
            1266 => Ok(PgType::Timetz),
            1560 => Ok(PgType::Bit),
            1562 => Ok(PgType::Varbit),
            1700 => Ok(PgType::Numeric),
            1790 => Ok(PgType::Refcursor),
            2950 => Ok(PgType::Uuid),
            3614 => Ok(PgType::Tsvector),
            3642 => Ok(PgType::Gtsvector),
            3615 => Ok(PgType::Tsquery),
            3802 => Ok(PgType::Jsonb),
            4072 => Ok(PgType::Jsonpath),
            2970 => Ok(PgType::TxidSnapshot),
            3904 => Ok(PgType::Int4range),
            3906 => Ok(PgType::Numrange),
            3908 => Ok(PgType::Tsrange),
            3910 => Ok(PgType::Tstzrange),
            3912 => Ok(PgType::Daterange),
            3926 => Ok(PgType::Int8range),
            4451 => Ok(PgType::Int4multirange),
            4532 => Ok(PgType::Nummultirange),
            4533 => Ok(PgType::Tsmultirange),
            4534 => Ok(PgType::Tstzmultirange),
            4535 => Ok(PgType::Datemultirange),
            _ => Err(()),
        }
    }
}