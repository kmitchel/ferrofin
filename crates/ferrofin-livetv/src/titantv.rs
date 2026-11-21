//! TitanTV lineup and schedule ingestion.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use chrono::{DateTime, Duration, DurationRound, Local, NaiveDateTime, TimeZone, Utc};
use ferrofin_model::live_tv::ListingsProviderInfo;
use ferrofin_traits::error::ServiceError;
use serde::Deserialize;
use uuid::Uuid;

use crate::fetch::SourceFetcher;
use crate::mapping::EpgChannel;

pub(crate) const PROVIDER_TYPE: &str = "TitanTV";
const BASE_URL: &str = "https://titantv.com/api";
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const BLOCK: Duration = Duration::hours(6);
const NEAR_TERM: Duration = Duration::hours(24);
const CACHE_LIFETIME: std::time::Duration = std::time::Duration::from_hours(48);

type BlockKey = (Uuid, Uuid, DateTime<Utc>);
type StationIds = BTreeMap<u64, String>;

struct CachedBlock {
    fetched_at: Instant,
    station_ids: Arc<StationIds>,
    programs: Arc<Vec<(u64, Program)>>,
}

#[derive(Clone)]
pub(crate) struct TitanTv {
    fetcher: Arc<dyn SourceFetcher>,
    blocks: Arc<Mutex<HashMap<BlockKey, CachedBlock>>>,
}

pub(crate) struct Guide {
    pub(crate) complete: bool,
    pub(crate) channels: Vec<EpgChannel>,
    pub(crate) programs: HashMap<String, Vec<Program>>,
}

#[derive(Clone)]
pub(crate) struct Program {
    pub(crate) start: DateTime<Utc>,
    pub(crate) end: DateTime<Utc>,
    pub(crate) event: Event,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Response<T> {
    error_code: i32,
    error_message: Option<String>,
    channels: Option<Vec<T>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Station {
    channel_index: u64,
    channel_id: u64,
    major_channel: u32,
    minor_channel: Option<u32>,
    call_sign: Option<String>,
    description: Option<String>,
    network: Option<String>,
    logo: Option<String>,
}

impl Station {
    fn epg(self) -> EpgChannel {
        let number = match self.minor_channel.filter(|minor| *minor != 0) {
            Some(minor) => format!("{}.{minor}", self.major_channel),
            None => self.major_channel.to_string(),
        };
        let name = nonempty(self.call_sign)
            .or_else(|| {
                self.description?
                    .split_whitespace()
                    .next()
                    .map(str::to_owned)
            })
            .or_else(|| nonempty(self.network))
            .unwrap_or_else(|| number.clone());
        EpgChannel {
            id: self.channel_id.to_string(),
            name,
            number,
            image_url: nonempty(self.logo),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Schedule {
    channel_index: u64,
    days: Vec<Day>,
}

#[derive(Deserialize)]
struct Day {
    events: Vec<Event>,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Event {
    start_time: String,
    end_time: String,
    duration: Option<i64>,
    pub(crate) title: String,
    pub(crate) episode_title: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) series_description: Option<String>,
    pub(crate) show_card: Option<String>,
    pub(crate) display_genre: Option<String>,
    pub(crate) program_type: Option<String>,
    pub(crate) tv_rating: Option<String>,
    pub(crate) mpaa_rating: Option<String>,
    pub(crate) year: Option<i32>,
    pub(crate) season_number: Option<i32>,
    pub(crate) season_episode_number: Option<i32>,
    pub(crate) program_id: Option<u64>,
    pub(crate) parent_program_id: Option<u64>,
    pub(crate) new_repeat: Option<String>,
    original_air_date: Option<String>,
    pub(crate) live_tape_replay: Option<String>,
}

fn nonempty(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.trim().is_empty())
}

fn configured_ids(info: &ListingsProviderInfo) -> Result<(Uuid, Uuid), ServiceError> {
    let parse = |value: Option<&str>, field| {
        value
            .and_then(|value| Uuid::parse_str(value.trim()).ok())
            .filter(|id| !id.is_nil())
            .ok_or_else(|| ServiceError::invalid_input(format!("TitanTV {field} must be a UUID")))
    };
    Ok((
        parse(info.username.as_deref(), "user ID")?,
        parse(info.listings_id.as_deref(), "lineup ID")?,
    ))
}

fn timestamp(value: &str, zone: &impl TimeZone) -> Option<DateTime<Utc>> {
    if let Ok(offset) = DateTime::parse_from_rfc3339(value) {
        return Some(offset.with_timezone(&Utc));
    }
    let local = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f").ok()?;
    zone.from_local_datetime(&local)
        .earliest()
        .map(|date| date.with_timezone(&Utc))
}

impl Event {
    /// TitanTV supplies the original broadcast calendar date, often as midnight.
    pub(crate) fn originally_aired_before(&self, airing_date: chrono::NaiveDate) -> bool {
        let Some(raw) = self.original_air_date.as_deref().map(str::trim) else {
            return false;
        };
        let date = chrono::NaiveDate::parse_from_str(raw, "%Y-%m-%d")
            .or_else(|_| {
                NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.f").map(|dt| dt.date())
            })
            .or_else(|_| DateTime::parse_from_rfc3339(raw).map(|dt| dt.date_naive()));
        date.is_ok_and(|date| chrono::Datelike::year(&date) > 1 && date < airing_date)
    }

    fn into_program(self, zone: &impl TimeZone) -> Option<Program> {
        if self.title.trim().is_empty() {
            return None;
        }
        let start = timestamp(&self.start_time, zone)?;
        let end = self
            .duration
            .filter(|duration| *duration > 0)
            .and_then(Duration::try_seconds)
            .and_then(|duration| start.checked_add_signed(duration))
            .or_else(|| timestamp(&self.end_time, zone))?;
        (end > start).then_some(Program {
            start,
            end,
            event: self,
        })
    }
}

impl TitanTv {
    pub(crate) fn new(fetcher: Arc<dyn SourceFetcher>) -> Self {
        Self {
            fetcher,
            blocks: Arc::default(),
        }
    }

    async fn request<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        operation: &str,
    ) -> Result<Vec<T>, ServiceError> {
        // The path contains the user's UUID. Report the operation and code
        // because transport errors and provider messages can echo that path.
        let failure =
            |reason: &str| ServiceError::backend(format!("TitanTV {operation}: {reason}"));
        let body = async {
            let mut response = self
                .fetcher
                .open(url, None)
                .await
                .map_err(|_| failure("request failed"))?;
            if !response.is_success() {
                return Err(failure(&format!("HTTP {}", response.status)));
            }
            let mut bytes = Vec::new();
            while let Some(chunk) = response
                .body
                .next_chunk()
                .await
                .map_err(|_| failure("response read failed"))?
            {
                if chunk.len() > MAX_RESPONSE_BYTES - bytes.len() {
                    return Err(failure("response exceeded the size limit"));
                }
                bytes.extend_from_slice(&chunk);
            }
            Ok(bytes)
        };
        let bytes = tokio::time::timeout(REQUEST_TIMEOUT, body)
            .await
            .map_err(|_| failure("request timed out"))??;
        let response: Response<T> =
            serde_json::from_slice(&bytes).map_err(|_| failure("invalid JSON response"))?;
        if response.error_code != 0 || nonempty(response.error_message).is_some() {
            return Err(failure(&format!("API error code {}", response.error_code)));
        }
        response
            .channels
            .filter(|channels| !channels.is_empty())
            .ok_or_else(|| failure("response contained no channels"))
    }

    async fn lineup(
        &self,
        user: Uuid,
        lineup: Uuid,
    ) -> Result<HashMap<u64, EpgChannel>, ServiceError> {
        let stations: Vec<Station> = self
            .request(&format!("{BASE_URL}/channel/{user}/{lineup}"), "lineup")
            .await?;
        Ok(stations
            .into_iter()
            .map(|station| (station.channel_index, station.epg()))
            .collect())
    }

    pub(crate) async fn channels(
        &self,
        info: &ListingsProviderInfo,
    ) -> Result<Vec<EpgChannel>, ServiceError> {
        let (user, lineup) = configured_ids(info)?;
        Ok(sorted_channels(self.lineup(user, lineup).await?))
    }

    fn cache_schedule(
        &self,
        key: BlockKey,
        station_ids: &Arc<StationIds>,
        fetched_at: Instant,
        schedules: Vec<Schedule>,
    ) -> (Arc<Vec<(u64, Program)>>, bool) {
        let mut block_complete = true;
        let mut block_programs = Vec::new();
        for schedule in schedules {
            if !station_ids.contains_key(&schedule.channel_index) {
                continue;
            }
            for event in schedule.days.into_iter().flat_map(|day| day.events) {
                if let Some(program) = event.into_program(&Local) {
                    block_programs.push((schedule.channel_index, program));
                } else {
                    block_complete = false;
                }
            }
        }
        if block_programs.is_empty() {
            block_complete = false;
            tracing::warn!(block = %key.2, "live tv: TitanTV schedule block had no usable programmes");
        }
        let block_programs = Arc::new(block_programs);
        let mut blocks = self.blocks.lock().expect("TitanTV cache lock");
        if blocks
            .get(&key)
            .is_none_or(|entry| entry.fetched_at <= fetched_at)
        {
            if block_complete {
                blocks.insert(
                    key,
                    CachedBlock {
                        fetched_at,
                        station_ids: Arc::clone(station_ids),
                        programs: Arc::clone(&block_programs),
                    },
                );
            } else {
                blocks.remove(&key);
            }
        }
        (block_programs, block_complete)
    }

    pub(crate) async fn guide(
        &self,
        info: &ListingsProviderInfo,
        window: (DateTime<Utc>, DateTime<Utc>),
        superseded: impl Fn() -> bool + Send,
    ) -> Result<Option<Guide>, ServiceError> {
        let (user, lineup) = configured_ids(info)?;
        let channels = self.lineup(user, lineup).await?;
        let end = window.1.min(window.0 + Duration::days(7));
        // Stable UTC boundaries let scheduled and manual runs share blocks.
        // The service still receives local wall-clock request times.
        let first_block = window
            .0
            .duration_trunc(BLOCK)
            .map_err(|_| ServiceError::invalid_input("invalid TitanTV guide window"))?;
        let station_ids = Arc::new(
            channels
                .iter()
                .map(|(index, channel)| (*index, channel.id.clone()))
                .collect::<StationIds>(),
        );
        self.blocks
            .lock()
            .expect("TitanTV cache lock")
            .retain(|(_, _, start), entry| {
                *start >= first_block && entry.fetched_at.elapsed() < CACHE_LIFETIME
            });
        let mut start = first_block;
        let mut complete = true;
        let mut programs: HashMap<String, BTreeMap<DateTime<Utc>, Program>> = HashMap::new();
        while start < end {
            if superseded() {
                return Ok(None);
            }
            let block_start = start;
            start += BLOCK;
            let key = (user, lineup, block_start);
            let cached = if block_start >= window.0 + NEAR_TERM {
                self.blocks
                    .lock()
                    .expect("TitanTV cache lock")
                    .get(&key)
                    .filter(|entry| {
                        entry.fetched_at.elapsed() < CACHE_LIFETIME
                            && entry.station_ids == station_ids
                    })
                    .map(|entry| Arc::clone(&entry.programs))
            } else {
                None
            };
            let block_programs = if let Some(cached) = cached {
                cached
            } else {
                let time = block_start.with_timezone(&Local).format("%Y%m%d%H%M");
                let url = format!(
                    "{BASE_URL}/schedule/{user}/{lineup}/{time}/{}",
                    BLOCK.num_minutes()
                );
                let fetched_at = Instant::now();
                let schedules: Vec<Schedule> =
                    match self.request(&url, &format!("schedule block {time}")).await {
                        Ok(schedules) => schedules,
                        Err(error) => {
                            complete = false;
                            tracing::warn!(%error, "live tv: TitanTV schedule block failed");
                            continue;
                        }
                    };
                if superseded() {
                    return Ok(None);
                }
                let (block_programs, block_complete) =
                    self.cache_schedule(key, &station_ids, fetched_at, schedules);
                complete &= block_complete;
                block_programs
            };

            for (index, program) in block_programs.iter() {
                // A block owns the airings that start in it. The first block
                // also supplies any earlier airing still in progress.
                if program.end > window.0
                    && program.start < end.min(start)
                    && (block_start == first_block || program.start >= block_start)
                {
                    programs
                        .entry(channels[index].id.clone())
                        .or_default()
                        .insert(program.start, program.clone());
                }
            }
        }
        if programs.is_empty() {
            return Err(ServiceError::backend(
                "TitanTV returned no programmes for the guide window",
            ));
        }
        Ok(Some(Guide {
            complete,
            channels: sorted_channels(channels),
            programs: programs
                .into_iter()
                .map(|(id, airings)| (id, airings.into_values().collect()))
                .collect(),
        }))
    }
}

fn sorted_channels(channels: HashMap<u64, EpgChannel>) -> Vec<EpgChannel> {
    let mut channels: Vec<_> = channels.into_values().collect();
    channels.sort_by(|a, b| (&a.number, &a.id).cmp(&(&b.number, &b.id)));
    channels
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn titantv_parses_local_and_offset_airings() {
        let eastern = chrono::FixedOffset::west_opt(5 * 3600).unwrap();
        let local = timestamp("2026-11-15T12:00:00", &eastern).unwrap();
        assert_eq!(local, Utc.with_ymd_and_hms(2026, 11, 15, 17, 0, 0).unwrap());
        assert_eq!(
            timestamp("2026-11-15T18:00:00+01:00", &eastern),
            Some(local)
        );
        assert!(timestamp("invalid", &eastern).is_none());
    }
}
