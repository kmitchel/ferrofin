//! TitanTV lineup and schedule ingestion.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use chrono::{DateTime, Duration, Local, NaiveDateTime, TimeZone, Utc};
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

#[derive(Clone)]
pub(crate) struct TitanTv {
    fetcher: Arc<dyn SourceFetcher>,
}

pub(crate) struct Guide {
    pub(crate) complete: bool,
    pub(crate) channels: Vec<EpgChannel>,
    pub(crate) programs: HashMap<String, Vec<Program>>,
}

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

#[derive(Deserialize)]
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
        Self { fetcher }
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

    pub(crate) async fn guide(
        &self,
        info: &ListingsProviderInfo,
        window: (DateTime<Utc>, DateTime<Utc>),
        superseded: impl Fn() -> bool + Send,
    ) -> Result<Option<Guide>, ServiceError> {
        let (user, lineup) = configured_ids(info)?;
        let channels = self.lineup(user, lineup).await?;
        let end = window.1.min(window.0 + Duration::days(7));
        let block = Duration::hours(6);
        let mut start = window.0;
        let mut complete = true;
        let mut programs: HashMap<String, BTreeMap<DateTime<Utc>, Program>> = HashMap::new();
        while start < end {
            if superseded() {
                return Ok(None);
            }
            let time = start.with_timezone(&Local).format("%Y%m%d%H%M").to_string();
            let url = format!(
                "{BASE_URL}/schedule/{user}/{lineup}/{time}/{}",
                block.num_minutes()
            );
            start += block;
            let schedules: Vec<Schedule> =
                match self.request(&url, &format!("schedule block {time}")).await {
                    Ok(schedules) => schedules,
                    Err(error) => {
                        complete = false;
                        tracing::warn!(%error, "live tv: TitanTV schedule block failed");
                        continue;
                    }
                };
            let mut usable = false;
            for schedule in schedules {
                let Some(channel) = channels.get(&schedule.channel_index) else {
                    continue;
                };
                for event in schedule.days.into_iter().flat_map(|day| day.events) {
                    let Some(program) = event.into_program(&Local) else {
                        complete = false;
                        continue;
                    };
                    usable = true;
                    if program.end > window.0 && program.start < end {
                        programs
                            .entry(channel.id.clone())
                            .or_default()
                            .insert(program.start, program);
                    }
                }
            }
            if !usable {
                complete = false;
                tracing::warn!(block = %time, "live tv: TitanTV schedule block had no usable programmes");
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
